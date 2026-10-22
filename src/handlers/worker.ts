import type { SQSBatchItemFailure, SQSBatchResponse, SQSEvent, SQSRecord } from 'aws-lambda';
import { INLINE_RESULT_MAX_BYTES, RETRY, SUPERSEDE_AFTER_MS, TIMEOUTS } from '../config';
import { providerHold, recordBackoff } from '../domain/backoff';
import { apiBreakerOpen, permanentBreakerOpen, providerBreakerOpen } from '../domain/breaker';
import { classify } from '../domain/classify';
import { resolveEndpoint } from '../domain/endpoint';
import { parseJob } from '../domain/job';
import { secondsUntil, turnAt } from '../domain/time';
import type { Job, Outcome } from '../domain/types';
import { log, metric } from '../observability';
import type { Clock, ExecutionsRepo, InternalApi, JobQueue, ResultStore } from '../ports';

export interface WorkerDeps {
  repo: ExecutionsRepo;
  queue: JobQueue;
  api: InternalApi;
  store: ResultStore;
  clock: Clock;
  config: {
    apiBaseUrl: string;
    /** true si λ2 ya tiene el lease por proveedor; si no, se aplica la cuarentena tras un desconocido. */
    leaseInApi: boolean;
  };
  random?: () => number;
}

type Result = 'DONE' | 'RETRY';

/** Worker (docs/ARQUITECTURA.md §3.2). Un mensaje por invocación (batch 1), sin asumirlo en el código. */
export function createWorker(deps: WorkerDeps) {
  return async (event: SQSEvent): Promise<SQSBatchResponse> => {
    const failures: SQSBatchItemFailure[] = [];
    for (const record of event.Records) {
      // En FIFO, si un mensaje vuelve a la cola, los siguientes del lote tienen que volver también.
      if (failures.length > 0) {
        failures.push({ itemIdentifier: record.messageId });
        continue;
      }
      if ((await processRecord(deps, record)) === 'RETRY') failures.push({ itemIdentifier: record.messageId });
    }
    return { batchItemFailures: failures };
  };
}

async function processRecord(deps: WorkerDeps, record: SQSRecord): Promise<Result> {
  const { repo, queue, clock } = deps;
  const random = deps.random ?? Math.random;

  const job = parseJob(record.body);
  if (!job) {
    log.error('mensaje_invalido', { messageId: record.messageId });
    metric('MensajesInvalidos', 1);
    await queue.sendExhausted({ code: 'INVALID_MESSAGE', messageId: record.messageId });
    return 'DONE';
  }
  const dims = { proveedor: job.proveedor };
  const startedAt = clock.now();

  // Retiene el grupo del proveedor: el mensaje vuelve a la cola dentro de `seconds` y nadie más
  // llama a ese proveedor mientras tanto. Siempre se llama después de dejar el estado escrito.
  const hold = async (seconds: number): Promise<Result> => {
    await queue.hold(record.receiptHandle, seconds);
    await bestEffort('registrar_retencion', () => repo.recordHold(job.proveedor, seconds, clock.now()));
    return 'RETRY';
  };

  // 1. Turno vencido: ya corresponde el del día siguiente.
  if (startedAt - turnAt(job.runDate, job.slot) > SUPERSEDE_AFTER_MS) {
    await repo.supersede(job, startedAt);
    metric('Superadas', 1, dims);
    return 'DONE';
  }

  // 2. Pausas: global (falla la API) o del proveedor. No cambian el estado ni gastan intentos.
  const global = await repo.getGlobalView(startedAt);
  if (global.pausedUntil > startedAt) return hold(secondsUntil(global.pausedUntil, startedAt, RETRY.maxHoldS));
  const view = await repo.getProviderView(job.proveedor, startedAt);
  const state = view.state;
  if (!state || !state.enabled) return hold(RETRY.maxHoldS);
  if (state.pausedUntil > startedAt) return hold(secondsUntil(state.pausedUntil, startedAt, RETRY.maxHoldS));

  // 3. Claim condicional. El token hace de fencing para todas las escrituras de esta corrida.
  const claim = await repo.claim(job, startedAt);
  if (claim.status === 'TERMINAL') return 'DONE';
  if (claim.status === 'BUSY') return hold(claim.leaseLeftS);
  const { token } = claim;

  // 4. Allowlist del endpoint (SSRF).
  const url = resolveEndpoint(job.endpoint, deps.config.apiBaseUrl, state.allowlist);
  if (!url) {
    await repo.finish(job, token, 'FAILED_PERMANENT', { code: 'ENDPOINT_NOT_ALLOWED' }, clock.now());
    log.warn('endpoint_rechazado', { execId: job.execId, proveedor: job.proveedor });
    metric('EndpointsRechazados', 1, dims);
    return 'DONE';
  }

  // 5. Llamada sin reintentos automáticos.
  const callStart = clock.now();
  const outcome = classify(await deps.api.call(job, url, TIMEOUTS.httpMs), callStart);
  const now = clock.now();
  log.info('llamada', { execId: job.execId, proveedor: job.proveedor, resultado: outcome.kind, codigo: 'code' in outcome ? outcome.code : undefined });
  // Las estadísticas son telemetría: si fallan no se pierde el resultado ni se repite la llamada.
  await bestEffort('registrar_llamada', () => repo.recordCall(job.proveedor, job.recordId, outcome, now - startedAt, now));

  // 6. Cierre.
  if (outcome.kind === 'success') {
    const details =
      Buffer.byteLength(outcome.body, 'utf8') > INLINE_RESULT_MAX_BYTES
        ? { httpStatus: outcome.httpStatus, resultKey: await deps.store.put(job, outcome.body) }
        : { httpStatus: outcome.httpStatus, result: outcome.body };
    await repo.finish(job, token, 'SUCCEEDED', details, now);
    if (state.backoffLevel > 0) await bestEffort('resetear_backoff', () => repo.resetProviderBackoff(job.proveedor));
    metric('Exitos', 1, dims);
    return 'DONE';
  }

  if (outcome.kind === 'permanent') {
    await repo.finish(job, token, 'FAILED_PERMANENT', { httpStatus: outcome.httpStatus, code: outcome.code }, now);
    metric('FallosPermanentes', 1, dims);
    if (permanentBreakerOpen(global.stats)) {
      await repo.pauseGlobal(now + RETRY.apiPauseS * 1_000);
      log.error('breaker_permanentes_abierto', {});
    }
    return 'DONE';
  }

  // Sin lease en λ2, un resultado desconocido puede haber dejado a λ2 viva: ninguna de las salidas
  // de abajo libera el grupo antes de que venza la cuarentena (§4), tampoco la del último intento.
  const quarantineLeftS =
    outcome.reason === 'unknown' && !deps.config.leaseInApi ? Math.max(0, Math.ceil(TIMEOUTS.quarantineS - (now - callStart) / 1_000)) : 0;

  // Falla de la API vista en varios registros y proveedores: pausa global, el registro no se quema.
  // Si solo falla este registro (ruta inexistente, por ejemplo), se trata como error del registro.
  if (outcome.reason === 'api' && apiBreakerOpen(global.stats)) {
    await repo.markWaiting(job, token, 'api', false, now);
    await repo.pauseGlobal(now + RETRY.apiPauseS * 1_000);
    log.error('falla_de_la_api', { execId: job.execId, codigo: outcome.code });
    metric('FallasDeApi', 1);
    return hold(RETRY.apiPauseS);
  }

  // Falla el proveedor (o el breaker detecta que la mayoría de las fallas son suyas): pausa sin gastar intentos.
  if (outcome.reason === 'provider' || providerBreakerOpen(view.stats)) {
    const receiveCount = Number(record.attributes.ApproximateReceiveCount ?? 1);
    const seconds = Math.max(providerHold(state.backoffLevel, outcome.retryAfterS, receiveCount, random), quarantineLeftS);
    await repo.markWaiting(job, token, 'provider', false, now);
    await repo.pauseProvider(job.proveedor, now + seconds * 1_000, { disable: outcome.disableProvider, raiseBackoff: true });
    if (outcome.disableProvider) {
      log.error('proveedor_deshabilitado', { proveedor: job.proveedor, codigo: outcome.code });
      metric('ProveedoresDeshabilitados', 1, dims);
    }
    metric('PausasDeProveedor', 1, dims);
    return hold(seconds);
  }

  // Falla del registro, resultado desconocido o falla de la API aislada a este registro: consume un intento.
  if (claim.attempt + 1 >= RETRY.maxAttempts) {
    await exhaust(deps, job, token, outcome, now);
    // La corrida ya quedó cerrada: al volver, el claim la ve terminal y el mensaje se borra.
    return quarantineLeftS > 0 ? hold(quarantineLeftS) : 'DONE';
  }
  const seconds = Math.max(quarantineLeftS, recordBackoff(claim.attempt, random));
  await repo.markWaiting(job, token, outcome.reason, true, now);
  metric('Reintentos', 1, { ...dims, motivo: outcome.reason });
  return hold(seconds);
}

async function exhaust(deps: WorkerDeps, job: Job, token: string, outcome: Extract<Outcome, { kind: 'retryable' }>, now: number): Promise<void> {
  await deps.repo.finish(job, token, 'FAILED_EXHAUSTED', { code: outcome.code, httpStatus: outcome.httpStatus }, now);
  await deps.queue.sendExhausted({ code: outcome.code, execId: job.execId, recordId: job.recordId, proveedor: job.proveedor, runDate: job.runDate });
  log.warn('intentos_agotados', { execId: job.execId, proveedor: job.proveedor, codigo: outcome.code });
  metric('Agotados', 1, { proveedor: job.proveedor });
}

async function bestEffort(event: string, fn: () => Promise<void>): Promise<void> {
  try {
    await fn();
  } catch (error) {
    log.warn(event, { error: error instanceof Error ? error.name : 'desconocido' });
  }
}
