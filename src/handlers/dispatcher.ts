import { JOB_MAX_BYTES, SWEEP_AFTER_MS } from '../config';
import { capFor, fitToBudget } from '../domain/admission';
import { exhaustedCursor, pendingWork, type Range } from '../domain/cursor';
import { buildJob, isDispatchable, type DueRow } from '../domain/job';
import { minuteOfDay } from '../domain/time';
import type { Cursor, Job, ProviderState } from '../domain/types';
import { log, metric } from '../observability';
import type { Clock, ExecutionsRepo, JobQueue, SourceDb } from '../ports';

export interface DispatcherDeps {
  repo: ExecutionsRepo;
  source: SourceDb;
  queue: JobQueue;
  clock: Clock;
}

interface Plan {
  provider: ProviderState;
  cursor: Cursor | undefined;
  room: number;
  ranges: Range[];
}

/**
 * Dispatcher (docs/ARQUITECTURA.md §3.1). Corre cada minuto con reserved concurrency 1.
 * Orden por registro: enviar → ledger condicional → cursor. Cualquier caída en el medio se repara
 * sola en el tick siguiente: SQS deduplica y el claim descarta lo repetido. Un proveedor que falla
 * no frena a los demás.
 */
export function createDispatcher(deps: DispatcherDeps) {
  return async (): Promise<void> => {
    const now = deps.clock.now();
    const global = await deps.repo.getGlobalView(now);
    if (global.pausedUntil > now) {
      log.warn('despacho_pausado', { hasta: new Date(global.pausedUntil).toISOString() });
      return;
    }

    const providers = (await deps.repo.listProviders()).filter((p) => p.enabled);
    if (minuteOfDay(now) % 60 === 0) await sweep(deps, providers, now);

    const active = providers.filter((p) => p.pausedUntil <= now);
    const budget = fitToBudget(new Map(active.map((p) => [p.proveedor, capFor(p.occupancyS, p.lanes)])));
    if (budget.overBudget) {
      // Ni con el mínimo por proveedor entra en la cota: hay que repartir proveedores en más colas.
      log.error('presupuesto_de_cola_excedido', { proveedores: active.length });
      metric('PresupuestoDeColaExcedido', 1);
    }

    const plans: Plan[] = [];
    for (const provider of active) {
      const plan = await isolated(provider.proveedor, () => planProvider(deps, provider, budget.caps.get(provider.proveedor) ?? 0, now));
      if (plan) plans.push(plan);
    }
    if (plans.length === 0) return;

    const rows = await deps.source.fetchDue(plans.flatMap((p) => p.ranges.map((r) => ({ ...r, limit: p.room }))));
    const byProvider = groupByProvider(rows);
    let enqueued = 0;
    for (const plan of plans) {
      const sent = await isolated(plan.provider.proveedor, () => dispatchProvider(deps, plan, byProvider.get(plan.provider.proveedor) ?? [], now));
      enqueued += sent ?? 0;
    }
    metric('Encolados', enqueued);
  };
}

async function planProvider(deps: DispatcherDeps, provider: ProviderState, cap: number, now: number): Promise<Plan | undefined> {
  const work = pendingWork(provider.proveedor, provider.cursor, now);
  let cursor = provider.cursor;

  if (work.skipped) {
    const count = await deps.source.countRange(work.skipped);
    if (count > 0) {
      await deps.repo.addSkipped(provider.proveedor, work.skipped.date, count);
      metric('Salteados', count, { proveedor: provider.proveedor });
      log.warn('registros_vencidos_salteados', { proveedor: provider.proveedor, cantidad: count });
    }
  }
  if (work.startCursor) {
    if (!(await deps.repo.saveCursor(provider.proveedor, cursor, work.startCursor))) return undefined;
    cursor = work.startCursor;
  }

  const room = cap - (await deps.repo.countOpen(provider.proveedor));
  if (room <= 0 || work.ranges.length === 0) return undefined;
  return { provider, cursor, room, ranges: work.ranges };
}

async function dispatchProvider(deps: DispatcherDeps, plan: Plan, rows: DueRow[], now: number): Promise<number> {
  const { proveedor } = plan.provider;
  const taken = rows.slice(0, plan.room);
  const jobs: Job[] = [];
  const discarded = new Map<string, number>();
  for (const row of taken) {
    const job = buildJob(row);
    if (!isDispatchable(job) || Buffer.byteLength(JSON.stringify(job), 'utf8') > JOB_MAX_BYTES) {
      // No entra en un mensaje o no pasaría la validación del worker: se cuenta como salteado
      // (cobertura) y no traba el cursor.
      discarded.set(row.date, (discarded.get(row.date) ?? 0) + 1);
      log.error('registro_no_despachable', { execId: job.execId, proveedor });
      continue;
    }
    jobs.push(job);
  }
  for (const [date, count] of discarded) {
    await deps.repo.addSkipped(proveedor, date, count);
    metric('RegistrosNoDespachables', count, { proveedor });
  }

  const sent = await deps.queue.send(jobs);
  await inChunks(
    jobs.filter((j) => sent.has(j.execId)),
    25,
    (job) => deps.repo.ensureEnqueued(job, now),
  );

  // El cursor avanza hasta el último mensaje enviado en orden. Si se envió todo y no quedaba más,
  // salta al minuto siguiente.
  const byKey = new Map(jobs.map((j) => [`${j.recordId}|${j.runDate}`, j]));
  let next: Cursor | undefined;
  let allSent = true;
  for (const row of taken) {
    const job = byKey.get(`${row.id}|${row.date}`);
    if (job && !sent.has(job.execId)) {
      allSent = false;
      break;
    }
    next = { date: row.date, slot: row.slot, id: row.id };
  }
  if (allSent && rows.length < plan.room) next = exhaustedCursor(now);
  if (next && !(await deps.repo.saveCursor(proveedor, plan.cursor, next))) {
    log.warn('cursor_concurrente', { proveedor });
  }
  return sent.size;
}

/** Barrido horario: cierra lo que siga abierto pasadas 25 h y libera capacidad de admisión. */
async function sweep(deps: DispatcherDeps, providers: ProviderState[], now: number): Promise<void> {
  for (const provider of providers) {
    await isolated(provider.proveedor, async () => {
      const stale = await deps.repo.findStale(provider.proveedor, now - SWEEP_AFTER_MS);
      await inChunks(stale, 25, (run) => deps.repo.supersede(run, now));
      if (stale.length > 0) metric('BarridoSuperadas', stale.length, { proveedor: provider.proveedor });
    });
  }
}

/** Un error con un proveedor se registra y no frena a los demás. */
async function isolated<T>(proveedor: string, fn: () => Promise<T>): Promise<T | undefined> {
  try {
    return await fn();
  } catch (error) {
    log.error('error_por_proveedor', { proveedor, error: error instanceof Error ? error.name : 'desconocido' });
    metric('ErroresPorProveedor', 1, { proveedor });
    return undefined;
  }
}

function groupByProvider(rows: DueRow[]): Map<string, DueRow[]> {
  const map = new Map<string, DueRow[]>();
  for (const row of rows) {
    const list = map.get(row.proveedor) ?? [];
    list.push(row);
    map.set(row.proveedor, list);
  }
  return map;
}

async function inChunks<T>(items: readonly T[], size: number, fn: (item: T) => Promise<void>): Promise<void> {
  for (let i = 0; i < items.length; i += size) await Promise.all(items.slice(i, i + size).map(fn));
}
