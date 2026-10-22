import { describe, expect, it } from 'vitest';
import { TIMEOUTS } from '../../src/config';
import type { CallResult } from '../../src/domain/classify';
import { turnAt } from '../../src/domain/time';
import type { Job } from '../../src/domain/types';
import { EMPTY_STATS } from '../../src/domain/types';
import { createWorker } from '../../src/handlers/worker';
import { FakeApi, FakeClock, FakeQueue, FakeRepo, FakeStore, RUN_DATE, SLOT, apiError, httpError, makeJob, ok, sqsEvent } from '../fakes';

function setup(respond: (job: Job, n: number) => CallResult | Promise<CallResult>, opts: { leaseInApi?: boolean } = {}) {
  const clock = new FakeClock(turnAt(RUN_DATE, SLOT) + 60_000);
  const repo = new FakeRepo();
  repo.addProvider({ proveedor: 'edenor' });
  const queue = new FakeQueue();
  const api = new FakeApi(respond);
  const store = new FakeStore();
  const worker = createWorker({
    repo,
    queue,
    api,
    store,
    clock,
    config: { apiBaseUrl: 'https://abc123.execute-api.us-east-1.amazonaws.com/prod', leaseInApi: opts.leaseInApi ?? true },
    random: () => 0.5,
  });
  return { clock, repo, queue, api, store, worker };
}

describe('worker: camino feliz', () => {
  it('guarda SUCCEEDED, firma contra el stage y libera el grupo', async () => {
    const { worker, repo, api } = setup(() => ok());
    const job = makeJob();
    const res = await worker(sqsEvent([job]));
    expect(res.batchItemFailures).toEqual([]);
    expect(repo.run(job)?.status).toBe('SUCCEEDED');
    expect(repo.run(job)?.details?.result).toBe('{"saldo":100}');
    expect(api.calls[0]?.url.href).toBe('https://abc123.execute-api.us-east-1.amazonaws.com/prod/consultas/saldo');
  });

  it('una respuesta de más de 8 KB va a S3 y la ejecución guarda solo la clave', async () => {
    const { worker, repo, store } = setup(() => ok(JSON.stringify({ data: 'x'.repeat(9_000) })));
    const job = makeJob();
    await worker(sqsEvent([job]));
    expect(store.puts).toHaveLength(1);
    expect(repo.run(job)?.details).toMatchObject({ resultKey: store.puts[0] });
    expect(repo.run(job)?.details?.result).toBeUndefined();
  });

  it('un duplicado de una corrida terminada se descarta sin llamar a la API', async () => {
    const { worker, repo, api } = setup(() => ok());
    const job = makeJob();
    repo.setRun(job, { status: 'SUCCEEDED', attempt: 0, proveedor: 'edenor', turno: 0 });
    const res = await worker(sqsEvent([job]));
    expect(res.batchItemFailures).toEqual([]);
    expect(api.calls).toHaveLength(0);
  });
});

describe('worker: errores', () => {
  it('dato inválido: FAILED_PERMANENT sin reintentar', async () => {
    const { worker, repo, queue } = setup(() => apiError(404, 'NOT_FOUND', false, 'record'));
    const job = makeJob();
    const res = await worker(sqsEvent([job]));
    expect(res.batchItemFailures).toEqual([]);
    expect(repo.run(job)).toMatchObject({ status: 'FAILED_PERMANENT', details: { code: 'NOT_FOUND' } });
    expect(queue.holds).toHaveLength(0);
  });

  it('error del registro: reintenta dentro del grupo con 15/30/60 s y al cuarto intento va a agotados', async () => {
    const { worker, repo, queue } = setup(() => apiError(500, 'PARSE', true, 'record'));
    const job = makeJob();
    for (let i = 0; i < 3; i++) {
      const res = await worker(sqsEvent([job]));
      expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'm0' }]);
      expect(repo.run(job)).toMatchObject({ status: 'WAITING', waitingReason: 'record', attempt: i + 1 });
    }
    expect(queue.holds.map((h) => h.seconds)).toEqual([12, 23, 45]);
    const last = await worker(sqsEvent([job]));
    expect(last.batchItemFailures).toEqual([]);
    expect(repo.run(job)?.status).toBe('FAILED_EXHAUSTED');
    expect(queue.exhausted).toEqual([{ code: 'PARSE', execId: job.execId, recordId: 'reg-1', proveedor: 'edenor', runDate: RUN_DATE }]);
  });

  it('error del proveedor: pausa al proveedor sin gastar intentos del registro', async () => {
    const { worker, repo, queue, clock } = setup(() => apiError(503, 'DOWN', true, 'provider'));
    const job = makeJob();
    const res = await worker(sqsEvent([job]));
    expect(res.batchItemFailures).toHaveLength(1);
    expect(repo.run(job)).toMatchObject({ status: 'WAITING', waitingReason: 'provider', attempt: 0 });
    const state = repo.providers.get('edenor');
    expect(state?.pausedUntil).toBeGreaterThan(clock.now());
    expect(state?.backoffLevel).toBe(1);
    expect(queue.holds[0]?.seconds).toBe(4);
  });

  it('respeta el Retry-After del proveedor', async () => {
    const { worker, queue } = setup(() => httpError(429, { code: 'SLOW', retryable: true, scope: 'provider', origin: 'provider' }, '30'));
    await worker(sqsEvent([makeJob()]));
    expect(queue.holds[0]?.seconds).toBe(30);
  });

  it('423 (lease ocupado): espera sin gastar intento', async () => {
    const { worker, repo } = setup(() => httpError(423, undefined, '20'));
    const job = makeJob();
    await worker(sqsEvent([job]));
    expect(repo.run(job)).toMatchObject({ status: 'WAITING', waitingReason: 'provider', attempt: 0 });
  });

  it('401 del proveedor: lo deshabilita y no quema el registro', async () => {
    const { worker, repo } = setup(() => apiError(401, 'AUTH', false, 'provider'));
    const job = makeJob();
    await worker(sqsEvent([job]));
    expect(repo.providers.get('edenor')?.enabled).toBe(false);
    expect(repo.run(job)).toMatchObject({ status: 'WAITING', attempt: 0 });
  });

  it('API caída (falla en varios registros y proveedores): pausa global, el registro no se quema', async () => {
    const { worker, repo, queue, clock } = setup(() => httpError(403));
    repo.global.stats = { ...EMPTY_STATS, apiFailRecords: 8, apiFailProviders: 3 };
    const job = makeJob();
    await worker(sqsEvent([job]));
    expect(repo.global.pausedUntil).toBeGreaterThan(clock.now());
    expect(repo.run(job)).toMatchObject({ status: 'WAITING', waitingReason: 'api', attempt: 0 });
    expect(queue.holds[0]?.seconds).toBe(60);
  });

  it('registro envenenado (403 de una ruta inexistente): no pausa el sistema, agota sus intentos', async () => {
    const { worker, repo, queue } = setup(() => httpError(403));
    const job = makeJob();
    for (let i = 0; i < 4; i++) await worker(sqsEvent([job]));
    expect(repo.global.pausedUntil).toBe(0);
    expect(repo.run(job)?.status).toBe('FAILED_EXHAUSTED');
    expect(queue.exhausted).toHaveLength(1);
  });

  it('desconocido sin lease en la API: cuarentena de 90 s contada desde el envío', async () => {
    let clockRef: FakeClock | undefined;
    const { worker, queue, clock } = setup(
      () => {
        clockRef?.advance(TIMEOUTS.httpMs);
        return { type: 'timeout' };
      },
      { leaseInApi: false },
    );
    clockRef = clock;
    await worker(sqsEvent([makeJob()]));
    expect(queue.holds[0]?.seconds).toBe(60);
  });

  it('desconocido con lease en la API: backoff normal, el lease protege al proveedor', async () => {
    const { worker, queue } = setup(() => ({ type: 'timeout' }), { leaseInApi: true });
    await worker(sqsEvent([makeJob()]));
    expect(queue.holds[0]?.seconds).toBe(12);
  });

  it('breaker abierto: las fallas de registro pasan a ser del proveedor', async () => {
    const { worker, repo } = setup(() => apiError(500, 'X', true, 'record'));
    repo.providerStats.set('edenor', { ...EMPTY_STATS, calls: 30, fails: 20, failRecords: 12 });
    const job = makeJob();
    await worker(sqsEvent([job]));
    expect(repo.run(job)).toMatchObject({ waitingReason: 'provider', attempt: 0 });
  });

  it('breaker de permanentes: un salto de FAILED_PERMANENT pausa el despacho', async () => {
    const { worker, repo, clock } = setup(() => apiError(400, 'BAD', false, 'record'));
    repo.global.stats = { ...EMPTY_STATS, calls: 100, fails: 30, failRecords: 30, permanents: 30 };
    await worker(sqsEvent([makeJob()]));
    expect(repo.global.pausedUntil).toBeGreaterThan(clock.now());
  });
});

describe('worker: exclusión y estados', () => {
  it('proveedor en pausa: retiene el grupo sin reclamar ni llamar', async () => {
    const { worker, repo, queue, api, clock } = setup(() => ok());
    repo.providers.get('edenor')!.pausedUntil = clock.now() + 10_000;
    const job = makeJob();
    await worker(sqsEvent([job]));
    expect(queue.holds[0]?.seconds).toBe(10);
    expect(repo.run(job)).toBeUndefined();
    expect(api.calls).toHaveLength(0);
  });

  it('proveedor deshabilitado o sin configurar: retiene 15 min', async () => {
    const { worker, queue } = setup(() => ok());
    await worker(sqsEvent([makeJob('reg-1', 'nuevo')]));
    expect(queue.holds[0]?.seconds).toBe(900);
  });

  it('otro worker con lease vigente: devuelve el mensaje sin descartarlo', async () => {
    const { worker, repo, queue, api, clock } = setup(() => ok());
    const job = makeJob();
    repo.setRun(job, { status: 'IN_PROGRESS', attempt: 0, proveedor: 'edenor', turno: 0, leaseUntil: clock.now() + 50_000 });
    const res = await worker(sqsEvent([job]));
    expect(res.batchItemFailures).toHaveLength(1);
    expect(queue.holds[0]?.seconds).toBe(50);
    expect(api.calls).toHaveLength(0);
  });

  it('fencing: un worker sin el claim vigente no puede cerrar la corrida', async () => {
    const { repo } = setup(() => ok());
    const job = makeJob();
    const first = await repo.claim(job, 0);
    if (first.status !== 'CLAIMED') throw new Error('claim');
    const second = await repo.claim(job, 10 * 60_000);
    expect(second.status).toBe('CLAIMED');
    await expect(repo.finish(job, first.token, 'SUCCEEDED', {})).rejects.toThrow();
  });

  it('worker caído: se reclama con el lease vencido y cuenta un intento', async () => {
    const { worker, repo, clock } = setup(() => ok());
    const job = makeJob();
    repo.setRun(job, { status: 'IN_PROGRESS', attempt: 0, proveedor: 'edenor', turno: 0, leaseUntil: clock.now() - 1 });
    await worker(sqsEvent([job]));
    expect(repo.run(job)).toMatchObject({ status: 'SUCCEEDED', attempt: 1 });
  });

  it('turno vencido hace más de 24 h: SUPERSEDED sin llamar', async () => {
    const { worker, repo, api, clock } = setup(() => ok());
    clock.t = turnAt(RUN_DATE, SLOT) + 25 * 3_600_000;
    const job = makeJob();
    await worker(sqsEvent([job]));
    expect(repo.run(job)?.status).toBe('SUPERSEDED');
    expect(api.calls).toHaveLength(0);
  });

  it('endpoint fuera de la allowlist: FAILED_PERMANENT sin llamar', async () => {
    const { worker, repo, api } = setup(() => ok());
    const job = makeJob('reg-1', 'edenor', '/admin/usuarios');
    await worker(sqsEvent([job]));
    expect(repo.run(job)).toMatchObject({ status: 'FAILED_PERMANENT', details: { code: 'ENDPOINT_NOT_ALLOWED' } });
    expect(api.calls).toHaveLength(0);
  });

  it('mensaje inválido o adulterado: va a agotados sin llamar', async () => {
    const { worker, queue, api } = setup(() => ok());
    const tampered = { ...makeJob(), recordId: 'otro-registro' };
    await worker(sqsEvent(['no es json']));
    await worker(sqsEvent([JSON.stringify(tampered)]));
    expect(queue.exhausted.map((e) => e.code)).toEqual(['INVALID_MESSAGE', 'INVALID_MESSAGE']);
    expect(api.calls).toHaveLength(0);
  });

  it('en un lote FIFO, después de un fallo los mensajes siguientes también vuelven', async () => {
    const { worker, api } = setup(() => apiError(503, 'DOWN', true, 'provider'));
    const res = await worker(sqsEvent([makeJob('reg-1'), makeJob('reg-2')]));
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'm0' }, { itemIdentifier: 'm1' }]);
    expect(api.calls).toHaveLength(1);
  });
});
