import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { dynamoClient, LOCAL } from '../../local/env';
import { DynamoExecutionsRepo } from '../../src/adapters/dynamo';
import { buildJob } from '../../src/domain/job';
import { EMPTY_STATS } from '../../src/domain/types';

const repo = new DynamoExecutionsRepo(LOCAL.table, dynamoClient());
const T0 = Date.parse('2026-10-20T10:00:00Z');
const job = () => buildJob({ proveedor: 'edenor', date: '2026-10-20', slot: 600, id: `int-${randomUUID()}`, endpoint: '/consultas/saldo', body: '{}' });

describe('DynamoDB Local: escrituras condicionales reales', () => {
  it('claim, fencing y cierre', async () => {
    const j = job();
    const first = await repo.claim(j, T0);
    expect(first.status).toBe('CLAIMED');
    // Otro worker con el lease vigente: BUSY.
    expect(await repo.claim(j, T0 + 1_000)).toMatchObject({ status: 'BUSY' });
    // Lease vencido: un segundo worker reclama y cuenta un intento.
    const second = await repo.claim(j, T0 + 200_000);
    expect(second).toMatchObject({ status: 'CLAIMED', attempt: 1 });
    // El primero (zombie) ya no puede cerrar: fencing por token.
    if (first.status !== 'CLAIMED' || second.status !== 'CLAIMED') throw new Error('claim');
    await expect(repo.finish(j, first.token, 'SUCCEEDED', { httpStatus: 200 }, T0 + 201_000)).rejects.toThrow();
    await repo.finish(j, second.token, 'SUCCEEDED', { httpStatus: 200, result: '{"ok":true}' }, T0 + 201_000);
    // Un duplicado de una corrida terminada se descarta.
    expect(await repo.claim(j, T0 + 300_000)).toEqual({ status: 'TERMINAL' });
  });

  it('el ledger del Dispatcher nunca pisa un estado posterior', async () => {
    const j = job();
    const claim = await repo.claim(j, T0);
    if (claim.status !== 'CLAIMED') throw new Error('claim');
    await repo.ensureEnqueued(j, T0); // llega tarde: la fila ya está en curso
    expect(await repo.claim(j, T0 + 1_000)).toMatchObject({ status: 'BUSY' });
  });

  it('WAITING sin gastar intento y vuelta al claim', async () => {
    const j = job();
    const c1 = await repo.claim(j, T0);
    if (c1.status !== 'CLAIMED') throw new Error('claim');
    await repo.markWaiting(j, c1.token, 'provider', false, T0);
    const c2 = await repo.claim(j, T0 + 5_000);
    expect(c2).toMatchObject({ status: 'CLAIMED', attempt: 0 });
  });

  it('el cursor se guarda con bloqueo optimista', async () => {
    const proveedor = `prov-${randomUUID()}`;
    expect(await repo.createProviderIfMissing(proveedor)).toBe(true);
    expect(await repo.createProviderIfMissing(proveedor)).toBe(false);
    const a = { date: '2026-10-20', slot: 10, id: 'x' };
    const b = { date: '2026-10-20', slot: 20, id: 'y' };
    expect(await repo.saveCursor(proveedor, undefined, a)).toBe(true);
    expect(await repo.saveCursor(proveedor, undefined, b)).toBe(false); // otro tick ya lo movió
    expect(await repo.saveCursor(proveedor, a, b)).toBe(true);
    const view = await repo.getProviderView(proveedor, T0);
    expect(view.state).toMatchObject({ enabled: false, allowlist: [], cursor: b });
  });

  it('las estadísticas del breaker suman las dos ventanas y el conjunto de ids tiene tope', async () => {
    const proveedor = `prov-${randomUUID()}`;
    await repo.createProviderIfMissing(proveedor);
    const failure = { kind: 'retryable' as const, reason: 'record' as const, code: 'X' };
    for (let i = 0; i < 120; i++) await repo.recordCall(proveedor, `r-${i}`, failure, 10, T0);
    const view = await repo.getProviderView(proveedor, T0);
    expect(view.stats).toMatchObject({ ...EMPTY_STATS, calls: 120, fails: 120, failRecords: 100 });
  });
});
