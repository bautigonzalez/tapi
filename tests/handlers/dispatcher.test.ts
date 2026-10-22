import { describe, expect, it } from 'vitest';
import { slotOf, turnAt } from '../../src/domain/time';
import { createDispatcher } from '../../src/handlers/dispatcher';
import { FakeClock, FakeQueue, FakeRepo, FakeSource, type SourceRow } from '../fakes';

const NOW = Date.parse('2026-10-20T10:00:30.000Z'); // minuto 600, hora en punto: corre el barrido

/** Registros del proveedor con el slot en [from, to]. */
function rowsInSlots(proveedor: string, from: number, to: number, max = 50): SourceRow[] {
  const rows: SourceRow[] = [];
  for (let i = 0; rows.length < max && i < 200_000; i++) {
    const id = `${proveedor}-${i}`;
    const slot = slotOf(id);
    if (slot >= from && slot <= to) rows.push({ proveedor, id, endpoint: '/consultas/saldo', body: { i } });
  }
  return rows;
}

function setup(rows: SourceRow[]) {
  const clock = new FakeClock(NOW);
  const repo = new FakeRepo();
  const queue = new FakeQueue();
  const source = new FakeSource(rows);
  const dispatch = createDispatcher({ repo, source, queue, clock });
  return { clock, repo, queue, source, dispatch };
}

describe('dispatcher', () => {
  it('un proveedor nuevo arranca en el minuto siguiente y no despacha lo que ya pasó', async () => {
    const { dispatch, repo, queue } = setup(rowsInSlots('edenor', 0, 600, 5));
    repo.addProvider({ proveedor: 'edenor' });
    await dispatch();
    expect(queue.sent).toHaveLength(0);
    expect(repo.providers.get('edenor')?.cursor).toEqual({ date: '2026-10-20', slot: 601, id: '' });
  });

  it('despacha en orden lo que vence hasta el minuto actual, escribe el ledger y avanza el cursor', async () => {
    const due = rowsInSlots('edenor', 591, 600, 5);
    const future = rowsInSlots('edenor', 700, 800, 3);
    const { dispatch, repo, queue } = setup([...due, ...future]);
    repo.addProvider({ proveedor: 'edenor', cursor: { date: '2026-10-20', slot: 590, id: '' } });
    await dispatch();

    expect(queue.sent.map((j) => j.recordId).sort()).toEqual(due.map((r) => r.id).sort());
    const slots = queue.sent.map((j) => j.slot);
    expect(slots).toEqual([...slots].sort((a, b) => a - b));
    for (const job of queue.sent) {
      expect(job).toMatchObject({ proveedor: 'edenor', runDate: '2026-10-20' });
      expect(repo.run(job)?.status).toBe('ENQUEUED');
    }
    expect(repo.providers.get('edenor')?.cursor).toEqual({ date: '2026-10-20', slot: 601, id: '' });
  });

  it('admisión: nunca encola más que el tope menos las abiertas', async () => {
    const { dispatch, repo, queue } = setup(rowsInSlots('edenor', 500, 600, 20));
    // 180 s por mensaje → tope de 5. Ya hay 3 abiertas → entran 2.
    repo.addProvider({ proveedor: 'edenor', occupancyS: 180, cursor: { date: '2026-10-20', slot: 499, id: '' } });
    for (let i = 0; i < 3; i++) {
      repo.setRun({ recordId: `abierta-${i}`, runDate: '2026-10-20' }, { status: 'WAITING', attempt: 0, proveedor: 'edenor', turno: NOW });
    }
    await dispatch();
    expect(queue.sent).toHaveLength(2);
    const last = queue.sent[1]!;
    expect(repo.providers.get('edenor')?.cursor).toEqual({ date: '2026-10-20', slot: last.slot, id: last.recordId });
  });

  it('envío parcial: el cursor se detiene antes del primer mensaje rechazado', async () => {
    const rows = rowsInSlots('edenor', 595, 600, 3);
    const { dispatch, repo, queue, source } = setup(rows);
    repo.addProvider({ proveedor: 'edenor', cursor: { date: '2026-10-20', slot: 594, id: '' } });
    const ordered = await source.fetchDue([{ proveedor: 'edenor', date: '2026-10-20', fromSlot: 594, fromId: '', toSlot: 600, limit: 10 }]);
    const { execIdOf } = await import('../../src/domain/time');
    queue.rejected.add(execIdOf(ordered[1]!.id, '2026-10-20'));
    await dispatch();
    expect(repo.providers.get('edenor')?.cursor).toEqual({ date: '2026-10-20', slot: ordered[0]!.slot, id: ordered[0]!.id });
    expect(repo.run({ recordId: ordered[1]!.id, runDate: '2026-10-20' })).toBeUndefined();
  });

  it('cambio de día: saltea lo vencido de ayer, lo cuenta y sigue con lo vigente', async () => {
    const stale = rowsInSlots('edenor', 101, 600, 4);
    const stillValid = rowsInSlots('edenor', 601, 1_439, 2);
    const { dispatch, repo, queue } = setup([...stale, ...stillValid]);
    repo.addProvider({ proveedor: 'edenor', cursor: { date: '2026-10-19', slot: 100, id: '' } });
    await dispatch();
    expect(repo.skipped).toEqual([{ proveedor: 'edenor', date: '2026-10-19', count: 4 }]);
    const yesterday = queue.sent.filter((j) => j.runDate === '2026-10-19');
    expect(yesterday.map((j) => j.recordId).sort()).toEqual(stillValid.map((r) => r.id).sort());
  });

  it('pausa global o proveedor deshabilitado: no despacha', async () => {
    const { dispatch, repo, queue, clock } = setup(rowsInSlots('edenor', 591, 600, 3));
    repo.addProvider({ proveedor: 'edenor', cursor: { date: '2026-10-20', slot: 590, id: '' } });
    repo.global.pausedUntil = clock.now() + 60_000;
    await dispatch();
    expect(queue.sent).toHaveLength(0);

    repo.global.pausedUntil = 0;
    repo.providers.get('edenor')!.enabled = false;
    await dispatch();
    expect(queue.sent).toHaveLength(0);
  });

  it('barrido horario: cierra lo que sigue abierto pasadas 25 h', async () => {
    const { dispatch, repo } = setup([]);
    repo.addProvider({ proveedor: 'edenor', cursor: { date: '2026-10-20', slot: 601, id: '' } });
    const old = { recordId: 'vieja', runDate: '2026-10-19' };
    repo.setRun(old, { status: 'WAITING', attempt: 1, proveedor: 'edenor', turno: turnAt('2026-10-19', 0) - 3_600_000 });
    await dispatch();
    expect(repo.run(old)?.status).toBe('SUPERSEDED');
  });
});
