import { describe, expect, it } from 'vitest';
import { decodeCursor, encodeCursor, exhaustedCursor, pendingWork } from '../../src/domain/cursor';

const NOW = Date.parse('2026-10-20T10:00:30.000Z'); // minuto 600

describe('pendingWork', () => {
  it('un proveedor nuevo arranca en el minuto siguiente', () => {
    const work = pendingWork('edenor', undefined, NOW);
    expect(work.ranges).toEqual([]);
    expect(work.startCursor).toEqual({ date: '2026-10-20', slot: 601, id: '' });
  });

  it('con el cursor de hoy, despacha hasta el minuto actual', () => {
    const work = pendingWork('edenor', { date: '2026-10-20', slot: 590, id: 'abc' }, NOW);
    expect(work.ranges).toEqual([{ proveedor: 'edenor', date: '2026-10-20', fromSlot: 590, fromId: 'abc', toSlot: 600 }]);
    expect(work.skipped).toBeUndefined();
  });

  it('con el cursor de ayer todavía vigente, termina ayer y sigue con hoy', () => {
    const work = pendingWork('edenor', { date: '2026-10-19', slot: 1_400, id: 'x' }, NOW);
    expect(work.ranges).toEqual([
      { proveedor: 'edenor', date: '2026-10-19', fromSlot: 1_400, fromId: 'x', toSlot: 1_439 },
      { proveedor: 'edenor', date: '2026-10-20', fromSlot: 0, fromId: '', toSlot: 600 },
    ]);
    expect(work.skipped).toBeUndefined();
  });

  it('si el cursor de ayer quedó en algo vencido, lo saltea y lo cuenta', () => {
    const work = pendingWork('edenor', { date: '2026-10-19', slot: 100, id: 'x' }, NOW);
    expect(work.skipped).toEqual({ proveedor: 'edenor', date: '2026-10-19', fromSlot: 100, fromId: 'x', toSlot: 600 });
    expect(work.startCursor).toEqual({ date: '2026-10-19', slot: 601, id: '' });
    expect(work.ranges).toEqual([
      { proveedor: 'edenor', date: '2026-10-19', fromSlot: 601, fromId: '', toSlot: 1_439 },
      { proveedor: 'edenor', date: '2026-10-20', fromSlot: 0, fromId: '', toSlot: 600 },
    ]);
  });

  it('un cursor de hace más de un día se reinicia desde lo que todavía vale de ayer', () => {
    const work = pendingWork('edenor', { date: '2026-10-15', slot: 10, id: 'x' }, NOW);
    expect(work.skipped?.date).toBe('2026-10-19');
    expect(work.startCursor).toEqual({ date: '2026-10-19', slot: 601, id: '' });
  });

  it('a las 23:59 no deja un rango de ayer vacío', () => {
    const late = Date.parse('2026-10-20T23:59:10.000Z');
    const work = pendingWork('edenor', { date: '2026-10-19', slot: 5, id: 'x' }, late);
    expect(work.ranges).toEqual([{ proveedor: 'edenor', date: '2026-10-20', fromSlot: 0, fromId: '', toSlot: 1_439 }]);
  });
});

describe('codificación del cursor', () => {
  it('ida y vuelta, aunque el id tenga el separador', () => {
    const cursor = { date: '2026-10-20', slot: 7, id: 'a|b|c' };
    expect(decodeCursor(encodeCursor(cursor))).toEqual(cursor);
    expect(decodeCursor(undefined)).toBeUndefined();
    expect(exhaustedCursor(NOW)).toEqual({ date: '2026-10-20', slot: 601, id: '' });
  });
});
