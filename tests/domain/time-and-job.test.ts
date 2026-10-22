import { describe, expect, it } from 'vitest';
import { buildJob, parseJob } from '../../src/domain/job';
import { execIdOf, minuteOfDay, previousDate, slotOf, turnAt, utcDate } from '../../src/domain/time';

describe('slotOf', () => {
  it('coincide con la expresión SQL del índice', () => {
    // md5('abc') = 900150983c… → 0x9001509 = 151000329 → % 1440 = 489
    expect(slotOf('abc')).toBe(489);
  });

  it('reparte los registros en forma pareja a lo largo del día', () => {
    const perHour = new Array<number>(24).fill(0);
    const n = 48_000;
    for (let i = 0; i < n; i++) {
      const hour = Math.floor(slotOf(`registro-${i}`) / 60);
      perHour[hour] = (perHour[hour] ?? 0) + 1;
    }
    // χ² con 23 grados de libertad: el valor crítico al 0,1% es ~49,7.
    const expected = n / 24;
    const chi2 = perHour.reduce((acc, observed) => acc + (observed - expected) ** 2 / expected, 0);
    expect(chi2).toBeLessThan(49.7);
  });

  it('siempre está entre 0 y 1439', () => {
    for (let i = 0; i < 5_000; i++) {
      const s = slotOf(`x${i}`);
      expect(s).toBeGreaterThanOrEqual(0);
      expect(s).toBeLessThan(1_440);
    }
  });
});

describe('fechas UTC', () => {
  it('calcula el turno, el día y el minuto', () => {
    const t = turnAt('2026-10-20', 600);
    expect(new Date(t).toISOString()).toBe('2026-10-20T10:00:00.000Z');
    expect(utcDate(t)).toBe('2026-10-20');
    expect(minuteOfDay(t)).toBe(600);
    expect(previousDate('2026-03-01')).toBe('2026-02-28');
  });
});

describe('parseJob', () => {
  const job = buildJob({ proveedor: 'edenor', date: '2026-10-20', slot: 600, id: 'reg-1', endpoint: '/consultas/saldo', body: { cuenta: 1 } });

  it('acepta un mensaje válido', () => {
    expect(parseJob(JSON.stringify(job))).toEqual(job);
  });

  it('rechaza un execId que no corresponde al registro y al día', () => {
    expect(parseJob(JSON.stringify({ ...job, execId: execIdOf('otro', '2026-10-20') }))).toBeUndefined();
    expect(parseJob(JSON.stringify({ ...job, runDate: '2026-10-21' }))).toBeUndefined();
  });

  it('rechaza mensajes mal formados', () => {
    expect(parseJob('no es json')).toBeUndefined();
    expect(parseJob('[]')).toBeUndefined();
    expect(parseJob(JSON.stringify({ ...job, schemaVersion: 2 }))).toBeUndefined();
    expect(parseJob(JSON.stringify({ ...job, slot: 1_440 }))).toBeUndefined();
    expect(parseJob(JSON.stringify({ ...job, proveedor: '' }))).toBeUndefined();
    const badDate = buildJob({ proveedor: 'edenor', date: '2026-13-99', slot: 1, id: 'reg-1', endpoint: '/consultas', body: {} });
    expect(parseJob(JSON.stringify(badDate))).toBeUndefined();
    const { body: _body, ...withoutBody } = job;
    expect(parseJob(JSON.stringify(withoutBody))).toBeUndefined();
  });
});
