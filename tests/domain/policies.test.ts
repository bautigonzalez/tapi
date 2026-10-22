import { describe, expect, it } from 'vitest';
import { capacityStatus, capFor, fitToBudget, utilization } from '../../src/domain/admission';
import { backoff, providerHold, recordBackoff } from '../../src/domain/backoff';
import { apiBreakerOpen, permanentBreakerOpen, providerBreakerOpen } from '../../src/domain/breaker';
import { EMPTY_STATS } from '../../src/domain/types';
import { chunkByBytes } from '../../src/adapters/sqs';
import { countOverlaps } from '../../src/domain/overlaps';

describe('backoff', () => {
  it('reintentos de un registro: 15, 30 y 60 s con equal jitter', () => {
    expect([0, 1, 2].map((n) => recordBackoff(n, () => 0))).toEqual([8, 15, 30]);
    expect([0, 1, 2].map((n) => recordBackoff(n, () => 1))).toEqual([15, 30, 60]);
    expect(recordBackoff(10, () => 1)).toBe(60);
  });

  it('un registro defectuoso retiene a su proveedor como máximo 105 s en esperas', () => {
    const worst = [0, 1, 2].reduce((acc, n) => acc + recordBackoff(n, () => 1), 0);
    expect(worst).toBe(105);
  });

  it('la retención por proveedor respeta Retry-After y nunca pasa de 15 min', () => {
    expect(providerHold(0, 30, 1, () => 0)).toBe(30);
    expect(providerHold(20, undefined, 1, () => 1)).toBe(900);
    expect(backoff(1_000, 5, 900, () => 1)).toBe(900);
  });

  it('con Retry-After: 5 durante 24 h, nunca supera maxReceiveCount = 1.000', () => {
    let elapsed = 0;
    let receives = 0;
    while (elapsed < 86_400) {
      receives++;
      elapsed += providerHold(0, 5, receives, () => 0);
    }
    expect(receives).toBeLessThanOrEqual(820);
  });
});

describe('admisión', () => {
  it('el tope por proveedor son 15 minutos de trabajo, entre 5 y 2.000', () => {
    expect(capFor(1)).toBe(900);
    expect(capFor(0.1)).toBe(2_000);
    expect(capFor(500)).toBe(5);
    expect(capFor(1, 3)).toBe(2_000);
  });

  it('escala los topes para no superar la cota global por cola', () => {
    const caps = new Map(Array.from({ length: 100 }, (_, i) => [`p${i}`, 2_000] as const));
    const fitted = fitToBudget(caps);
    const total = [...fitted.caps.values()].reduce((a, b) => a + b, 0);
    expect(total).toBeLessThanOrEqual(60_000);
    expect(fitted.overBudget).toBe(false);
  });

  it('avisa cuando ni el mínimo por proveedor entra', () => {
    const caps = new Map(Array.from({ length: 13_000 }, (_, i) => [`p${i}`, 5] as const));
    expect(fitToBudget(caps).overBudget).toBe(true);
  });

  it('ρ de la tabla de la §6', () => {
    expect(utilization(400_000, 0.55)).toBeCloseTo(2.55, 2);
    expect(utilization(400_000, 0.55, 4)).toBeCloseTo(0.64, 2);
    expect(utilization(120_000, 0.6)).toBeCloseTo(0.83, 2);
    expect(capacityStatus(2.55)).toBe('INVIABLE');
    expect(capacityStatus(0.83)).toBe('ALERTA');
    expect(capacityStatus(0.59)).toBe('OK');
  });
});

describe('breakers', () => {
  it('un proveedor sano con pocos registros rotos no abre el breaker', () => {
    // 5 registros rotos × 4 intentos = 20 fallas sobre 200 llamadas.
    expect(providerBreakerOpen({ ...EMPTY_STATS, calls: 200, fails: 20, failRecords: 5 })).toBe(false);
  });

  it('si falla la mayoría, con muchos registros distintos, se abre', () => {
    expect(providerBreakerOpen({ ...EMPTY_STATS, calls: 40, fails: 30, failRecords: 25 })).toBe(true);
    expect(providerBreakerOpen({ ...EMPTY_STATS, calls: 10, fails: 10, failRecords: 10 })).toBe(false);
  });

  it('un salto de permanentes en todo el sistema pausa el despacho', () => {
    expect(permanentBreakerOpen({ ...EMPTY_STATS, calls: 100, fails: 30, failRecords: 30, permanents: 30 })).toBe(true);
    expect(permanentBreakerOpen({ ...EMPTY_STATS, calls: 100, fails: 5, failRecords: 5, permanents: 5 })).toBe(false);
  });
});

describe('breaker de la API', () => {
  it('un solo registro (o un solo proveedor) no alcanza para pausar todo', () => {
    expect(apiBreakerOpen({ ...EMPTY_STATS, apiFailRecords: 1, apiFailProviders: 1 })).toBe(false);
    expect(apiBreakerOpen({ ...EMPTY_STATS, apiFailRecords: 50, apiFailProviders: 1 })).toBe(false);
    expect(apiBreakerOpen({ ...EMPTY_STATS, apiFailRecords: 5, apiFailProviders: 2 })).toBe(true);
  });
});

describe('lotes de SQS', () => {
  it('máximo 10 mensajes y 1 MiB por lote, sin cambiar el orden', () => {
    const small = Array.from({ length: 25 }, () => 'x');
    expect(chunkByBytes(small).map((c) => c.length)).toEqual([10, 10, 5]);
    const big = Array.from({ length: 4 }, () => 'x'.repeat(400_000));
    expect(chunkByBytes(big)).toEqual([[0, 1], [2, 3]]);
  });
});

describe('SLI de solapamiento', () => {
  it('cuenta llamadas que empiezan antes de que termine otra', () => {
    expect(countOverlaps([{ start: 0, end: 10 }, { start: 10, end: 20 }], 24_000)).toBe(0);
    expect(countOverlaps([{ start: 0, end: 10 }, { start: 5, end: 20 }], 24_000)).toBe(1);
  });

  it('una λ2 que murió sin registrar el fin cuenta hasta su vida máxima', () => {
    expect(countOverlaps([{ start: 0 }, { start: 20_000, end: 21_000 }], 24_000)).toBe(1);
    expect(countOverlaps([{ start: 0 }, { start: 25_000, end: 26_000 }], 24_000)).toBe(0);
  });
});
