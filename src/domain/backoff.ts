import { RETRY } from '../config';

/**
 * Backoff exponencial con equal jitter: un valor entre la mitad y el total de min(cap, base·2^n).
 * El piso de la mitad evita reintentos casi inmediatos.
 */
export function backoff(attempt: number, baseS: number, capS: number, random: () => number = Math.random): number {
  const exponent = Math.min(Math.max(attempt, 0), RETRY.maxBackoffLevel);
  const d = Math.min(capS, baseS * 2 ** exponent);
  return Math.ceil(d / 2 + random() * (d / 2));
}

/** Espera por error de un registro: 15, 30 y 60 s (con jitter). */
export function recordBackoff(attempt: number, random: () => number = Math.random): number {
  return backoff(attempt, RETRY.recordBaseS, RETRY.recordCapS, random);
}

/**
 * Retención por error del proveedor (docs/ARQUITECTURA.md §5). Nunca menos que Retry-After.
 * Desde la recepción 100 del mismo mensaje, mínimo 120 s: así en 24 h hay como máximo
 * 100 + 86.400 / 120 = 820 recepciones, por debajo de maxReceiveCount = 1.000.
 */
export function providerHold(
  level: number,
  retryAfterS: number | undefined,
  receiveCount: number,
  random: () => number = Math.random,
): number {
  let seconds = Math.max(retryAfterS ?? 0, backoff(level, RETRY.providerBaseS, RETRY.providerCapS, random));
  if (receiveCount >= RETRY.receiveFloorAfter) seconds = Math.max(seconds, RETRY.receiveFloorS);
  return Math.min(seconds, RETRY.maxHoldS);
}
