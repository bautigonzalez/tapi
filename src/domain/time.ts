import { createHash } from 'node:crypto';
import { SLOTS_PER_DAY } from '../config';

const DAY_MS = 86_400_000;

/**
 * Minuto del día de un registro. Tiene que coincidir con la expresión del índice
 * (sql/001_indice_slot.sql): los primeros 28 bits del md5 del id, módulo 1440.
 */
export function slotOf(recordId: string): number {
  const hex = createHash('md5').update(recordId, 'utf8').digest('hex');
  return parseInt(hex.slice(0, 7), 16) % SLOTS_PER_DAY;
}

/** Identificador de la corrida de un registro en un día: es la DedupId de SQS y la Idempotency-Key. */
export function execIdOf(recordId: string, runDate: string): string {
  return createHash('sha256').update(`${recordId}\u0000${runDate}`, 'utf8').digest('hex');
}

export function utcDate(ms: number): string {
  return new Date(ms).toISOString().slice(0, 10);
}

export function minuteOfDay(ms: number): number {
  const d = new Date(ms);
  return d.getUTCHours() * 60 + d.getUTCMinutes();
}

export function previousDate(date: string): string {
  return utcDate(Date.parse(`${date}T00:00:00.000Z`) - DAY_MS);
}

/** Momento en que le toca correr a un registro: el inicio de su minuto en su día. */
export function turnAt(runDate: string, slot: number): number {
  return Date.parse(`${runDate}T00:00:00.000Z`) + slot * 60_000;
}

export function secondsUntil(untilMs: number, nowMs: number, maxS: number): number {
  return Math.min(maxS, Math.max(1, Math.ceil((untilMs - nowMs) / 1_000)));
}
