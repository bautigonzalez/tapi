import { SLOTS_PER_DAY } from '../config';
import { minuteOfDay, previousDate, utcDate } from './time';
import type { Cursor } from './types';

/** Rango de registros de un proveedor: `(slot, id) > (fromSlot, fromId)` y `slot ≤ toSlot`. */
export interface Range {
  proveedor: string;
  date: string;
  fromSlot: number;
  fromId: string;
  toSlot: number;
}

export interface PendingWork {
  ranges: Range[];
  /** Registros de ayer que vencieron sin correr: se cuentan para el SLI de cobertura. */
  skipped?: Range;
  /** Posición a guardar antes de despachar (proveedor nuevo o salto de lo vencido). */
  startCursor?: Cursor;
}

/**
 * Como todo id es mayor que '', la posición (slot, '') queda justo antes del primer registro del slot.
 */
function startOf(date: string, slot: number): Cursor {
  return { date, slot, id: '' };
}

/**
 * Qué le falta despachar a un proveedor (docs/ARQUITECTURA.md §3.1, cambio de día).
 * - De hoy, todo hasta el minuto actual.
 * - De ayer, solo los slots posteriores al minuto actual: los anteriores ya vencieron porque hoy
 *   les toca de nuevo. Esos se saltean y se cuentan.
 * - Un proveedor sin cursor arranca en el minuto siguiente: lo que ya pasó hoy corre mañana.
 */
export function pendingWork(proveedor: string, cursor: Cursor | undefined, nowMs: number): PendingWork {
  const today = utcDate(nowMs);
  const nowSlot = minuteOfDay(nowMs);
  const yesterday = previousDate(today);
  const range = (from: Cursor, toSlot: number): Range => ({
    proveedor,
    date: from.date,
    fromSlot: from.slot,
    fromId: from.id,
    toSlot,
  });

  if (!cursor) {
    return { ranges: [], startCursor: startOf(today, nowSlot + 1) };
  }
  if (cursor.date === today) {
    return { ranges: [range(cursor, nowSlot)] };
  }

  const ranges: Range[] = [];
  let skipped: Range | undefined;
  let startCursor: Cursor | undefined;
  const firstValid = startOf(yesterday, nowSlot + 1);

  if (cursor.date === yesterday && cursor.slot > nowSlot) {
    ranges.push(range(cursor, SLOTS_PER_DAY - 1));
  } else {
    // El cursor quedó en algo que ya venció: se saltea hasta el primer slot que todavía vale.
    const from = cursor.date === yesterday ? cursor : startOf(yesterday, 0);
    skipped = range(from, nowSlot);
    startCursor = firstValid;
    if (firstValid.slot < SLOTS_PER_DAY) ranges.push(range(firstValid, SLOTS_PER_DAY - 1));
  }
  ranges.push(range(startOf(today, 0), nowSlot));
  return { ranges, skipped, startCursor };
}

/** Posición después de despachar todo lo que vencía hasta ahora. */
export function exhaustedCursor(nowMs: number): Cursor {
  return startOf(utcDate(nowMs), minuteOfDay(nowMs) + 1);
}

export function encodeCursor(c: Cursor): string {
  return `${c.date}|${String(c.slot).padStart(4, '0')}|${c.id}`;
}

export function decodeCursor(value: unknown): Cursor | undefined {
  if (typeof value !== 'string') return undefined;
  const first = value.indexOf('|');
  const second = value.indexOf('|', first + 1);
  if (first < 0 || second < 0) return undefined;
  const slot = Number(value.slice(first + 1, second));
  if (!Number.isInteger(slot)) return undefined;
  return { date: value.slice(0, first), slot, id: value.slice(second + 1) };
}
