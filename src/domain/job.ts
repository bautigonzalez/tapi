import { ENDPOINT_MAX_CHARS, ID_MAX_CHARS, SLOTS_PER_DAY } from '../config';
import { execIdOf, utcDate } from './time';
import type { Job } from './types';

export interface DueRow {
  proveedor: string;
  date: string;
  slot: number;
  id: string;
  endpoint: string;
  body: unknown;
}

export function buildJob(row: DueRow): Job {
  return {
    schemaVersion: 1,
    execId: execIdOf(row.id, row.date),
    recordId: row.id,
    proveedor: row.proveedor,
    endpoint: row.endpoint,
    body: row.body,
    runDate: row.date,
    slot: row.slot,
  };
}

const DATE = /^\d{4}-\d{2}-\d{2}$/;
const HEX64 = /^[0-9a-f]{64}$/;
const MAX_MESSAGE_CHARS = 1_048_576;

/**
 * Valida un mensaje de la cola. Si no pasa, el worker no lo procesa: un mensaje inválido no debe
 * llegar a la API. Además verifica que el execId corresponda al registro y al día.
 */
export function parseJob(raw: string): Job | undefined {
  if (raw.length > MAX_MESSAGE_CHARS) return undefined;
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return undefined;
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) return undefined;
  const o = parsed as Record<string, unknown>;

  if (o.schemaVersion !== 1) return undefined;
  if (!isText(o.recordId, ID_MAX_CHARS) || !isText(o.proveedor, 128) || !isText(o.endpoint, ENDPOINT_MAX_CHARS)) return undefined;
  if (typeof o.runDate !== 'string' || !isValidDate(o.runDate)) return undefined;
  if (typeof o.slot !== 'number' || !Number.isInteger(o.slot) || o.slot < 0 || o.slot >= SLOTS_PER_DAY) return undefined;
  if (typeof o.execId !== 'string' || !HEX64.test(o.execId)) return undefined;
  if (o.execId !== execIdOf(o.recordId, o.runDate)) return undefined;
  if (!('body' in o)) return undefined;

  return {
    schemaVersion: 1,
    execId: o.execId,
    recordId: o.recordId,
    proveedor: o.proveedor,
    endpoint: o.endpoint,
    body: o.body,
    runDate: o.runDate,
    slot: o.slot,
  };
}

/** Mismos límites que valida el worker: lo que no pasa no se envía. */
export function isDispatchable(job: Job): boolean {
  return isText(job.recordId, ID_MAX_CHARS) && isText(job.proveedor, 128) && isText(job.endpoint, ENDPOINT_MAX_CHARS);
}

function isText(value: unknown, max: number): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= max;
}

/** YYYY-MM-DD que además es una fecha real (rechaza 2026-13-99). */
function isValidDate(value: string): boolean {
  if (!DATE.test(value)) return false;
  const ms = Date.parse(`${value}T00:00:00.000Z`);
  return Number.isFinite(ms) && utcDate(ms) === value;
}
