import type { CallResult } from './domain/classify';
import type { Range } from './domain/cursor';
import type { DueRow } from './domain/job';
import type { CallInterval } from './domain/overlaps';
import type { Cursor, Job, Outcome, ProviderState, WaitingReason, WindowStats } from './domain/types';

/**
 * Puertos: los handlers dependen de estas interfaces y no de AWS. Las implementaciones reales están en
 * src/adapters y las de prueba, en tests/fakes.ts.
 */

export type ClaimResult =
  /** `token` es el fencing del claim: solo quien lo tiene puede cerrar o pasar a WAITING la corrida. */
  | { status: 'CLAIMED'; attempt: number; token: string }
  | { status: 'TERMINAL' }
  | { status: 'BUSY'; leaseLeftS: number };

export interface ProviderView {
  state?: ProviderState;
  stats: WindowStats;
}

export interface GlobalView {
  pausedUntil: number;
  stats: WindowStats;
}

export interface FinishDetails {
  httpStatus?: number;
  code?: string;
  result?: string;
  resultKey?: string;
}

export interface RunRef {
  recordId: string;
  runDate: string;
  proveedor?: string;
}

export interface ExecutionsRepo {
  getProviderView(proveedor: string, nowMs: number): Promise<ProviderView>;
  getGlobalView(nowMs: number): Promise<GlobalView>;
  /** Claim condicional; crea la fila si el mensaje llegó antes que el ledger. */
  claim(job: Job, nowMs: number): Promise<ClaimResult>;
  finish(job: Job, token: string, status: 'SUCCEEDED' | 'FAILED_PERMANENT' | 'FAILED_EXHAUSTED', details: FinishDetails, nowMs: number): Promise<void>;
  markWaiting(job: Job, token: string, reason: WaitingReason, countsAsAttempt: boolean, nowMs: number): Promise<void>;
  supersede(run: RunRef, nowMs: number): Promise<void>;
  recordCall(proveedor: string, recordId: string, outcome: Outcome, busyMs: number, nowMs: number): Promise<void>;
  recordHold(proveedor: string, seconds: number, nowMs: number): Promise<void>;
  pauseProvider(proveedor: string, untilMs: number, opts: { disable?: boolean; raiseBackoff?: boolean }): Promise<void>;
  resetProviderBackoff(proveedor: string): Promise<void>;
  pauseGlobal(untilMs: number): Promise<void>;

  listProviders(): Promise<ProviderState[]>;
  countOpen(proveedor: string): Promise<number>;
  /** Escribe la ejecución ENQUEUED solo si no existe: nunca pisa un estado posterior. */
  ensureEnqueued(job: Job, nowMs: number): Promise<void>;
  saveCursor(proveedor: string, previous: Cursor | undefined, next: Cursor): Promise<boolean>;
  addSkipped(proveedor: string, date: string, count: number): Promise<void>;
  findStale(proveedor: string, turnBeforeMs: number): Promise<RunRef[]>;

  /** Crea un proveedor deshabilitado si no existe (deny by default). Devuelve true si lo creó. */
  createProviderIfMissing(proveedor: string): Promise<boolean>;
  setOccupancy(proveedor: string, occupancyS: number): Promise<void>;
  getDailyBusy(proveedor: string, date: string): Promise<{ completed: number; busyMs: number; holdMs: number }>;
}

export interface SourceDb {
  /** Registros pendientes de cada rango, en orden, hasta `limit` por rango. */
  fetchDue(ranges: ReadonlyArray<Range & { limit: number }>): Promise<DueRow[]>;
  countRange(range: Range): Promise<number>;
  countByProvider(): Promise<Map<string, number>>;
  /** Registros que no se pueden despachar (body, id o endpoint fuera de los límites). */
  countInvalid(): Promise<Map<string, number>>;
}

export interface ExhaustedEntry {
  code: string;
  execId?: string;
  recordId?: string;
  proveedor?: string;
  runDate?: string;
  messageId?: string;
}

export interface JobQueue {
  /** Envía en orden y devuelve los execId que SQS aceptó. */
  send(jobs: readonly Job[]): Promise<Set<string>>;
  hold(receiptHandle: string, seconds: number): Promise<void>;
  sendExhausted(entry: ExhaustedEntry): Promise<void>;
}

export interface InternalApi {
  call(job: Job, url: URL, timeoutMs: number): Promise<CallResult>;
}

export interface ResultStore {
  put(job: Job, body: string): Promise<string>;
}

export interface CallLog {
  listCalls(proveedor: string, fromMs: number, toMs: number): Promise<CallInterval[]>;
}

export interface Clock {
  now(): number;
}

export const systemClock: Clock = { now: () => Date.now() };
