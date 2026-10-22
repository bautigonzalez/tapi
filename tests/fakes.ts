import type { SQSEvent } from 'aws-lambda';
import { TIMEOUTS } from '../src/config';
import type { CallResult } from '../src/domain/classify';
import type { Range } from '../src/domain/cursor';
import { buildJob, type DueRow } from '../src/domain/job';
import { slotOf, turnAt } from '../src/domain/time';
import {
  EMPTY_STATS,
  type Cursor,
  type ExecutionStatus,
  type Job,
  type Outcome,
  type ProviderState,
  type WaitingReason,
  type WindowStats,
} from '../src/domain/types';
import type {
  ClaimResult,
  Clock,
  ExecutionsRepo,
  ExhaustedEntry,
  FinishDetails,
  GlobalView,
  InternalApi,
  JobQueue,
  ProviderView,
  ResultStore,
  RunRef,
  SourceDb,
} from '../src/ports';

/** Implementaciones en memoria de los puertos, con la misma semántica condicional que DynamoDB y SQS. */

export class FakeClock implements Clock {
  constructor(public t: number) {}
  now(): number {
    return this.t;
  }
  advance(ms: number): void {
    this.t += ms;
  }
}

export interface FakeRun {
  status: ExecutionStatus;
  attempt: number;
  proveedor: string;
  turno: number;
  leaseUntil?: number;
  claimToken?: string;
  waitingReason?: WaitingReason;
  details?: FinishDetails;
}

const TERMINAL = new Set<ExecutionStatus>(['SUCCEEDED', 'FAILED_PERMANENT', 'FAILED_EXHAUSTED', 'SUPERSEDED']);
const runKey = (r: { recordId: string; runDate: string }) => `${r.recordId}|${r.runDate}`;
const sameCursor = (a?: Cursor, b?: Cursor) =>
  (!a && !b) || (!!a && !!b && a.date === b.date && a.slot === b.slot && a.id === b.id);

export class FakeRepo implements ExecutionsRepo {
  runs = new Map<string, FakeRun>();
  providers = new Map<string, ProviderState>();
  providerStats = new Map<string, WindowStats>();
  global = { pausedUntil: 0, stats: { ...EMPTY_STATS } };
  skipped: Array<{ proveedor: string; date: string; count: number }> = [];
  holds: Array<{ proveedor: string; seconds: number }> = [];
  calls: Array<{ proveedor: string; outcome: Outcome }> = [];
  daily = new Map<string, { completed: number; busyMs: number; holdMs: number }>();
  private tokens = 0;

  addProvider(p: Partial<ProviderState> & { proveedor: string }): ProviderState {
    const state: ProviderState = {
      enabled: true,
      lanes: 1,
      occupancyS: 1,
      allowlist: ['/consultas'],
      pausedUntil: 0,
      backoffLevel: 0,
      ...p,
    };
    this.providers.set(p.proveedor, state);
    return state;
  }

  run(ref: { recordId: string; runDate: string }): FakeRun | undefined {
    return this.runs.get(runKey(ref));
  }

  setRun(ref: { recordId: string; runDate: string }, run: FakeRun): void {
    this.runs.set(runKey(ref), run);
  }

  async getProviderView(proveedor: string): Promise<ProviderView> {
    const state = this.providers.get(proveedor);
    return { state: state ? { ...state } : undefined, stats: { ...(this.providerStats.get(proveedor) ?? EMPTY_STATS) } };
  }

  async getGlobalView(): Promise<GlobalView> {
    return { pausedUntil: this.global.pausedUntil, stats: { ...this.global.stats } };
  }

  async claim(job: Job, nowMs: number): Promise<ClaimResult> {
    const lease = nowMs + TIMEOUTS.claimLeaseS * 1_000;
    const token = `tok-${++this.tokens}`;
    const run = this.run(job);
    if (!run) {
      this.setRun(job, { status: 'IN_PROGRESS', attempt: 0, proveedor: job.proveedor, turno: turnAt(job.runDate, job.slot), leaseUntil: lease, claimToken: token });
      return { status: 'CLAIMED', attempt: 0, token };
    }
    if (run.status === 'ENQUEUED' || run.status === 'WAITING') {
      run.status = 'IN_PROGRESS';
      run.leaseUntil = lease;
      run.claimToken = token;
      return { status: 'CLAIMED', attempt: run.attempt, token };
    }
    if (run.status === 'IN_PROGRESS' && (run.leaseUntil ?? 0) < nowMs) {
      run.attempt += 1;
      run.leaseUntil = lease;
      run.claimToken = token;
      return { status: 'CLAIMED', attempt: run.attempt, token };
    }
    if (TERMINAL.has(run.status)) return { status: 'TERMINAL' };
    return { status: 'BUSY', leaseLeftS: Math.max(1, Math.ceil(((run.leaseUntil ?? nowMs) - nowMs) / 1_000)) };
  }

  async finish(job: Job, token: string, status: 'SUCCEEDED' | 'FAILED_PERMANENT' | 'FAILED_EXHAUSTED', details: FinishDetails): Promise<void> {
    const run = this.inProgress(job, token);
    run.status = status;
    run.details = details;
    run.leaseUntil = undefined;
  }

  async markWaiting(job: Job, token: string, reason: WaitingReason, countsAsAttempt: boolean): Promise<void> {
    const run = this.inProgress(job, token);
    run.status = 'WAITING';
    run.waitingReason = reason;
    run.leaseUntil = undefined;
    if (countsAsAttempt) run.attempt += 1;
  }

  async supersede(ref: RunRef, nowMs: number): Promise<void> {
    const run = this.run(ref);
    if (!run) {
      this.setRun(ref, { status: 'SUPERSEDED', attempt: 0, proveedor: ref.proveedor ?? 'desconocido', turno: 0 });
      return;
    }
    if (run.status === 'ENQUEUED' || run.status === 'WAITING' || (run.status === 'IN_PROGRESS' && (run.leaseUntil ?? 0) < nowMs)) {
      run.status = 'SUPERSEDED';
    }
  }

  async recordCall(proveedor: string, _recordId: string, outcome: Outcome): Promise<void> {
    this.calls.push({ proveedor, outcome });
  }

  async recordHold(proveedor: string, seconds: number): Promise<void> {
    this.holds.push({ proveedor, seconds });
  }

  async pauseProvider(proveedor: string, untilMs: number, opts: { disable?: boolean; raiseBackoff?: boolean }): Promise<void> {
    const s = this.providers.get(proveedor);
    if (!s || s.pausedUntil >= untilMs) return;
    s.pausedUntil = untilMs;
    if (opts.disable) s.enabled = false;
    if (opts.raiseBackoff) s.backoffLevel += 1;
  }

  async resetProviderBackoff(proveedor: string): Promise<void> {
    const s = this.providers.get(proveedor);
    if (s) s.backoffLevel = 0;
  }

  async pauseGlobal(untilMs: number): Promise<void> {
    this.global.pausedUntil = Math.max(this.global.pausedUntil, untilMs);
  }

  async listProviders(): Promise<ProviderState[]> {
    return [...this.providers.values()].map((p) => ({ ...p }));
  }

  async countOpen(proveedor: string): Promise<number> {
    return [...this.runs.values()].filter((r) => r.proveedor === proveedor && !TERMINAL.has(r.status)).length;
  }

  async ensureEnqueued(job: Job): Promise<void> {
    if (!this.run(job)) {
      this.setRun(job, { status: 'ENQUEUED', attempt: 0, proveedor: job.proveedor, turno: turnAt(job.runDate, job.slot) });
    }
  }

  async saveCursor(proveedor: string, previous: Cursor | undefined, next: Cursor): Promise<boolean> {
    const s = this.providers.get(proveedor);
    if (!s || !sameCursor(s.cursor, previous)) return false;
    s.cursor = { ...next };
    return true;
  }

  async addSkipped(proveedor: string, date: string, count: number): Promise<void> {
    this.skipped.push({ proveedor, date, count });
  }

  async findStale(proveedor: string, turnBeforeMs: number): Promise<RunRef[]> {
    return [...this.runs.entries()]
      .filter(([, r]) => r.proveedor === proveedor && !TERMINAL.has(r.status) && r.turno < turnBeforeMs)
      .map(([key]) => {
        const [recordId = '', runDate = ''] = key.split('|');
        return { recordId, runDate, proveedor };
      });
  }

  async createProviderIfMissing(proveedor: string): Promise<boolean> {
    if (this.providers.has(proveedor)) return false;
    this.addProvider({ proveedor, enabled: false, allowlist: [] });
    return true;
  }

  async setOccupancy(proveedor: string, occupancyS: number): Promise<void> {
    const s = this.providers.get(proveedor);
    if (s) s.occupancyS = occupancyS;
  }

  async getDailyBusy(proveedor: string, date: string): Promise<{ completed: number; busyMs: number; holdMs: number }> {
    return this.daily.get(`${proveedor}|${date}`) ?? { completed: 0, busyMs: 0, holdMs: 0 };
  }

  private inProgress(job: Job, token: string): FakeRun {
    const run = this.run(job);
    if (!run || run.status !== 'IN_PROGRESS' || run.claimToken !== token) throw new Error(`La corrida ${job.execId} no está en curso con este claim`);
    run.claimToken = undefined;
    return run;
  }
}

export class FakeQueue implements JobQueue {
  sent: Job[] = [];
  holds: Array<{ receiptHandle: string; seconds: number }> = [];
  exhausted: ExhaustedEntry[] = [];
  /** execIds que SQS rechaza (para simular un SendMessageBatch parcial). */
  rejected = new Set<string>();

  async send(jobs: readonly Job[]): Promise<Set<string>> {
    const ok = new Set<string>();
    for (const job of jobs) {
      if (this.rejected.has(job.execId)) continue;
      this.sent.push(job);
      ok.add(job.execId);
    }
    return ok;
  }

  async hold(receiptHandle: string, seconds: number): Promise<void> {
    this.holds.push({ receiptHandle, seconds });
  }

  async sendExhausted(entry: ExhaustedEntry): Promise<void> {
    this.exhausted.push(entry);
  }
}

export class FakeApi implements InternalApi {
  calls: Array<{ job: Job; url: URL }> = [];
  constructor(private readonly respond: (job: Job, n: number) => CallResult | Promise<CallResult>) {}

  async call(job: Job, url: URL): Promise<CallResult> {
    this.calls.push({ job, url });
    return this.respond(job, this.calls.length);
  }
}

export class FakeStore implements ResultStore {
  puts: string[] = [];
  async put(job: Job): Promise<string> {
    const key = `resultados/${job.runDate}/${job.execId}.json`;
    this.puts.push(key);
    return key;
  }
}

export interface SourceRow {
  proveedor: string;
  id: string;
  endpoint: string;
  body: unknown;
}

/** Simula la query LATERAL de PostgresSource con la misma semántica de rangos. */
export class FakeSource implements SourceDb {
  constructor(public rows: SourceRow[]) {}

  async fetchDue(ranges: ReadonlyArray<Range & { limit: number }>): Promise<DueRow[]> {
    return ranges.flatMap((r) =>
      this.inRange(r)
        .slice(0, r.limit)
        .map((x) => ({ proveedor: x.proveedor, date: r.date, slot: x.slot, id: x.id, endpoint: x.endpoint, body: x.body })),
    );
  }

  async countRange(range: Range): Promise<number> {
    return this.inRange(range).length;
  }

  async countByProvider(): Promise<Map<string, number>> {
    const counts = new Map<string, number>();
    for (const row of this.rows) counts.set(row.proveedor, (counts.get(row.proveedor) ?? 0) + 1);
    return counts;
  }

  async countInvalid(): Promise<Map<string, number>> {
    return new Map();
  }

  private inRange(r: Range) {
    return this.rows
      .filter((x) => x.proveedor === r.proveedor)
      .map((x) => ({ ...x, slot: slotOf(x.id) }))
      .filter((x) => (x.slot > r.fromSlot || (x.slot === r.fromSlot && x.id > r.fromId)) && x.slot <= r.toSlot)
      .sort((a, b) => a.slot - b.slot || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  }
}

// ---------- Helpers ----------

export const RUN_DATE = '2026-10-20';
export const SLOT = 600;

export function makeJob(id = 'reg-1', proveedor = 'edenor', endpoint = '/consultas/saldo'): Job {
  return buildJob({ proveedor, date: RUN_DATE, slot: SLOT, id, endpoint, body: { cuenta: id } });
}

export function sqsEvent(bodies: Array<Job | string>, receiveCount = 1): SQSEvent {
  return {
    Records: bodies.map((b, i) => ({
      messageId: `m${i}`,
      receiptHandle: `rh${i}`,
      body: typeof b === 'string' ? b : JSON.stringify(b),
      attributes: { ApproximateReceiveCount: String(receiveCount) },
      messageAttributes: {},
      md5OfBody: '',
      eventSource: 'aws:sqs',
      eventSourceARN: 'arn:aws:sqs:us-east-1:000000000000:consultas-jobs-dev.fifo',
      awsRegion: 'us-east-1',
    })),
  } as unknown as SQSEvent;
}

export const ok = (body = '{"saldo":100}'): CallResult => ({ type: 'response', response: { status: 200, body } });

export const httpError = (status: number, body?: Record<string, unknown>, retryAfter?: string): CallResult => ({
  type: 'response',
  response: { status, body: body ? JSON.stringify(body) : '', retryAfter },
});

export const apiError = (status: number, code: string, retryable: boolean, scope: 'record' | 'provider', origin: 'api' | 'provider' = 'provider') =>
  httpError(status, { code, retryable, scope, origin });
