import { randomUUID } from 'node:crypto';
import { ConditionalCheckFailedException, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { BatchGetCommand, DynamoDBDocumentClient, GetCommand, PutCommand, QueryCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { ADMISSION, BREAKER, RESULT_TTL_DAYS, TIMEOUTS } from '../config';
import { windowStart } from '../domain/breaker';
import { decodeCursor, encodeCursor } from '../domain/cursor';
import { turnAt, utcDate } from '../domain/time';
import { EMPTY_STATS, type Cursor, type Job, type Outcome, type ProviderState, type WaitingReason, type WindowStats } from '../domain/types';
import { log } from '../observability';
import type { ClaimResult, ExecutionsRepo, FinishDetails, GlobalView, ProviderView, RunRef } from '../ports';

/**
 * Tabla única `executions` (docs/ARQUITECTURA.md, Anexo D):
 *   PROV#<p> / STATE         configuración, cursor y pausa del proveedor
 *   PROV#<p> / STATS#<t>     contadores de la ventana de 10 min (breaker), TTL 2 h
 *   PROV#<p> / DAY#<fecha>   ocupación del día (Verificador), TTL 3 días
 *   PROV#<p> / SKIPPED#<f>   registros vencidos o inválidos que no corrieron (cobertura)
 *   GLOBAL   / STATE | STATS#<t>
 *   REC#<id> / RUN#<fecha>   ledger y resultado de la corrida
 * GSI `open`      (openProvider, turno): ejecuciones no terminales, para admisión y barrido.
 * GSI `providers` (entity, proveedor): lista de proveedores para el Dispatcher.
 */
const OPEN_INDEX = 'open';
const PROVIDERS_INDEX = 'providers';
const TERMINAL = ['SUCCEEDED', 'FAILED_PERMANENT', 'FAILED_EXHAUSTED', 'SUPERSEDED'];

const providerPk = (p: string) => `PROV#${p}`;
const runKey = (r: { recordId: string; runDate: string }) => ({ PK: `REC#${r.recordId}`, SK: `RUN#${r.runDate}` });
const ttlFrom = (nowMs: number, days: number) => Math.floor(nowMs / 1_000) + days * 86_400;
const statsKeys = (pk: string, nowMs: number) => {
  const current = windowStart(nowMs);
  return [`STATS#${current}`, `STATS#${current - BREAKER.windowMs}`].map((SK) => ({ PK: pk, SK }));
};

function isConditionalFailure(error: unknown): boolean {
  return error instanceof ConditionalCheckFailedException || (error as { name?: string })?.name === 'ConditionalCheckFailedException';
}

async function ignoreConditional(promise: Promise<unknown>): Promise<boolean> {
  try {
    await promise;
    return true;
  } catch (error) {
    if (isConditionalFailure(error)) return false;
    throw error;
  }
}

export class DynamoExecutionsRepo implements ExecutionsRepo {
  private readonly doc: DynamoDBDocumentClient;

  constructor(private readonly table: string, client = new DynamoDBClient({})) {
    this.doc = DynamoDBDocumentClient.from(client, { marshallOptions: { removeUndefinedValues: true } });
  }

  /** Lectura acotada: el estado y solo las dos ventanas que usa el breaker (no los ítems vencidos que el TTL aún no borró). */
  async getProviderView(proveedor: string, nowMs: number): Promise<ProviderView> {
    const pk = providerPk(proveedor);
    const items = await this.batchGet([{ PK: pk, SK: 'STATE' }, ...statsKeys(pk, nowMs)]);
    const state = items.find((i) => i.SK === 'STATE');
    return { state: state ? toProviderState(state) : undefined, stats: sumStats(items) };
  }

  async getGlobalView(nowMs: number): Promise<GlobalView> {
    const items = await this.batchGet([{ PK: 'GLOBAL', SK: 'STATE' }, ...statsKeys('GLOBAL', nowMs)]);
    const state = items.find((i) => i.SK === 'STATE');
    return { pausedUntil: Number(state?.pausedUntil ?? 0), stats: sumStats(items) };
  }

  async claim(job: Job, nowMs: number): Promise<ClaimResult> {
    const key = runKey(job);
    const token = randomUUID();
    const leaseUntil = nowMs + TIMEOUTS.claimLeaseS * 1_000;

    // 1. Desde ENQUEUED, WAITING o fila inexistente (el mensaje llegó antes que el ledger).
    try {
      const res = await this.doc.send(
        new UpdateCommand({
          TableName: this.table,
          Key: key,
          UpdateExpression:
            'SET #s = :inp, leaseUntil = :lease, claimToken = :tok, startedAt = :now, proveedor = :p, recordId = :rid, runDate = :rd, ' +
            'turno = if_not_exists(turno, :turno), openProvider = if_not_exists(openProvider, :p), ' +
            'attempt = if_not_exists(attempt, :zero), #ttl = if_not_exists(#ttl, :ttl)',
          ConditionExpression: 'attribute_not_exists(PK) OR #s IN (:enq, :wait)',
          ExpressionAttributeNames: { '#s': 'status', '#ttl': 'ttl' },
          ExpressionAttributeValues: {
            ':inp': 'IN_PROGRESS',
            ':lease': leaseUntil,
            ':tok': token,
            ':now': nowMs,
            ':p': job.proveedor,
            ':rid': job.recordId,
            ':rd': job.runDate,
            ':turno': turnAt(job.runDate, job.slot),
            ':zero': 0,
            ':ttl': ttlFrom(nowMs, RESULT_TTL_DAYS),
            ':enq': 'ENQUEUED',
            ':wait': 'WAITING',
          },
          ReturnValues: 'ALL_NEW',
        }),
      );
      return { status: 'CLAIMED', attempt: Number(res.Attributes?.attempt ?? 0), token };
    } catch (error) {
      if (!isConditionalFailure(error)) throw error;
    }

    // 2. IN_PROGRESS con el lease vencido: el worker anterior cayó. Cuenta como un intento.
    try {
      const res = await this.doc.send(
        new UpdateCommand({
          TableName: this.table,
          Key: key,
          UpdateExpression: 'SET leaseUntil = :lease, claimToken = :tok, startedAt = :now, attempt = attempt + :one',
          ConditionExpression: '#s = :inp AND leaseUntil < :now',
          ExpressionAttributeNames: { '#s': 'status' },
          ExpressionAttributeValues: { ':inp': 'IN_PROGRESS', ':lease': leaseUntil, ':tok': token, ':now': nowMs, ':one': 1 },
          ReturnValues: 'ALL_NEW',
        }),
      );
      return { status: 'CLAIMED', attempt: Number(res.Attributes?.attempt ?? 0), token };
    } catch (error) {
      if (!isConditionalFailure(error)) throw error;
    }

    // 3. Terminal (duplicado) u ocupado por otro worker con lease vigente.
    const current = await this.doc.send(new GetCommand({ TableName: this.table, Key: key, ConsistentRead: true }));
    const status = current.Item?.status as string | undefined;
    if (status && TERMINAL.includes(status)) return { status: 'TERMINAL' };
    const leaseLeftMs = Number(current.Item?.leaseUntil ?? 0) - nowMs;
    return { status: 'BUSY', leaseLeftS: Math.max(1, Math.ceil(leaseLeftMs / 1_000)) };
  }

  async finish(job: Job, token: string, status: 'SUCCEEDED' | 'FAILED_PERMANENT' | 'FAILED_EXHAUSTED', details: FinishDetails, nowMs: number): Promise<void> {
    const sets = ['#s = :s', 'endedAt = :now'];
    const values: Record<string, unknown> = { ':s': status, ':now': nowMs, ':inp': 'IN_PROGRESS', ':tok': token };
    const fields: Array<[keyof FinishDetails, string]> = [
      ['httpStatus', 'httpStatus'],
      ['code', 'errorCode'],
      ['result', 'resultBody'],
      ['resultKey', 'resultKey'],
    ];
    for (const [field, attribute] of fields) {
      if (details[field] === undefined) continue;
      sets.push(`${attribute} = :${attribute}`);
      values[`:${attribute}`] = details[field];
    }
    const ok = await ignoreConditional(
      this.doc.send(
        new UpdateCommand({
          TableName: this.table,
          Key: runKey(job),
          UpdateExpression: `SET ${sets.join(', ')} REMOVE openProvider, leaseUntil, claimToken, waitingReason`,
          // Fencing: solo cierra quien tiene el claim vigente.
          ConditionExpression: '#s = :inp AND claimToken = :tok',
          ExpressionAttributeNames: { '#s': 'status' },
          ExpressionAttributeValues: values,
        }),
      ),
    );
    if (!ok) throw new Error(`La corrida ${job.execId} ya no estaba en curso con este claim`);
  }

  async markWaiting(job: Job, token: string, reason: WaitingReason, countsAsAttempt: boolean, nowMs: number): Promise<void> {
    await this.doc.send(
      new UpdateCommand({
        TableName: this.table,
        Key: runKey(job),
        UpdateExpression:
          'SET #s = :wait, waitingReason = :reason, attempt = attempt + :inc, waitingSince = :now REMOVE leaseUntil, claimToken',
        ConditionExpression: '#s = :inp AND claimToken = :tok',
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: { ':wait': 'WAITING', ':reason': reason, ':inc': countsAsAttempt ? 1 : 0, ':now': nowMs, ':inp': 'IN_PROGRESS', ':tok': token },
      }),
    );
  }

  async supersede(run: RunRef, nowMs: number): Promise<void> {
    await ignoreConditional(
      this.doc.send(
        new UpdateCommand({
          TableName: this.table,
          Key: runKey(run),
          UpdateExpression:
            'SET #s = :sup, endedAt = :now, recordId = :rid, runDate = :rd, proveedor = if_not_exists(proveedor, :p), ' +
            '#ttl = if_not_exists(#ttl, :ttl) REMOVE openProvider, leaseUntil, claimToken',
          ConditionExpression: 'attribute_not_exists(PK) OR #s IN (:enq, :wait) OR (#s = :inp AND leaseUntil < :now)',
          ExpressionAttributeNames: { '#s': 'status', '#ttl': 'ttl' },
          ExpressionAttributeValues: {
            ':sup': 'SUPERSEDED',
            ':now': nowMs,
            ':rid': run.recordId,
            ':rd': run.runDate,
            ':p': run.proveedor ?? 'desconocido',
            ':ttl': ttlFrom(nowMs, RESULT_TTL_DAYS),
            ':enq': 'ENQUEUED',
            ':wait': 'WAITING',
            ':inp': 'IN_PROGRESS',
          },
        }),
      ),
    );
  }

  async recordCall(proveedor: string, recordId: string, outcome: Outcome, busyMs: number, nowMs: number): Promise<void> {
    const failed = outcome.kind !== 'success';
    const permanent = outcome.kind === 'permanent' ? 1 : 0;
    const apiFail = outcome.kind === 'retryable' && outcome.reason === 'api';
    const window = windowStart(nowMs);
    const statsTtl = ttlFrom(nowMs, 0) + 2 * 3_600;
    const providerStatsKey = { PK: providerPk(proveedor), SK: `STATS#${window}` };
    const globalStatsKey = { PK: 'GLOBAL', SK: `STATS#${window}` };

    const updates: Array<Promise<unknown>> = [
      this.doc.send(
        new UpdateCommand({
          TableName: this.table,
          Key: providerStatsKey,
          UpdateExpression: 'ADD calls :one, fails :fail, permanents :perm SET #ttl = :ttl',
          ExpressionAttributeNames: { '#ttl': 'ttl' },
          ExpressionAttributeValues: { ':one': 1, ':fail': failed ? 1 : 0, ':perm': permanent, ':ttl': statsTtl },
        }),
      ),
      this.doc.send(
        new UpdateCommand({
          TableName: this.table,
          Key: globalStatsKey,
          UpdateExpression: 'ADD calls :one, permanents :perm SET #ttl = :ttl',
          ExpressionAttributeNames: { '#ttl': 'ttl' },
          ExpressionAttributeValues: { ':one': 1, ':perm': permanent, ':ttl': statsTtl },
        }),
      ),
      this.doc.send(
        new UpdateCommand({
          TableName: this.table,
          Key: { PK: providerPk(proveedor), SK: `DAY#${utcDate(nowMs)}` },
          UpdateExpression: 'ADD completed :done, busyMs :busy SET #ttl = :ttl',
          ExpressionAttributeNames: { '#ttl': 'ttl' },
          ExpressionAttributeValues: { ':done': outcome.kind === 'retryable' ? 0 : 1, ':busy': Math.round(busyMs), ':ttl': ttlFrom(nowMs, 3) },
        }),
      ),
    ];
    if (failed) updates.push(this.addCapped(providerStatsKey, 'failRecords', recordId));
    if (apiFail) {
      updates.push(this.addCapped(globalStatsKey, 'apiFailRecords', recordId));
      updates.push(this.addCapped(globalStatsKey, 'apiFailProviders', proveedor));
    }
    await Promise.all(updates);
  }

  async recordHold(proveedor: string, seconds: number, nowMs: number): Promise<void> {
    await this.doc.send(
      new UpdateCommand({
        TableName: this.table,
        Key: { PK: providerPk(proveedor), SK: `DAY#${utcDate(nowMs)}` },
        UpdateExpression: 'ADD holdMs :ms SET #ttl = :ttl',
        ExpressionAttributeNames: { '#ttl': 'ttl' },
        ExpressionAttributeValues: { ':ms': Math.round(seconds * 1_000), ':ttl': ttlFrom(nowMs, 3) },
      }),
    );
  }

  async pauseProvider(proveedor: string, untilMs: number, opts: { disable?: boolean; raiseBackoff?: boolean }): Promise<void> {
    const sets = ['pausedUntil = :until'];
    const values: Record<string, unknown> = { ':until': untilMs };
    if (opts.disable) {
      sets.push('enabled = :false');
      values[':false'] = false;
    }
    if (opts.raiseBackoff) {
      sets.push('backoffLevel = if_not_exists(backoffLevel, :zero) + :one');
      values[':zero'] = 0;
      values[':one'] = 1;
    }
    await ignoreConditional(
      this.doc.send(
        new UpdateCommand({
          TableName: this.table,
          Key: { PK: providerPk(proveedor), SK: 'STATE' },
          UpdateExpression: `SET ${sets.join(', ')}`,
          // Solo extiende la pausa: dos workers no se pisan acortándola.
          ConditionExpression: 'attribute_exists(PK) AND (attribute_not_exists(pausedUntil) OR pausedUntil < :until)',
          ExpressionAttributeValues: values,
        }),
      ),
    );
  }

  async resetProviderBackoff(proveedor: string): Promise<void> {
    await ignoreConditional(
      this.doc.send(
        new UpdateCommand({
          TableName: this.table,
          Key: { PK: providerPk(proveedor), SK: 'STATE' },
          UpdateExpression: 'SET backoffLevel = :zero',
          ConditionExpression: 'backoffLevel > :zero',
          ExpressionAttributeValues: { ':zero': 0 },
        }),
      ),
    );
  }

  async pauseGlobal(untilMs: number): Promise<void> {
    await ignoreConditional(
      this.doc.send(
        new UpdateCommand({
          TableName: this.table,
          Key: { PK: 'GLOBAL', SK: 'STATE' },
          UpdateExpression: 'SET pausedUntil = :until',
          ConditionExpression: 'attribute_not_exists(pausedUntil) OR pausedUntil < :until',
          ExpressionAttributeValues: { ':until': untilMs },
        }),
      ),
    );
  }

  async listProviders(): Promise<ProviderState[]> {
    const items = await this.queryAll({
      IndexName: PROVIDERS_INDEX,
      KeyConditionExpression: 'entity = :e',
      ExpressionAttributeValues: { ':e': 'PROVIDER' },
    });
    return items.map(toProviderState);
  }

  async countOpen(proveedor: string): Promise<number> {
    let count = 0;
    let startKey: Record<string, unknown> | undefined;
    do {
      const res = await this.doc.send(
        new QueryCommand({
          TableName: this.table,
          IndexName: OPEN_INDEX,
          KeyConditionExpression: 'openProvider = :p',
          ExpressionAttributeValues: { ':p': proveedor },
          Select: 'COUNT',
          ExclusiveStartKey: startKey,
        }),
      );
      count += res.Count ?? 0;
      startKey = res.LastEvaluatedKey;
    } while (startKey);
    return count;
  }

  async ensureEnqueued(job: Job, nowMs: number): Promise<void> {
    await ignoreConditional(
      this.doc.send(
        new PutCommand({
          TableName: this.table,
          Item: {
            ...runKey(job),
            status: 'ENQUEUED',
            recordId: job.recordId,
            runDate: job.runDate,
            proveedor: job.proveedor,
            openProvider: job.proveedor,
            turno: turnAt(job.runDate, job.slot),
            attempt: 0,
            gen: 0,
            enqueuedAt: nowMs,
            ttl: ttlFrom(nowMs, RESULT_TTL_DAYS),
          },
          ConditionExpression: 'attribute_not_exists(PK)',
        }),
      ),
    );
  }

  async saveCursor(proveedor: string, previous: Cursor | undefined, next: Cursor): Promise<boolean> {
    return ignoreConditional(
      this.doc.send(
        new UpdateCommand({
          TableName: this.table,
          Key: { PK: providerPk(proveedor), SK: 'STATE' },
          UpdateExpression: 'SET #c = :next',
          ConditionExpression: previous ? '#c = :prev' : 'attribute_exists(PK) AND attribute_not_exists(#c)',
          ExpressionAttributeNames: { '#c': 'cursor' },
          ExpressionAttributeValues: previous
            ? { ':next': encodeCursor(next), ':prev': encodeCursor(previous) }
            : { ':next': encodeCursor(next) },
        }),
      ),
    );
  }

  async addSkipped(proveedor: string, date: string, count: number): Promise<void> {
    await this.doc.send(
      new UpdateCommand({
        TableName: this.table,
        Key: { PK: providerPk(proveedor), SK: `SKIPPED#${date}` },
        UpdateExpression: 'ADD #n :n SET #ttl = :ttl',
        ExpressionAttributeNames: { '#n': 'count', '#ttl': 'ttl' },
        ExpressionAttributeValues: { ':n': count, ':ttl': ttlFrom(Date.now(), RESULT_TTL_DAYS) },
      }),
    );
  }

  async findStale(proveedor: string, turnBeforeMs: number): Promise<RunRef[]> {
    const items = await this.queryAll({
      IndexName: OPEN_INDEX,
      KeyConditionExpression: 'openProvider = :p AND turno < :t',
      ExpressionAttributeValues: { ':p': proveedor, ':t': turnBeforeMs },
    });
    return items.map((i) => ({
      recordId: String(i.PK).slice('REC#'.length),
      runDate: String(i.SK).slice('RUN#'.length),
      proveedor,
    }));
  }

  async createProviderIfMissing(proveedor: string): Promise<boolean> {
    return ignoreConditional(
      this.doc.send(
        new PutCommand({
          TableName: this.table,
          Item: {
            PK: providerPk(proveedor),
            SK: 'STATE',
            entity: 'PROVIDER',
            proveedor,
            // Deny by default: un proveedor nuevo no se llama hasta que alguien configure su allowlist.
            enabled: false,
            lanes: 1,
            occupancyS: ADMISSION.defaultOccupancyS,
            allowlist: [],
            pausedUntil: 0,
            backoffLevel: 0,
          },
          ConditionExpression: 'attribute_not_exists(PK)',
        }),
      ),
    );
  }

  async setOccupancy(proveedor: string, occupancyS: number): Promise<void> {
    await this.doc.send(
      new UpdateCommand({
        TableName: this.table,
        Key: { PK: providerPk(proveedor), SK: 'STATE' },
        UpdateExpression: 'SET occupancyS = :o',
        ConditionExpression: 'attribute_exists(PK)',
        ExpressionAttributeValues: { ':o': occupancyS },
      }),
    );
  }

  async getDailyBusy(proveedor: string, date: string): Promise<{ completed: number; busyMs: number; holdMs: number }> {
    const res = await this.doc.send(new GetCommand({ TableName: this.table, Key: { PK: providerPk(proveedor), SK: `DAY#${date}` } }));
    return {
      completed: Number(res.Item?.completed ?? 0),
      busyMs: Number(res.Item?.busyMs ?? 0),
      holdMs: Number(res.Item?.holdMs ?? 0),
    };
  }

  /** Agrega un id a un conjunto con tope: alcanza para el breaker y el ítem no crece sin límite. */
  private async addCapped(key: { PK: string; SK: string }, attribute: string, value: string): Promise<void> {
    await ignoreConditional(
      this.doc.send(
        new UpdateCommand({
          TableName: this.table,
          Key: key,
          UpdateExpression: 'ADD #a :v',
          ConditionExpression: 'attribute_not_exists(#a) OR size(#a) < :max',
          ExpressionAttributeNames: { '#a': attribute },
          ExpressionAttributeValues: { ':v': new Set([value]), ':max': BREAKER.maxTrackedRecords },
        }),
      ),
    );
  }

  private async batchGet(keys: Array<{ PK: string; SK: string }>): Promise<Record<string, unknown>[]> {
    const res = await this.doc.send(new BatchGetCommand({ RequestItems: { [this.table]: { Keys: keys } } }));
    const items = (res.Responses?.[this.table] ?? []) as Record<string, unknown>[];
    const unprocessed = res.UnprocessedKeys?.[this.table]?.Keys;
    if (unprocessed && unprocessed.length > 0) {
      log.warn('batch_get_incompleto', { pendientes: unprocessed.length });
      const retry = await this.doc.send(new BatchGetCommand({ RequestItems: { [this.table]: { Keys: unprocessed } } }));
      items.push(...((retry.Responses?.[this.table] ?? []) as Record<string, unknown>[]));
    }
    return items;
  }

  private async queryAll(input: Omit<ConstructorParameters<typeof QueryCommand>[0], 'TableName'>): Promise<Record<string, unknown>[]> {
    const items: Record<string, unknown>[] = [];
    let startKey: Record<string, unknown> | undefined;
    do {
      const res = await this.doc.send(new QueryCommand({ ...input, TableName: this.table, ExclusiveStartKey: startKey }));
      items.push(...((res.Items ?? []) as Record<string, unknown>[]));
      startKey = res.LastEvaluatedKey;
    } while (startKey);
    return items;
  }
}

function toProviderState(item: Record<string, unknown>): ProviderState {
  return {
    proveedor: String(item.proveedor),
    enabled: item.enabled === true,
    lanes: Number(item.lanes ?? 1),
    occupancyS: Number(item.occupancyS ?? ADMISSION.defaultOccupancyS),
    allowlist: Array.isArray(item.allowlist) ? item.allowlist.filter((p): p is string => typeof p === 'string') : [],
    pausedUntil: Number(item.pausedUntil ?? 0),
    backoffLevel: Number(item.backoffLevel ?? 0),
    cursor: decodeCursor(item.cursor),
  };
}

/** Suma las ventanas leídas (actual y anterior): el breaker mira los últimos 10 a 20 minutos. */
function sumStats(items: Record<string, unknown>[]): WindowStats {
  const stats: WindowStats = { ...EMPTY_STATS };
  const sets = { failRecords: new Set<string>(), apiFailRecords: new Set<string>(), apiFailProviders: new Set<string>() };
  for (const item of items) {
    if (!String(item.SK).startsWith('STATS#')) continue;
    stats.calls += Number(item.calls ?? 0);
    stats.fails += Number(item.fails ?? 0);
    stats.permanents += Number(item.permanents ?? 0);
    for (const name of Object.keys(sets) as Array<keyof typeof sets>) {
      const value = item[name];
      if (value instanceof Set) for (const v of value) sets[name].add(String(v));
    }
  }
  stats.failRecords = sets.failRecords.size;
  stats.apiFailRecords = sets.apiFailRecords.size;
  stats.apiFailProviders = sets.apiFailProviders.size;
  return stats;
}
