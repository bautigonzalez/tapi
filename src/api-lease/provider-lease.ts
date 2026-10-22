import { randomUUID } from 'node:crypto';
import { ConditionalCheckFailedException, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, PutCommand, QueryCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import type { CallInterval } from '../domain/overlaps';
import type { CallLog } from '../ports';

/**
 * Lease por proveedor para λ2 (docs/ARQUITECTURA.md §4, capa 2). Es la pieza que se le entrega al
 * equipo de la API: λ2 envuelve la llamada al proveedor con `withProviderLease`.
 *
 * - Antes de llamar, toma el lease con una escritura condicional (TTL = vida máxima de λ2 + margen).
 * - Registra la llamada antes de hacerla, para el SLI de solapamiento.
 * - Al terminar, libera el lease. Si la llamada al proveedor terminó por timeout, NO lo libera: lo
 *   deja vencer para darle margen al proveedor.
 * - Si el lease está ocupado, devuelve `retryAfterS` y λ2 responde 423.
 * - Si DynamoDB falla, la excepción se propaga y λ2 no llama: falla cerrada.
 *
 * La garantía no depende de fencing tokens (el proveedor no los valida), sino de que el TTL del lease
 * supere la vida máxima del titular, que impone Lambda.
 */

export interface LeaseStore {
  acquire(proveedor: string, holder: string, ttlMs: number, nowMs: number): Promise<{ ok: true } | { ok: false; retryAfterS: number }>;
  release(proveedor: string, holder: string): Promise<void>;
  logStart(proveedor: string, holder: string, startMs: number): Promise<void>;
  logEnd(proveedor: string, holder: string, startMs: number, endMs: number, outcome: string): Promise<void>;
}

export interface LeasedCall<T> {
  value: T;
  /** true si la llamada al proveedor terminó por timeout: el lease se deja vencer. */
  providerTimedOut: boolean;
  outcome: string;
}

export type LeaseResult<T> = { acquired: true; value: T } | { acquired: false; retryAfterS: number };

export async function withProviderLease<T>(
  store: LeaseStore,
  proveedor: string,
  options: {
    ttlMs: number;
    now: () => number;
    /** `context.getRemainingTimeInMillis()` de λ2: el lease nunca dura menos que lo que le queda de vida. */
    remainingMs?: number;
    onLogError?: (error: unknown) => void;
  },
  call: () => Promise<LeasedCall<T>>,
): Promise<LeaseResult<T>> {
  const holder = randomUUID();
  const start = options.now();
  const ttlMs = Math.max(options.ttlMs, (options.remainingMs ?? 0) + 5_000);
  const lease = await store.acquire(proveedor, holder, ttlMs, start);
  if (!lease.ok) return { acquired: false, retryAfterS: lease.retryAfterS };

  // Sin el registro de inicio no hay SLI: si falla, no se llama (falla cerrada).
  await store.logStart(proveedor, holder, start);
  let timedOut = true;
  try {
    const result = await call();
    timedOut = result.providerTimedOut;
    try {
      await store.logEnd(proveedor, holder, start, options.now(), result.outcome);
    } catch (error) {
      // El registro del fin es telemetría: si falla, el SLI imputa inicio + vida máxima. No se pierde el resultado.
      options.onLogError?.(error);
    }
    return { acquired: true, value: result.value };
  } finally {
    if (!timedOut) await store.release(proveedor, holder);
  }
}

function isConditionalFailure(error: unknown): boolean {
  return error instanceof ConditionalCheckFailedException || (error as { name?: string })?.name === 'ConditionalCheckFailedException';
}

/** Implementación sobre la tabla `provider_leases` (PK = PROV#<p>; SK = LEASE | CALL#<inicio>#<holder>). */
export class DynamoLeaseStore implements LeaseStore {
  private readonly doc: DynamoDBDocumentClient;

  constructor(private readonly table: string, client = new DynamoDBClient({})) {
    this.doc = DynamoDBDocumentClient.from(client);
  }

  async acquire(proveedor: string, holder: string, ttlMs: number, nowMs: number): Promise<{ ok: true } | { ok: false; retryAfterS: number }> {
    try {
      await this.doc.send(
        new UpdateCommand({
          TableName: this.table,
          Key: { PK: `PROV#${proveedor}`, SK: 'LEASE' },
          UpdateExpression: 'SET holder = :h, leaseUntil = :until',
          ConditionExpression: 'attribute_not_exists(leaseUntil) OR leaseUntil < :now',
          ExpressionAttributeValues: { ':h': holder, ':until': nowMs + ttlMs, ':now': nowMs },
        }),
      );
      return { ok: true };
    } catch (error) {
      if (!isConditionalFailure(error)) throw error;
    }
    const current = await this.doc.send(new GetCommand({ TableName: this.table, Key: { PK: `PROV#${proveedor}`, SK: 'LEASE' }, ConsistentRead: true }));
    const leftMs = Number(current.Item?.leaseUntil ?? 0) - nowMs;
    return { ok: false, retryAfterS: Math.max(1, Math.ceil(leftMs / 1_000)) };
  }

  async release(proveedor: string, holder: string): Promise<void> {
    try {
      await this.doc.send(
        new UpdateCommand({
          TableName: this.table,
          Key: { PK: `PROV#${proveedor}`, SK: 'LEASE' },
          UpdateExpression: 'REMOVE leaseUntil, holder',
          ConditionExpression: 'holder = :h',
          ExpressionAttributeValues: { ':h': holder },
        }),
      );
    } catch (error) {
      // Si otro titular lo tomó (el nuestro venció), no se toca.
      if (!isConditionalFailure(error)) throw error;
    }
  }

  async logStart(proveedor: string, holder: string, startMs: number): Promise<void> {
    await this.doc.send(
      new PutCommand({
        TableName: this.table,
        Item: { PK: `PROV#${proveedor}`, SK: callSk(startMs, holder), start: startMs, ttl: Math.floor(startMs / 1_000) + 7 * 86_400 },
      }),
    );
  }

  async logEnd(proveedor: string, holder: string, startMs: number, endMs: number, outcome: string): Promise<void> {
    await this.doc.send(
      new UpdateCommand({
        TableName: this.table,
        Key: { PK: `PROV#${proveedor}`, SK: callSk(startMs, holder) },
        UpdateExpression: 'SET #e = :end, outcome = :o',
        ExpressionAttributeNames: { '#e': 'end' },
        ExpressionAttributeValues: { ':end': endMs, ':o': outcome },
      }),
    );
  }
}

/** Lectura del registro de llamadas para el SLI de solapamiento (lo usa el Verificador). */
export class DynamoCallLog implements CallLog {
  private readonly doc: DynamoDBDocumentClient;

  constructor(private readonly table: string, client = new DynamoDBClient({})) {
    this.doc = DynamoDBDocumentClient.from(client);
  }

  async listCalls(proveedor: string, fromMs: number, toMs: number): Promise<CallInterval[]> {
    const calls: CallInterval[] = [];
    let startKey: Record<string, unknown> | undefined;
    do {
      const res = await this.doc.send(
        new QueryCommand({
          TableName: this.table,
          KeyConditionExpression: 'PK = :pk AND SK BETWEEN :from AND :to',
          ExpressionAttributeValues: { ':pk': `PROV#${proveedor}`, ':from': callSk(fromMs, ''), ':to': callSk(toMs, '~') },
          ExclusiveStartKey: startKey,
        }),
      );
      for (const item of res.Items ?? []) {
        calls.push({ start: Number(item.start), end: item.end === undefined ? undefined : Number(item.end) });
      }
      startKey = res.LastEvaluatedKey;
    } while (startKey);
    return calls;
  }
}

function callSk(startMs: number, holder: string): string {
  return `CALL#${String(startMs).padStart(13, '0')}#${holder}`;
}
