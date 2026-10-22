import { readFileSync } from 'node:fs';
import { Signer } from '@aws-sdk/rds-signer';
import { Pool } from 'pg';
import { BODY_MAX_BYTES, ENDPOINT_MAX_CHARS, ID_MAX_CHARS } from '../config';
import type { Range } from '../domain/cursor';
import type { DueRow } from '../domain/job';
import { log } from '../observability';
import type { SourceDb } from '../ports';

/**
 * Expresión del slot. Tiene que ser idéntica a la del índice (sql/001_indice_slot.sql) para que
 * Postgres lo use. Todas las consultas son parametrizadas: los valores nunca se concatenan al SQL.
 */
const SLOT_SQL = `(('x' || substr(md5(r.id::text), 1, 7))::bit(28)::int % 1440)`;

/** Un registro que no cumple los límites no se despacha: no debe frenar al Dispatcher ni a su proveedor. */
const DISPATCHABLE_SQL = `octet_length(r.body::text) <= $7 AND length(r.id) <= $8 AND length(r.endpoint) <= $9`;

const FETCH_DUE_SQL = `
SELECT q.proveedor, q.fecha::text AS fecha, d.slot, d.id, d.endpoint, d.body
FROM unnest($1::text[], $2::date[], $3::int[], $4::text[], $5::int[], $6::int[])
     WITH ORDINALITY AS q(proveedor, fecha, desde_slot, desde_id, hasta_slot, limite, orden)
CROSS JOIN LATERAL (
  SELECT r.id, r.endpoint, r.body::text AS body, ${SLOT_SQL} AS slot
  FROM registros r
  WHERE r.proveedor = q.proveedor
    AND (${SLOT_SQL}, r.id) > (q.desde_slot, q.desde_id)
    AND ${SLOT_SQL} <= q.hasta_slot
    AND ${DISPATCHABLE_SQL}
  ORDER BY ${SLOT_SQL}, r.id
  LIMIT q.limite
) d
ORDER BY q.orden, d.slot, d.id`;

const COUNT_RANGE_SQL = `
SELECT count(*)::int AS n
FROM registros r
WHERE r.proveedor = $1
  AND (${SLOT_SQL}, r.id) > ($2::int, $3::text)
  AND ${SLOT_SQL} <= $4::int`;

const COUNT_BY_PROVIDER_SQL = `SELECT proveedor, count(*)::int AS n FROM registros GROUP BY proveedor`;

const COUNT_INVALID_SQL = `
SELECT r.proveedor, count(*)::int AS n
FROM registros r
WHERE octet_length(r.body::text) > $1 OR length(r.id) > $2 OR length(r.endpoint) > $3
GROUP BY r.proveedor`;

export interface PostgresConfig {
  host: string;
  port: number;
  database: string;
  user: string;
  region: string;
  /** Bundle de CA para validar el certificado de Aurora. Se usa solo en esta conexión. */
  caPath: string;
}

/** Pool de producción: autenticación IAM (sin contraseñas ni secretos) y TLS verificado. */
export function createIamPool(config: PostgresConfig): Pool {
  // El token IAM dura 15 min y se pide por conexión.
  const signer = new Signer({ hostname: config.host, port: config.port, username: config.user, region: config.region });
  return new Pool({
    host: config.host,
    port: config.port,
    database: config.database,
    user: config.user,
    password: () => signer.getAuthToken(),
    ssl: { rejectUnauthorized: true, ca: readFileSync(config.caPath, 'utf8') },
    max: 2,
    idleTimeoutMillis: 60_000,
    connectionTimeoutMillis: 5_000,
    statement_timeout: 20_000,
    query_timeout: 25_000,
    application_name: 'consultas-diarias',
  });
}

export class PostgresSource implements SourceDb {
  constructor(private readonly pool: Pool) {
    // Un error en un cliente inactivo (por ejemplo, tras un failover) no debe tirar el proceso.
    this.pool.on('error', (error) => log.error('postgres_cliente_inactivo', { error: error.message }));
  }

  async fetchDue(ranges: ReadonlyArray<Range & { limit: number }>): Promise<DueRow[]> {
    if (ranges.length === 0) return [];
    const res = await this.pool.query(FETCH_DUE_SQL, [
      ranges.map((r) => r.proveedor),
      ranges.map((r) => r.date),
      ranges.map((r) => r.fromSlot),
      ranges.map((r) => r.fromId),
      ranges.map((r) => r.toSlot),
      ranges.map((r) => r.limit),
      BODY_MAX_BYTES,
      ID_MAX_CHARS,
      ENDPOINT_MAX_CHARS,
    ]);
    return res.rows.map((row) => ({
      proveedor: String(row.proveedor),
      date: String(row.fecha),
      slot: Number(row.slot),
      id: String(row.id),
      endpoint: String(row.endpoint),
      // Se envía el texto tal cual: sin JSON.parse no se pierden enteros grandes ni cambia el formato.
      body: String(row.body),
    }));
  }

  async countRange(range: Range): Promise<number> {
    const res = await this.pool.query(COUNT_RANGE_SQL, [range.proveedor, range.fromSlot, range.fromId, range.toSlot]);
    return Number(res.rows[0]?.n ?? 0);
  }

  async countByProvider(): Promise<Map<string, number>> {
    const res = await this.pool.query(COUNT_BY_PROVIDER_SQL);
    return new Map(res.rows.map((row) => [String(row.proveedor), Number(row.n)]));
  }

  async countInvalid(): Promise<Map<string, number>> {
    const res = await this.pool.query(COUNT_INVALID_SQL, [BODY_MAX_BYTES, ID_MAX_CHARS, ENDPOINT_MAX_CHARS]);
    return new Map(res.rows.map((row) => [String(row.proveedor), Number(row.n)]));
  }
}
