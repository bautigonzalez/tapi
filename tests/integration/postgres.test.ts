import { Pool } from 'pg';
import { afterAll, describe, expect, it } from 'vitest';
import { LOCAL } from '../../local/env';
import { PostgresSource } from '../../src/adapters/postgres';
import { slotOf } from '../../src/domain/time';

const pool = new Pool({ connectionString: LOCAL.postgresUrl, max: 2 });
const source = new PostgresSource(pool);
afterAll(() => pool.end());

const SLOT_SQL = `(('x' || substr(md5(id::text), 1, 7))::bit(28)::int % 1440)`;

describe('Postgres real', () => {
  it('el slot calculado en SQL coincide con slotOf() para todos los registros', async () => {
    const res = await pool.query(`SELECT id, ${SLOT_SQL} AS slot FROM registros`);
    const mismatches = res.rows.filter((r) => slotOf(String(r.id)) !== Number(r.slot));
    expect(res.rows.length).toBeGreaterThan(5_000);
    expect(mismatches).toEqual([]);
  });

  it('la búsqueda del Dispatcher usa el índice (proveedor, slot, id)', async () => {
    await pool.query('SET enable_seqscan = off');
    const plan = await pool.query(
      `EXPLAIN SELECT id FROM registros WHERE proveedor = 'edenor' AND (${SLOT_SQL}, id) > (10, '') AND ${SLOT_SQL} <= 600 ORDER BY ${SLOT_SQL}, id LIMIT 50`,
    );
    const text = plan.rows.map((r) => String(r['QUERY PLAN'])).join('\n');
    expect(text).toContain('idx_registros_proveedor_slot');
  });

  it('fetchDue respeta el rango, el orden y el límite, y devuelve el body intacto', async () => {
    const rows = await source.fetchDue([{ proveedor: 'edenor', date: '2026-10-20', fromSlot: 100, fromId: '', toSlot: 300, limit: 20 }]);
    expect(rows.length).toBe(20);
    for (const r of rows) {
      expect(r.slot).toBeGreaterThanOrEqual(100);
      expect(r.slot).toBeLessThanOrEqual(300);
      expect(r.slot).toBe(slotOf(r.id));
    }
    const ordered = [...rows].sort((a, b) => a.slot - b.slot || (a.id < b.id ? -1 : 1));
    expect(rows.map((r) => r.id)).toEqual(ordered.map((r) => r.id));
    // El entero de 20 dígitos llega sin pasar por JSON.parse.
    expect(String(rows[0]?.body)).toContain('12345678901234567890');
  });

  it('countInvalid encuentra los registros que no se pueden despachar', async () => {
    await pool.query(`INSERT INTO registros VALUES ('reg-gigante', 'telecom', '/consultas/saldo', jsonb_build_object('x', repeat('a', 300000))) ON CONFLICT DO NOTHING`);
    const invalid = await source.countInvalid();
    expect(invalid.get('telecom')).toBe(1);
    const due = await source.fetchDue([{ proveedor: 'telecom', date: '2026-10-20', fromSlot: -1, fromId: '', toSlot: 1_439, limit: 5_000 }]);
    expect(due.some((r) => r.id === 'reg-gigante')).toBe(false);
    await pool.query(`DELETE FROM registros WHERE id = 'reg-gigante'`);
  });
});
