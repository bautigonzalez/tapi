-- Índice que asigna a cada registro un minuto fijo del día (slot 0..1439) a partir de un hash estable del id.
-- md5 es inmutable y estable entre versiones de Postgres; bit(28)::int nunca da negativo.
-- La expresión tiene que coincidir con slotOf() en src/domain/time.ts y con SLOT_SQL en src/adapters/postgres.ts.
--
-- Se crea en el writer con CONCURRENTLY (no bloquea escrituras) y lo usa el reader.
-- Ocupa ~50 MB por millón de filas y solo se actualiza en altas, bajas o cambios de proveedor.
-- Supuesto: registros(id text, proveedor text, endpoint text, body jsonb).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_registros_proveedor_slot ON registros
  (proveedor, (('x' || substr(md5(id::text), 1, 7))::bit(28)::int % 1440), id);

-- Rollback:
-- DROP INDEX CONCURRENTLY IF EXISTS idx_registros_proveedor_slot;
