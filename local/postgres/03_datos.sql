-- Datos de prueba: 5.000 registros repartidos en 5 proveedores.
-- El body lleva un entero de 20 dígitos para verificar que llega intacto a la API (no pasa por JSON.parse).
INSERT INTO registros (id, proveedor, endpoint, body)
SELECT 'reg-' || g,
       p.proveedor,
       '/consultas/saldo',
       jsonb_build_object('proveedor', p.proveedor, 'cuenta', g, 'referencia', 12345678901234567890::numeric)
FROM generate_series(1, 5000) AS g
CROSS JOIN LATERAL (SELECT (ARRAY['edenor', 'aysa', 'metrogas', 'telecom', 'inestable'])[1 + g % 5] AS proveedor) p;

-- Registro envenenado: una ruta que no existe en la API. API Gateway responde 403 sin el cuerpo de error de la API.
-- No debe pausar el sistema: tiene que agotar sus intentos e ir a la cola de agotados.
INSERT INTO registros (id, proveedor, endpoint, body)
VALUES ('reg-veneno', 'edenor', '/consultas/no-existe', '{"proveedor": "edenor", "cuenta": 0}');

-- Registro que no pasa la allowlist (intento de SSRF): se rechaza sin llamar a la API.
INSERT INTO registros (id, proveedor, endpoint, body)
VALUES ('reg-ssrf', 'aysa', '/consultas/%252e%252e/admin', '{"proveedor": "aysa", "cuenta": -1}');

ANALYZE registros;
