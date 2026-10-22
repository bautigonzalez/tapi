/**
 * API interna simulada (API Gateway → λ1 → λ2 → proveedor), por HTTPS en 127.0.0.1.
 *
 * - Detector de concurrencia: cuenta las llamadas en vuelo por proveedor. Si alguna vez ve 2, la
 *   exclusión falló.
 * - Proveedores con distinta latencia y uno inestable (caídas y errores de registro).
 * - Una ruta que no existe responde como API Gateway: 403 sin el cuerpo de error de la API.
 * - Idempotency-Key: un reintento de algo que ya salió bien devuelve el resultado guardado.
 */
import { readFileSync } from 'node:fs';
import { createServer, type Server } from 'node:https';
import * as path from 'node:path';

interface Behavior {
  latencyMs: number;
  providerDownRate?: number;
  recordErrorRate?: number;
}

const BEHAVIOR: Record<string, Behavior> = {
  edenor: { latencyMs: 15 },
  aysa: { latencyMs: 40 },
  metrogas: { latencyMs: 5 },
  telecom: { latencyMs: 25 },
  inestable: { latencyMs: 30, providerDownRate: 0.15, recordErrorRate: 0.05 },
};

export interface ProviderStats {
  calls: number;
  inFlight: number;
  maxInFlight: number;
  overlaps: number;
  status: Record<string, number>;
}

export interface MockStats {
  calls: number;
  signed: number;
  bigIntIntact: number;
  idempotentReplays: number;
  /** Qué registros se reintentaron después de haber salido bien (para diagnóstico). */
  replays: string[];
  poisonCalls: number;
  byProvider: Record<string, ProviderStats>;
}

const SUCCESS_PATH = '/prod/consultas/saldo';
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export async function startMockApi(port: number): Promise<{ stats: MockStats; close: () => Promise<void> }> {
  const certDir = path.join(__dirname, '.certs');
  const stats: MockStats = { calls: 0, signed: 0, bigIntIntact: 0, idempotentReplays: 0, replays: [], poisonCalls: 0, byProvider: {} };
  const idempotency = new Map<string, string>();

  const server: Server = createServer(
    { key: readFileSync(path.join(certDir, 'mock-api.key')), cert: readFileSync(path.join(certDir, 'mock-api.pem')) },
    (req, res) => {
      const chunks: Buffer[] = [];
      req.on('data', (c: Buffer) => chunks.push(c));
      req.on('end', async () => {
        stats.calls++;
        if (String(req.headers.authorization ?? '').startsWith('AWS4-HMAC-SHA256')) stats.signed++;
        const raw = Buffer.concat(chunks).toString('utf8');
        const url = new URL(req.url ?? '/', 'https://127.0.0.1');

        // Ruta inexistente: API Gateway responde 403 sin el cuerpo de error de la API.
        if (req.method !== 'POST' || url.pathname !== SUCCESS_PATH) {
          stats.poisonCalls++;
          res.writeHead(403, { 'content-type': 'application/json' }).end('{"message":"Missing Authentication Token"}');
          return;
        }

        let proveedor = 'desconocido';
        try {
          proveedor = String((JSON.parse(raw) as { proveedor?: unknown }).proveedor ?? 'desconocido');
        } catch {
          /* body inválido: se atiende igual */
        }
        if (raw.includes('12345678901234567890')) stats.bigIntIntact++;
        const p = (stats.byProvider[proveedor] ??= { calls: 0, inFlight: 0, maxInFlight: 0, overlaps: 0, status: {} });

        const key = String(req.headers['idempotency-key'] ?? '');
        const stored = idempotency.get(key);
        if (stored) {
          stats.idempotentReplays++;
          stats.replays.push(`${proveedor}:${String((JSON.parse(raw) as { cuenta?: unknown }).cuenta)}`);
          res.writeHead(200, { 'content-type': 'application/json' }).end(stored);
          return;
        }

        // Detector de concurrencia: λ2 "llamando al proveedor".
        p.calls++;
        p.inFlight++;
        p.maxInFlight = Math.max(p.maxInFlight, p.inFlight);
        if (p.inFlight > 1) p.overlaps++;
        const behavior = BEHAVIOR[proveedor] ?? { latencyMs: 10 };
        try {
          await sleep(behavior.latencyMs);
          const roll = Math.random();
          let status = 200;
          let body: string;
          const headers: Record<string, string> = { 'content-type': 'application/json' };
          if (roll < (behavior.providerDownRate ?? 0)) {
            status = 503;
            headers['x-error-origin'] = 'provider';
            headers['retry-after'] = '2';
            body = JSON.stringify({ code: 'PROVEEDOR_CAIDO', retryable: true, scope: 'provider', origin: 'provider' });
          } else if (roll < (behavior.providerDownRate ?? 0) + (behavior.recordErrorRate ?? 0)) {
            status = 500;
            headers['x-error-origin'] = 'provider';
            body = JSON.stringify({ code: 'PARSE_ERROR', retryable: true, scope: 'record', origin: 'provider' });
          } else {
            body = JSON.stringify({ proveedor, saldo: Math.round(Math.random() * 100_000) / 100 });
            idempotency.set(key, body);
          }
          p.status[status] = (p.status[status] ?? 0) + 1;
          res.writeHead(status, headers).end(body);
        } finally {
          p.inFlight--;
        }
      });
    },
  );

  await new Promise<void>((resolve) => server.listen(port, '127.0.0.1', resolve));
  return { stats, close: () => new Promise((resolve) => server.close(() => resolve())) };
}
