/**
 * Simulación de punta a punta contra los emuladores: Dispatcher y Worker reales (los mismos handlers
 * que corren en Lambda), Postgres, DynamoDB Local, ElasticMQ (SQS FIFO) y la API mock por HTTPS.
 *
 * El reloj está corrido al final del día UTC: todos los registros de hoy ya vencieron, así que se
 * despacha el día completo en pocos minutos. Los pollers emulan el event source mapping de Lambda:
 * un mensaje por vez, se borra si el handler devuelve éxito y si no vuelve solo a la cola.
 *
 * Al final verifica las garantías del diseño y termina con código 1 si alguna falla.
 */
import { ScanCommand, type AttributeValue } from '@aws-sdk/client-dynamodb';
import { DeleteMessageCommand, GetQueueAttributesCommand, ReceiveMessageCommand } from '@aws-sdk/client-sqs';
import type { SQSEvent } from 'aws-lambda';
import { Pool } from 'pg';
import { DynamoExecutionsRepo } from '../src/adapters/dynamo';
import { SignedHttpApi } from '../src/adapters/http';
import { PostgresSource } from '../src/adapters/postgres';
import { SqsQueue } from '../src/adapters/sqs';
import { utcDate } from '../src/domain/time';
import type { Job } from '../src/domain/types';
import { createDispatcher } from '../src/handlers/dispatcher';
import { createWorker } from '../src/handlers/worker';
import type { Clock, ResultStore } from '../src/ports';
import { dynamoClient, LOCAL, PROVIDERS, queueUrl, sqsClient } from './env';
import { startMockApi } from './mock-api';

process.env.AWS_ACCESS_KEY_ID ??= LOCAL.credentials.accessKeyId;
process.env.AWS_SECRET_ACCESS_KEY ??= LOCAL.credentials.secretAccessKey;

const DURATION_S = Number(process.env.DURACION_S ?? 240);
const POLLERS = Number(process.env.POLLERS ?? 8);
const DISPATCH_EVERY_MS = 5_000;

// Reloj del sistema corrido a las 23:55 UTC de hoy (el mismo día que el cursor de local/setup.ts):
// vence casi todo el día y la corrida no cruza la medianoche.
const today = utcDate(Date.now());
const offset = Date.parse(`${today}T23:55:00.000Z`) - Date.now();
const clock: Clock = { now: () => Date.now() + offset };

class MemoryStore implements ResultStore {
  keys: string[] = [];
  async put(job: Job): Promise<string> {
    const key = `resultados/${job.runDate}/${job.execId}.json`;
    this.keys.push(key);
    return key;
  }
}

async function main(): Promise<void> {
  const ddb = dynamoClient();
  const sqs = sqsClient();
  const jobsUrl = await queueUrl(sqs, LOCAL.jobsQueue);
  const exhaustedUrl = await queueUrl(sqs, LOCAL.exhaustedQueue);
  const dlqUrl = await queueUrl(sqs, LOCAL.dlqQueue);

  const api = await startMockApi(LOCAL.apiPort);
  const repo = new DynamoExecutionsRepo(LOCAL.table, ddb);
  const queue = new SqsQueue(jobsUrl, exhaustedUrl, sqs);
  const pool = new Pool({ connectionString: LOCAL.postgresUrl, max: 2 });
  const dispatcher = createDispatcher({ repo, source: new PostgresSource(pool), queue, clock });
  const worker = createWorker({
    repo,
    queue,
    api: new SignedHttpApi(LOCAL.region),
    store: new MemoryStore(),
    clock,
    config: { apiBaseUrl: LOCAL.apiBaseUrl, leaseInApi: false },
  });

  const stopAt = Date.now() + DURATION_S * 1_000;
  let handlerErrors = 0;
  let processed = 0;
  console.log(`Simulando ${DURATION_S} s con ${POLLERS} pollers. Reloj del sistema: ${new Date(clock.now()).toISOString()}`);

  const dispatchLoop = async () => {
    while (Date.now() < stopAt) {
      await dispatcher().catch((e: unknown) => console.error('dispatcher', e));
      await new Promise((r) => setTimeout(r, DISPATCH_EVERY_MS));
    }
  };

  // Emula el event source mapping de Lambda con batch 1.
  const poller = async () => {
    while (Date.now() < stopAt) {
      const res = await sqs.send(
        new ReceiveMessageCommand({ QueueUrl: jobsUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 1, MessageSystemAttributeNames: ['All'] }),
      );
      for (const m of res.Messages ?? []) {
        const event = {
          Records: [
            {
              messageId: m.MessageId,
              receiptHandle: m.ReceiptHandle,
              body: m.Body,
              attributes: { ApproximateReceiveCount: m.Attributes?.ApproximateReceiveCount ?? '1' },
            },
          ],
        } as unknown as SQSEvent;
        try {
          const out = await worker(event);
          processed++;
          if (out.batchItemFailures.length === 0) await sqs.send(new DeleteMessageCommand({ QueueUrl: jobsUrl, ReceiptHandle: m.ReceiptHandle }));
        } catch (error) {
          // Como en Lambda: un error no controlado deja el mensaje en la cola hasta que vence la visibilidad.
          handlerErrors++;
          console.error('worker', error);
        }
      }
    }
  };

  const progress = setInterval(() => {
    const done = Object.values(api.stats.byProvider).reduce((a, p) => a + p.calls, 0);
    console.log(`  … ${Math.round((stopAt - Date.now()) / 1_000)} s restantes · mensajes procesados ${processed} · llamadas al proveedor ${done}`);
  }, 20_000);
  await Promise.all([dispatchLoop(), ...Array.from({ length: POLLERS }, poller)]);
  clearInterval(progress);

  // ---------- Reporte ----------
  const statuses: Record<string, number> = {};
  const runs = new Map<string, Record<string, unknown>>();
  let startKey: Record<string, AttributeValue> | undefined;
  do {
    const page = await ddb.send(new ScanCommand({ TableName: LOCAL.table, ExclusiveStartKey: startKey }));
    for (const item of page.Items ?? []) {
      if (!item.PK?.S?.startsWith('REC#')) continue;
      const status = item.status?.S ?? '?';
      statuses[status] = (statuses[status] ?? 0) + 1;
      runs.set(item.PK.S.slice(4), { status, attempt: item.attempt?.N, code: item.errorCode?.S, reason: item.waitingReason?.S });
    }
    startKey = page.LastEvaluatedKey;
  } while (startKey);

  const count = async (url: string) =>
    Number((await sqs.send(new GetQueueAttributesCommand({ QueueUrl: url, AttributeNames: ['ApproximateNumberOfMessages'] }))).Attributes?.ApproximateNumberOfMessages ?? 0);
  const global = await repo.getGlobalView(clock.now());
  const providers = await repo.listProviders();

  console.log('\n=== Ledger (DynamoDB) ===');
  console.table(statuses);
  console.log('=== Proveedores (API mock) ===');
  console.table(
    Object.fromEntries(
      PROVIDERS.map((p) => {
        const s = api.stats.byProvider[p];
        const state = providers.find((x) => x.proveedor === p);
        return [p, { llamadas: s?.calls ?? 0, maxEnVuelo: s?.maxInFlight ?? 0, solapamientos: s?.overlaps ?? 0, respuestas: JSON.stringify(s?.status ?? {}), backoff: state?.backoffLevel }];
      }),
    ),
  );
  const poison = runs.get('reg-veneno');
  const ssrf = runs.get('reg-ssrf');
  console.log('=== Casos especiales ===');
  console.table({
    'reg-veneno (ruta inexistente)': poison,
    'reg-ssrf (endpoint fuera de la allowlist)': ssrf,
  });
  const exhaustedCount = await count(exhaustedUrl);
  const dlqCount = await count(dlqUrl);
  console.log(
    `Llamadas a la API: ${api.stats.calls} (firmadas SigV4: ${api.stats.signed}) · reintentos idempotentes: ${api.stats.idempotentReplays} · ` +
      `body con entero de 20 dígitos intacto: ${api.stats.bigIntIntact} · cola de agotados: ${exhaustedCount} · DLQ: ${dlqCount} · ` +
      `pausa global: ${global.pausedUntil > clock.now() ? 'SÍ' : 'no'} · errores no controlados: ${handlerErrors}`,
  );
  if (api.stats.replays.length > 0) {
    console.log('Reintentos idempotentes (proveedor:cuenta → estado en el ledger):');
    for (const r of api.stats.replays) {
      const cuenta = r.split(':')[1];
      console.log(`  ${r} → ${JSON.stringify(runs.get(`reg-${cuenta}`))}`);
    }
  }

  // ---------- Verificaciones ----------
  const providerStats = Object.values(api.stats.byProvider);
  const succeededCalls = providerStats.reduce((a, p) => a + (p.status['200'] ?? 0), 0);
  const checks: Array<[string, boolean]> = [
    ['Nunca hubo dos llamadas simultáneas al mismo proveedor', providerStats.every((p) => p.maxInFlight <= 1 && p.overlaps === 0)],
    ['Todas las llamadas salieron firmadas con SigV4', api.stats.signed === api.stats.calls],
    ['El body llegó intacto (entero de 20 dígitos sin redondear)', api.stats.bigIntIntact > 0 && api.stats.bigIntIntact >= succeededCalls],
    ['El registro envenenado no pausó el sistema', global.pausedUntil <= clock.now()],
    ['El registro envenenado agotó sus intentos y fue a la cola de agotados', poison?.status === 'FAILED_EXHAUSTED' && exhaustedCount >= 1],
    ['El intento de SSRF se rechazó sin llamar a la API', ssrf?.status === 'FAILED_PERMANENT' && ssrf?.code === 'ENDPOINT_NOT_ALLOWED'],
    ['Ningún mensaje cayó a la DLQ de seguridad', dlqCount === 0],
    ['Sin errores no controlados en el worker', handlerErrors === 0],
    ['Se completaron consultas en los 5 proveedores', PROVIDERS.every((p) => (api.stats.byProvider[p]?.status['200'] ?? 0) > 0)],
  ];
  console.log('\n=== Verificaciones ===');
  for (const [name, ok] of checks) console.log(`${ok ? '✔' : '✘'} ${name}`);

  await api.close();
  await pool.end();
  process.exit(checks.every(([, ok]) => ok) ? 0 : 1);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
