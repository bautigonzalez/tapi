import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { GetQueueUrlCommand, SQSClient } from '@aws-sdk/client-sqs';

/** Configuración del entorno local. Las credenciales son de los emuladores, no secretos. */
export const LOCAL = {
  region: 'us-east-1',
  credentials: { accessKeyId: 'local', secretAccessKey: 'local' },
  dynamoEndpoint: 'http://127.0.0.1:8000',
  sqsEndpoint: 'http://127.0.0.1:9324',
  postgresUrl: 'postgres://local:local@127.0.0.1:5433/registros',
  table: 'consultas-executions-local',
  jobsQueue: 'consultas-jobs-local.fifo',
  dlqQueue: 'consultas-jobs-dlq-local.fifo',
  exhaustedQueue: 'consultas-agotados-local',
  apiPort: 8443,
  apiBaseUrl: 'https://127.0.0.1:8443/prod',
} as const;

export const PROVIDERS = ['edenor', 'aysa', 'metrogas', 'telecom', 'inestable'] as const;

export const dynamoClient = () => new DynamoDBClient({ region: LOCAL.region, credentials: LOCAL.credentials, endpoint: LOCAL.dynamoEndpoint });
export const sqsClient = () => new SQSClient({ region: LOCAL.region, credentials: LOCAL.credentials, endpoint: LOCAL.sqsEndpoint });

export async function queueUrl(client: SQSClient, name: string): Promise<string> {
  const res = await client.send(new GetQueueUrlCommand({ QueueName: name }));
  if (!res.QueueUrl) throw new Error(`No existe la cola ${name}: correr npm run local:setup`);
  return res.QueueUrl;
}
