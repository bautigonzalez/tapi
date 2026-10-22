/**
 * Crea en los emuladores lo mismo que crea el stack de CDK: la tabla `executions` con sus GSIs, la cola
 * FIFO con su DLQ, la cola de agotados y la configuración de los proveedores. Es idempotente.
 */
import { CreateTableCommand, DeleteTableCommand, ResourceNotFoundException } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';
import { CreateQueueCommand, GetQueueAttributesCommand, PurgeQueueCommand } from '@aws-sdk/client-sqs';
import { encodeCursor } from '../src/domain/cursor';
import { utcDate } from '../src/domain/time';
import { dynamoClient, LOCAL, PROVIDERS, sqsClient } from './env';

async function createTable(): Promise<void> {
  const client = dynamoClient();
  try {
    await client.send(new DeleteTableCommand({ TableName: LOCAL.table }));
  } catch (error) {
    if (!(error instanceof ResourceNotFoundException)) throw error;
  }
  await client.send(
    new CreateTableCommand({
      TableName: LOCAL.table,
      BillingMode: 'PAY_PER_REQUEST',
      KeySchema: [
        { AttributeName: 'PK', KeyType: 'HASH' },
        { AttributeName: 'SK', KeyType: 'RANGE' },
      ],
      AttributeDefinitions: [
        { AttributeName: 'PK', AttributeType: 'S' },
        { AttributeName: 'SK', AttributeType: 'S' },
        { AttributeName: 'openProvider', AttributeType: 'S' },
        { AttributeName: 'turno', AttributeType: 'N' },
        { AttributeName: 'entity', AttributeType: 'S' },
        { AttributeName: 'proveedor', AttributeType: 'S' },
      ],
      GlobalSecondaryIndexes: [
        {
          IndexName: 'open',
          KeySchema: [
            { AttributeName: 'openProvider', KeyType: 'HASH' },
            { AttributeName: 'turno', KeyType: 'RANGE' },
          ],
          Projection: { ProjectionType: 'INCLUDE', NonKeyAttributes: ['status'] },
        },
        {
          IndexName: 'providers',
          KeySchema: [
            { AttributeName: 'entity', KeyType: 'HASH' },
            { AttributeName: 'proveedor', KeyType: 'RANGE' },
          ],
          Projection: { ProjectionType: 'ALL' },
        },
      ],
    }),
  );
}

async function createQueues(): Promise<void> {
  const client = sqsClient();
  const dlq = await client.send(new CreateQueueCommand({ QueueName: LOCAL.dlqQueue, Attributes: { FifoQueue: 'true' } }));
  const dlqArn = (await client.send(new GetQueueAttributesCommand({ QueueUrl: dlq.QueueUrl, AttributeNames: ['QueueArn'] }))).Attributes?.QueueArn;
  const jobs = await client.send(
    new CreateQueueCommand({
      QueueName: LOCAL.jobsQueue,
      Attributes: {
        FifoQueue: 'true',
        ContentBasedDeduplication: 'false',
        VisibilityTimeout: '240',
        RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlqArn, maxReceiveCount: 1_000 }),
      },
    }),
  );
  const exhausted = await client.send(new CreateQueueCommand({ QueueName: LOCAL.exhaustedQueue }));
  for (const url of [jobs.QueueUrl, dlq.QueueUrl, exhausted.QueueUrl]) {
    await client.send(new PurgeQueueCommand({ QueueUrl: url }));
  }
}

/** Proveedores habilitados, con allowlist y el cursor al inicio del día: todo lo que venció hoy está pendiente. */
async function seedProviders(): Promise<void> {
  const doc = DynamoDBDocumentClient.from(dynamoClient());
  const today = utcDate(Date.now());
  for (const proveedor of PROVIDERS) {
    await doc.send(
      new PutCommand({
        TableName: LOCAL.table,
        Item: {
          PK: `PROV#${proveedor}`,
          SK: 'STATE',
          entity: 'PROVIDER',
          proveedor,
          enabled: true,
          lanes: 1,
          occupancyS: 1,
          // Amplia a propósito: el registro envenenado tiene que pasar la allowlist y llegar a la API.
          allowlist: ['/consultas'],
          pausedUntil: 0,
          backoffLevel: 0,
          cursor: encodeCursor({ date: today, slot: 0, id: '' }),
        },
      }),
    );
  }
}

async function main(): Promise<void> {
  await createTable();
  await createQueues();
  await seedProviders();
  console.log(`Listo: tabla ${LOCAL.table}, colas ${LOCAL.jobsQueue} / ${LOCAL.dlqQueue} / ${LOCAL.exhaustedQueue}, ${PROVIDERS.length} proveedores.`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
