import { DynamoExecutionsRepo } from '../adapters/dynamo';
import { createIamPool, PostgresSource } from '../adapters/postgres';
import { SqsQueue } from '../adapters/sqs';
import { requireEnv } from '../config';
import { createDispatcher } from '../handlers/dispatcher';
import { systemClock } from '../ports';

export const handler = createDispatcher({
  repo: new DynamoExecutionsRepo(requireEnv('TABLE_NAME')),
  source: new PostgresSource(
    createIamPool({
      host: requireEnv('DB_HOST'),
      port: Number(process.env.DB_PORT ?? 5432),
      database: requireEnv('DB_NAME'),
      user: requireEnv('DB_USER'),
      region: requireEnv('AWS_REGION'),
      caPath: requireEnv('DB_CA_PATH'),
    }),
  ),
  queue: new SqsQueue(requireEnv('JOBS_QUEUE_URL'), requireEnv('EXHAUSTED_QUEUE_URL')),
  clock: systemClock,
});
