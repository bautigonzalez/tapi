import { DynamoExecutionsRepo } from '../adapters/dynamo';
import { SignedHttpApi } from '../adapters/http';
import { S3ResultStore } from '../adapters/s3';
import { SqsQueue } from '../adapters/sqs';
import { requireEnv } from '../config';
import { createWorker } from '../handlers/worker';
import { systemClock } from '../ports';

export const handler = createWorker({
  repo: new DynamoExecutionsRepo(requireEnv('TABLE_NAME')),
  queue: new SqsQueue(requireEnv('JOBS_QUEUE_URL'), requireEnv('EXHAUSTED_QUEUE_URL')),
  api: new SignedHttpApi(requireEnv('AWS_REGION')),
  store: new S3ResultStore(requireEnv('RESULTS_BUCKET'), requireEnv('KMS_KEY_ID')),
  clock: systemClock,
  config: {
    apiBaseUrl: requireEnv('API_BASE_URL'),
    leaseInApi: process.env.LEASE_IN_API === 'true',
  },
});
