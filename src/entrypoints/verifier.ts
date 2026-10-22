import { DynamoExecutionsRepo } from '../adapters/dynamo';
import { createIamPool, PostgresSource } from '../adapters/postgres';
import { DynamoCallLog } from '../api-lease/provider-lease';
import { requireEnv } from '../config';
import { createVerifier } from '../handlers/verifier';
import { systemClock } from '../ports';

const leasesTable = process.env.LEASES_TABLE_NAME;

export const handler = createVerifier({
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
  clock: systemClock,
  callLog: leasesTable ? new DynamoCallLog(leasesTable) : undefined,
});
