import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import type { Job } from '../domain/types';
import type { ResultStore } from '../ports';

/** Respuestas de más de 8 KB: van a S3 cifradas con KMS y la ejecución guarda solo la clave. */
export class S3ResultStore implements ResultStore {
  constructor(
    private readonly bucket: string,
    private readonly kmsKeyId: string,
    private readonly client = new S3Client({}),
  ) {}

  async put(job: Job, body: string): Promise<string> {
    const key = `resultados/${job.runDate}/${job.execId}.json`;
    await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        Body: body,
        ContentType: 'application/json',
        ServerSideEncryption: 'aws:kms',
        SSEKMSKeyId: this.kmsKeyId,
      }),
    );
    return key;
  }
}
