import { createHash } from 'node:crypto';
import { ChangeMessageVisibilityCommand, SendMessageBatchCommand, SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { SQS_BATCH_MAX_BYTES } from '../config';
import type { Job } from '../domain/types';
import type { ExhaustedEntry, JobQueue } from '../ports';

const VALID_GROUP_ID = /^[\x21-\x7E]{1,128}$/;

/** MessageGroupId = proveedor. Si el nombre tiene caracteres que SQS no acepta, se usa su hash. */
export function groupIdOf(proveedor: string): string {
  return VALID_GROUP_ID.test(proveedor) ? proveedor : `p_${createHash('sha256').update(proveedor, 'utf8').digest('hex')}`;
}

/** Arma lotes de hasta 10 mensajes y 1 MiB en total, sin cambiar el orden. */
export function chunkByBytes(bodies: readonly string[], maxBytes = SQS_BATCH_MAX_BYTES, maxCount = 10): number[][] {
  const chunks: number[][] = [];
  let current: number[] = [];
  let bytes = 0;
  bodies.forEach((body, index) => {
    const size = Buffer.byteLength(body, 'utf8');
    if (current.length > 0 && (current.length === maxCount || bytes + size > maxBytes)) {
      chunks.push(current);
      current = [];
      bytes = 0;
    }
    current.push(index);
    bytes += size;
  });
  if (current.length > 0) chunks.push(current);
  return chunks;
}

export class SqsQueue implements JobQueue {
  constructor(
    private readonly jobsQueueUrl: string,
    private readonly exhaustedQueueUrl: string,
    private readonly client = new SQSClient({}),
  ) {}

  async send(jobs: readonly Job[]): Promise<Set<string>> {
    const sent = new Set<string>();
    const bodies = jobs.map((job) => JSON.stringify(job));
    for (const chunk of chunkByBytes(bodies)) {
      const res = await this.client.send(
        new SendMessageBatchCommand({
          QueueUrl: this.jobsQueueUrl,
          Entries: chunk.map((index) => {
            const job = jobs[index]!;
            return {
              Id: String(index),
              MessageBody: bodies[index]!,
              MessageGroupId: groupIdOf(job.proveedor),
              // Dentro de los 5 minutos de deduplicación, un reenvío del mismo execId se descarta.
              MessageDeduplicationId: job.execId,
            };
          }),
        }),
      );
      for (const ok of res.Successful ?? []) {
        const job = jobs[Number(ok.Id)];
        if (job) sent.add(job.execId);
      }
    }
    return sent;
  }

  async hold(receiptHandle: string, seconds: number): Promise<void> {
    await this.client.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: this.jobsQueueUrl,
        ReceiptHandle: receiptHandle,
        VisibilityTimeout: Math.min(43_200, Math.max(0, Math.round(seconds))),
      }),
    );
  }

  /** La cola de agotados lleva solo ids: los datos completos están en el ledger. */
  async sendExhausted(entry: ExhaustedEntry): Promise<void> {
    await this.client.send(new SendMessageCommand({ QueueUrl: this.exhaustedQueueUrl, MessageBody: JSON.stringify(entry) }));
  }
}
