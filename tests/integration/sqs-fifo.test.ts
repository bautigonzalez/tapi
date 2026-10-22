import { randomUUID } from 'node:crypto';
import { ChangeMessageVisibilityCommand, CreateQueueCommand, DeleteMessageCommand, DeleteQueueCommand, ReceiveMessageCommand, SendMessageCommand } from '@aws-sdk/client-sqs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { sqsClient } from '../../local/env';

/**
 * Semántica de SQS FIFO sobre ElasticMQ. Es lo que sostiene la exclusión por proveedor.
 * Ojo: ElasticMQ emula SQS; la confirmación definitiva con Lambda + FIFO es el PoC en AWS (semana 1).
 */
const sqs = sqsClient();
let url = '';

beforeAll(async () => {
  const res = await sqs.send(
    new CreateQueueCommand({ QueueName: `int-${randomUUID().slice(0, 8)}.fifo`, Attributes: { FifoQueue: 'true', VisibilityTimeout: '240' } }),
  );
  url = res.QueueUrl ?? '';
});
afterAll(() => sqs.send(new DeleteQueueCommand({ QueueUrl: url })));

const send = (group: string, id: string) =>
  sqs.send(new SendMessageCommand({ QueueUrl: url, MessageBody: id, MessageGroupId: group, MessageDeduplicationId: id }));
const receive = async () =>
  (await sqs.send(new ReceiveMessageCommand({ QueueUrl: url, MaxNumberOfMessages: 1, WaitTimeSeconds: 0 }))).Messages?.[0];

describe('ElasticMQ: SQS FIFO', () => {
  it('un grupo con un mensaje en vuelo no entrega el siguiente, pero otro grupo sí', async () => {
    await send('edenor', 'e1');
    await send('edenor', 'e2');
    await send('aysa', 'a1');
    const first = await receive();
    expect(first?.Body).toBe('e1');
    const second = await receive();
    expect(second?.Body).toBe('a1'); // e2 no sale: edenor tiene e1 en vuelo
    expect(await receive()).toBeUndefined();
    await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: first?.ReceiptHandle }));
    await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: second?.ReceiptHandle }));
    const third = await receive();
    expect(third?.Body).toBe('e2');
    await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: third?.ReceiptHandle }));
  });

  it('retener con ChangeMessageVisibility bloquea el grupo hasta que vence la espera', async () => {
    await send('metrogas', 'm1');
    await send('metrogas', 'm2');
    const held = await receive();
    expect(held?.Body).toBe('m1');
    await sqs.send(new ChangeMessageVisibilityCommand({ QueueUrl: url, ReceiptHandle: held?.ReceiptHandle, VisibilityTimeout: 2 }));
    expect(await receive()).toBeUndefined(); // m2 no sale mientras m1 está retenido
    await new Promise((r) => setTimeout(r, 2_500));
    const again = await receive();
    expect(again?.Body).toBe('m1'); // vuelve el mismo, en orden
    await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: again?.ReceiptHandle }));
    const next = await receive();
    expect(next?.Body).toBe('m2');
    await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: next?.ReceiptHandle }));
  });

  it('deduplica por MessageDeduplicationId (reenvío del Dispatcher)', async () => {
    await send('telecom', 't1');
    await send('telecom', 't1');
    const m = await receive();
    expect(m?.Body).toBe('t1');
    await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: m?.ReceiptHandle }));
    expect(await receive()).toBeUndefined();
  });
});
