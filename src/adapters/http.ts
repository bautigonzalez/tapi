import { Sha256 } from '@aws-crypto/sha256-js';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import { HttpRequest } from '@smithy/protocol-http';
import { SignatureV4 } from '@smithy/signature-v4';
import { RESPONSE_MAX_BYTES } from '../config';
import type { CallResult } from '../domain/classify';
import type { Job } from '../domain/types';
import type { InternalApi } from '../ports';

/** Errores en los que la conexión nunca se estableció: el request no llegó a salir. */
const NOT_SENT_CODES = new Set(['ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN', 'EHOSTUNREACH', 'ENETUNREACH', 'CERT_HAS_EXPIRED', 'UNABLE_TO_VERIFY_LEAF_SIGNATURE']);

/**
 * Cliente de la API interna privada. Firma con IAM (SigV4), no sigue redirecciones y no reintenta:
 * los reintentos ocurren en una sola capa, el worker (docs/ARQUITECTURA.md §4, capa 3).
 */
export class SignedHttpApi implements InternalApi {
  private readonly signer: SignatureV4;

  constructor(region: string) {
    this.signer = new SignatureV4({ service: 'execute-api', region, credentials: defaultProvider(), sha256: Sha256 });
  }

  async call(job: Job, url: URL, timeoutMs: number): Promise<CallResult> {
    const payload = typeof job.body === 'string' ? job.body : JSON.stringify(job.body ?? {});
    const query: Record<string, string | string[]> = {};
    for (const [key, value] of url.searchParams) {
      const prev = query[key];
      query[key] = prev === undefined ? value : Array.isArray(prev) ? [...prev, value] : [prev, value];
    }
    const request = new HttpRequest({
      method: 'POST',
      protocol: url.protocol,
      hostname: url.hostname,
      path: url.pathname,
      query,
      headers: {
        host: url.hostname,
        'content-type': 'application/json',
        'idempotency-key': job.execId,
        'x-correlation-id': job.execId,
        // El lease en λ2 se aplica al tráfico del batch (docs §4).
        'x-batch-caller': 'consultas-diarias',
      },
      body: payload,
    });
    const signed = await this.signer.sign(request);

    let response: Response;
    try {
      response = await fetch(url, {
        method: 'POST',
        headers: signed.headers as Record<string, string>,
        body: payload,
        redirect: 'manual',
        signal: AbortSignal.timeout(timeoutMs),
      });
    } catch (error) {
      if (isTimeout(error)) return { type: 'timeout' };
      return { type: 'network', requestSent: !NOT_SENT_CODES.has(causeCode(error) ?? '') };
    }

    try {
      const body = await readLimited(response, RESPONSE_MAX_BYTES);
      if (body === undefined) return { type: 'too_large' };
      return {
        type: 'response',
        response: {
          status: response.status,
          retryAfter: response.headers.get('retry-after') ?? undefined,
          errorOrigin: response.headers.get('x-error-origin') ?? undefined,
          body,
        },
      };
    } catch (error) {
      if (isTimeout(error)) return { type: 'timeout' };
      return { type: 'network', requestSent: true };
    }
  }
}

/** Lee la respuesta cortando si supera el máximo: protege la memoria del worker. */
async function readLimited(response: Response, maxBytes: number): Promise<string | undefined> {
  const declared = Number(response.headers.get('content-length') ?? 0);
  if (declared > maxBytes) {
    await response.body?.cancel();
    return undefined;
  }
  if (!response.body) return '';
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > maxBytes) {
      await reader.cancel();
      return undefined;
    }
    chunks.push(value);
  }
  return Buffer.concat(chunks).toString('utf8');
}

function isTimeout(error: unknown): boolean {
  const name = (error as { name?: string })?.name;
  return name === 'TimeoutError' || name === 'AbortError';
}

function causeCode(error: unknown): string | undefined {
  return (error as { cause?: { code?: string } })?.cause?.code;
}
