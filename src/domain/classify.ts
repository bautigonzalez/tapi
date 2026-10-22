import type { Outcome, WaitingReason } from './types';

export interface ApiResponse {
  status: number;
  retryAfter?: string;
  /**
   * Header `x-error-origin` que agrega λ1 (`api` | `provider`). Tiene prioridad sobre el cuerpo:
   * un proveedor no puede falsificarlo aunque la API le reenvíe su cuerpo de error.
   */
  errorOrigin?: string;
  body: string;
}

export type CallResult =
  | { type: 'response'; response: ApiResponse }
  | { type: 'timeout' }
  /** `requestSent` es falso solo si la conexión ni siquiera se estableció. */
  | { type: 'network'; requestSent: boolean }
  | { type: 'too_large' };

/** Contrato de error que expone la API: `{ code, retryable, scope, origin }`. */
export interface ApiErrorBody {
  code: string;
  retryable?: boolean;
  scope?: 'record' | 'provider';
  origin?: 'api' | 'provider';
}

/** Códigos tipo constante (NOT_FOUND, PARSE_ERROR): termina en logs y en la cola de agotados, no admite texto libre. */
const CODE_PATTERN = /^[A-Z][A-Z0-9_]{0,63}$/;
const MAX_RETRY_AFTER_S = 900;

function retry(reason: WaitingReason, code: string, httpStatus?: number, retryAfterS?: number): Outcome {
  return { kind: 'retryable', reason, code, httpStatus, retryAfterS };
}

/** Traduce el resultado de una llamada a la API en una acción (docs/ARQUITECTURA.md §5). */
export function classify(result: CallResult, nowMs: number = Date.now()): Outcome {
  switch (result.type) {
    case 'timeout':
      return retry('unknown', 'TIMEOUT');
    case 'network':
      return result.requestSent ? retry('unknown', 'CONNECTION_LOST') : retry('api', 'CONNECTION_FAILED');
    case 'too_large':
      return { kind: 'permanent', code: 'RESPONSE_TOO_LARGE' };
    case 'response':
      break;
  }

  const { status, body } = result.response;
  if (status >= 200 && status < 300) return { kind: 'success', httpStatus: status, body };

  const retryAfterS = parseRetryAfter(result.response.retryAfter, nowMs);
  // 423: el lease del proveedor en λ2 está tomado. Hay que esperar sin gastar intentos.
  if (status === 423) return retry('provider', 'LEASE_BUSY', status, retryAfterS);

  const error = parseApiError(body);
  if (!error) {
    // Sin el cuerpo de error de la API, la respuesta la generó API Gateway o λ1, no el proveedor.
    // 502 y 504 pueden esconder una λ2 todavía viva: son resultados desconocidos.
    if (status === 502 || status === 504) return retry('unknown', `HTTP_${status}`, status);
    return retry('api', `HTTP_${status}`, status, retryAfterS);
  }

  const headerOrigin = result.response.errorOrigin;
  const origin = headerOrigin === 'api' || headerOrigin === 'provider' ? headerOrigin : error.origin;
  if (origin === 'api') return retry('api', error.code, status, retryAfterS);
  if (status === 401 || status === 403) {
    return { kind: 'retryable', reason: 'provider', code: error.code, httpStatus: status, retryAfterS, disableProvider: true };
  }

  const retryable = error.retryable ?? (status >= 500 || status === 429);
  if (!retryable) return { kind: 'permanent', httpStatus: status, code: error.code };
  if (error.scope) return retry(error.scope, error.code, status, retryAfterS);

  // Plan B si la API no expone `scope`: mapeo por código HTTP.
  if (status === 429 || status === 503) return retry('provider', error.code, status, retryAfterS);
  if (status === 502 || status === 504) return retry('unknown', error.code, status);
  return retry('record', error.code, status, retryAfterS);
}

/** Valida el cuerpo de error. Es una entrada no confiable: solo se aceptan campos y valores conocidos. */
export function parseApiError(body: string): ApiErrorBody | undefined {
  if (!body || body.length > 64 * 1_024) return undefined;
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    return undefined;
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) return undefined;
  const o = parsed as Record<string, unknown>;
  if (typeof o.code !== 'string') return undefined;
  return {
    code: CODE_PATTERN.test(o.code) ? o.code : 'API_ERROR',
    retryable: typeof o.retryable === 'boolean' ? o.retryable : undefined,
    scope: o.scope === 'record' || o.scope === 'provider' ? o.scope : undefined,
    origin: o.origin === 'api' || o.origin === 'provider' ? o.origin : undefined,
  };
}

/** `Retry-After` en segundos o como fecha HTTP, acotado a [0, 900]. */
export function parseRetryAfter(value: string | undefined, nowMs: number = Date.now()): number | undefined {
  if (!value) return undefined;
  const trimmed = value.trim();
  const seconds = /^\d{1,6}$/.test(trimmed)
    ? Number(trimmed)
    : Math.ceil((Date.parse(trimmed) - nowMs) / 1_000);
  if (!Number.isFinite(seconds)) return undefined;
  return Math.min(Math.max(seconds, 0), MAX_RETRY_AFTER_S);
}
