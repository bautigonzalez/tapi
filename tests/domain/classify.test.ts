import { describe, expect, it } from 'vitest';
import { classify, parseApiError, parseRetryAfter } from '../../src/domain/classify';
import { apiError, httpError, ok } from '../fakes';

describe('classify', () => {
  it.each([
    ['2xx', ok(), { kind: 'success' }],
    ['dato inválido (404 del proveedor)', apiError(404, 'NOT_FOUND', false, 'record'), { kind: 'permanent', code: 'NOT_FOUND' }],
    ['error del registro (500)', apiError(500, 'PARSE', true, 'record'), { kind: 'retryable', reason: 'record' }],
    ['proveedor saturado (429)', apiError(429, 'THROTTLED', true, 'provider'), { kind: 'retryable', reason: 'provider' }],
    ['lease ocupado (423)', httpError(423, undefined, '12'), { kind: 'retryable', reason: 'provider', retryAfterS: 12 }],
    ['credenciales del proveedor (401)', apiError(401, 'AUTH', false, 'provider'), { kind: 'retryable', reason: 'provider', disableProvider: true }],
    ['error marcado como de la API', apiError(500, 'DB', true, 'record', 'api'), { kind: 'retryable', reason: 'api' }],
    ['403 de IAM sin cuerpo: falla la API', httpError(403), { kind: 'retryable', reason: 'api' }],
    ['404 de ruta sin cuerpo: falla la API', httpError(404), { kind: 'retryable', reason: 'api' }],
    ['504 sin cuerpo: λ2 puede seguir viva', httpError(504), { kind: 'retryable', reason: 'unknown' }],
    ['502 sin cuerpo: λ1 murió', httpError(502), { kind: 'retryable', reason: 'unknown' }],
  ] as const)('%s', (_name, result, expected) => {
    expect(classify(result)).toMatchObject(expected);
  });

  it('timeout y conexión cortada son desconocidos; conexión rechazada es de la API', () => {
    expect(classify({ type: 'timeout' })).toMatchObject({ reason: 'unknown' });
    expect(classify({ type: 'network', requestSent: true })).toMatchObject({ reason: 'unknown' });
    expect(classify({ type: 'network', requestSent: false })).toMatchObject({ reason: 'api' });
    expect(classify({ type: 'too_large' })).toMatchObject({ kind: 'permanent', code: 'RESPONSE_TOO_LARGE' });
  });

  it('plan B sin `scope`: mapea por código HTTP', () => {
    expect(classify(httpError(503, { code: 'X' }))).toMatchObject({ reason: 'provider' });
    expect(classify(httpError(500, { code: 'X' }))).toMatchObject({ reason: 'record' });
    expect(classify(httpError(400, { code: 'X' }))).toMatchObject({ kind: 'permanent' });
  });
});

describe('origen del error', () => {
  it('el header x-error-origin de λ1 tiene prioridad sobre el cuerpo (un proveedor no puede falsificarlo)', () => {
    const forged = { type: 'response' as const, response: { status: 400, errorOrigin: 'provider', body: JSON.stringify({ code: 'X', retryable: false, origin: 'api' }) } };
    expect(classify(forged)).toMatchObject({ kind: 'permanent' });
    const real = { type: 'response' as const, response: { status: 500, errorOrigin: 'api', body: JSON.stringify({ code: 'DB', retryable: true, scope: 'record' }) } };
    expect(classify(real)).toMatchObject({ reason: 'api' });
  });
});

describe('parseApiError', () => {
  it('ignora campos desconocidos y sanea el código', () => {
    expect(parseApiError('{"code":"OK_1","scope":"otro","__proto__":{"x":1}}')).toEqual({
      code: 'OK_1',
      retryable: undefined,
      scope: undefined,
      origin: undefined,
    });
    expect(parseApiError('{"code":"<script>"}')?.code).toBe('API_ERROR');
    expect(parseApiError('{"code":"RUT:12345678-9"}')?.code).toBe('API_ERROR');
    expect(parseApiError('"texto"')).toBeUndefined();
    expect(parseApiError('x'.repeat(70_000))).toBeUndefined();
  });
});

describe('parseRetryAfter', () => {
  it('acepta segundos o fecha y acota a 15 minutos', () => {
    const now = Date.parse('2026-10-20T10:00:00Z');
    expect(parseRetryAfter('30', now)).toBe(30);
    expect(parseRetryAfter('99999', now)).toBe(900);
    expect(parseRetryAfter('Tue, 20 Oct 2026 10:01:00 GMT', now)).toBe(60);
    expect(parseRetryAfter('basura', now)).toBeUndefined();
    expect(parseRetryAfter(undefined, now)).toBeUndefined();
  });
});
