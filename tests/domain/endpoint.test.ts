import { describe, expect, it } from 'vitest';
import { resolveEndpoint } from '../../src/domain/endpoint';

const BASE = 'https://abc123.execute-api.us-east-1.amazonaws.com/prod';
const ALLOW = ['/consultas'];

describe('resolveEndpoint (SSRF)', () => {
  it('acepta rutas dentro de la allowlist y conserva el stage y la query', () => {
    expect(resolveEndpoint('/consultas/saldo', BASE, ALLOW)?.href).toBe(
      'https://abc123.execute-api.us-east-1.amazonaws.com/prod/consultas/saldo',
    );
    expect(resolveEndpoint('/consultas?cuenta=1&tipo=a', BASE, ALLOW)?.href).toBe(
      'https://abc123.execute-api.us-east-1.amazonaws.com/prod/consultas?cuenta=1&tipo=a',
    );
  });

  it.each([
    ['URL absoluta', 'https://169.254.169.254/latest/meta-data'],
    ['host relativo al protocolo', '//evil.example.com/consultas'],
    ['travesía', '/consultas/../admin'],
    ['punto', '/consultas/./saldo/../../admin'],
    ['travesía codificada', '/consultas/%2e%2e/admin'],
    ['barra codificada', '/consultas%2f..%2fadmin'],
    ['barra invertida', '/consultas\\..\\admin'],
    ['byte nulo', '/consultas/%00'],
    ['prefijo confundible', '/consultas-admin/x'],
    ['fuera de la allowlist', '/admin/usuarios'],
    ['fragmento', '/consultas#x'],
    ['espacios', '/consultas/a b'],
    ['sin barra inicial', 'consultas/saldo'],
    ['vacío', ''],
    ['credenciales', '/@evil.example.com'],
    ['muy largo', `/consultas/${'a'.repeat(3_000)}`],
    ['doble codificación', '/consultas/%252e%252e/%252e%252e/admin'],
    ['punto y coma (Tomcat/Spring)', '/consultas/..;/admin'],
    ['segmento vacío', '/consultas//saldo'],
    ['tres puntos', '/consultas/.../admin'],
    ['UTF-8 overlong', '/consultas/%c0%ae'],
    ['porcentaje suelto', '/consultas/%'],
    ['más en la query (firma SigV4)', '/consultas?q=a+b'],
    ['URL en la query', '/consultas?u=https://evil.example/'],
    ['arroba y dos puntos', '/consultas/a@b:c'],
    ['barra final', '/consultas/'],
  ])('rechaza: %s', (_name, endpoint) => {
    expect(resolveEndpoint(endpoint, BASE, ALLOW)).toBeUndefined();
  });

  it('el host siempre es el de la API', () => {
    const url = resolveEndpoint('/consultas/saldo-2026_10.v1~x', BASE, ALLOW);
    expect(url?.host).toBe('abc123.execute-api.us-east-1.amazonaws.com');
  });

  it('sin allowlist configurada no se acepta nada (deny by default)', () => {
    expect(resolveEndpoint('/consultas/saldo', BASE, [])).toBeUndefined();
    expect(resolveEndpoint('/consultas/saldo', BASE, ['/'])).toBeUndefined();
  });

  it('rechaza una base que no es HTTPS', () => {
    expect(resolveEndpoint('/consultas/saldo', 'http://api.local', ALLOW)).toBeUndefined();
  });
});
