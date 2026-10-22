/**
 * Protección contra SSRF: la columna `endpoint` es un dato y no se confía en ella.
 *
 * Se valida por segmento con un alfabeto mínimo (sin `%`, `+`, `;`, `@` ni segmentos vacíos o de
 * puntos) y la query solo admite `clave=valor` sin codificar. Así no hay doble codificación,
 * travesías que un servidor normalice distinto (`%252e`, `..;`) ni diferencias entre lo que se
 * firma con SigV4 y lo que se envía. El host siempre es el de la API interna.
 */
const SEGMENT = /^[A-Za-z0-9\-._~]+$/;
const ONLY_DOTS = /^\.+$/;
const QUERY = /^[A-Za-z0-9\-._~=&]*$/;

export function resolveEndpoint(endpoint: string, baseUrl: string, allowedPrefixes: readonly string[]): URL | undefined {
  if (endpoint.length === 0 || endpoint.length > 2_048 || !endpoint.startsWith('/')) return undefined;

  const q = endpoint.indexOf('?');
  const path = q === -1 ? endpoint : endpoint.slice(0, q);
  const query = q === -1 ? '' : endpoint.slice(q + 1);
  if (!QUERY.test(query)) return undefined;
  const segments = path.slice(1).split('/');
  if (segments.some((s) => !SEGMENT.test(s) || ONLY_DOTS.test(s))) return undefined;
  if (!allowedPrefixes.some((prefix) => isUnder(path, prefix))) return undefined;

  const base = new URL(baseUrl);
  if (base.protocol !== 'https:') return undefined;
  const url = new URL(`${base.origin}${base.pathname.replace(/\/+$/, '')}${path}${query ? `?${query}` : ''}`);
  return url.origin === base.origin ? url : undefined;
}

function isUnder(path: string, prefix: string): boolean {
  const p = prefix.replace(/\/+$/, '');
  return p.startsWith('/') && p.length > 1 && (path === p || path.startsWith(`${p}/`));
}
