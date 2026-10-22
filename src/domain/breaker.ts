import { BREAKER } from '../config';
import type { WindowStats } from './types';

/**
 * Breaker por proveedor: falla más de la mitad de las llamadas de la ventana, contando también los
 * éxitos, y las fallas vienen de varios registros distintos. Unos pocos registros rotos en un
 * proveedor sano no lo abren.
 */
export function providerBreakerOpen(s: WindowStats): boolean {
  return s.calls >= BREAKER.minCalls && s.fails / s.calls > BREAKER.failRate && s.failRecords >= BREAKER.minFailRecords;
}

/**
 * Breaker de permanentes: un salto de FAILED_PERMANENT en todo el sistema suele ser un cambio de IAM
 * o un deploy roto, no datos malos. Pausa el despacho.
 */
export function permanentBreakerOpen(s: WindowStats): boolean {
  return s.calls >= BREAKER.minCallsGlobal && s.permanents / s.calls > BREAKER.permanentRate;
}

/**
 * Breaker de la API: se pausa todo solo si la falla aparece en varios registros y al menos dos
 * proveedores. Un registro envenenado (por ejemplo, una ruta que no existe y devuelve 403 de API
 * Gateway) no alcanza: agota sus intentos y va a la cola de agotados.
 */
export function apiBreakerOpen(s: WindowStats): boolean {
  return s.apiFailRecords >= BREAKER.minApiFailRecords && s.apiFailProviders >= BREAKER.minApiFailProviders;
}

/** Inicio de la ventana de estadísticas que contiene a `nowMs`. */
export function windowStart(nowMs: number): number {
  return nowMs - (nowMs % BREAKER.windowMs);
}
