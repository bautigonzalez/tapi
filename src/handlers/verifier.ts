import { TIMEOUTS } from '../config';
import { capacityStatus, utilization } from '../domain/admission';
import { countOverlaps } from '../domain/overlaps';
import { utcDate } from '../domain/time';
import { log, metric } from '../observability';
import type { CallLog, Clock, ExecutionsRepo, SourceDb } from '../ports';

export interface VerifierDeps {
  repo: ExecutionsRepo;
  source: SourceDb;
  clock: Clock;
  /** Registro de llamadas de λ2. Opcional hasta que el equipo de la API implemente el lease. */
  callLog?: CallLog;
}

/**
 * Verificador diario (23:00 UTC): calcula ρ por proveedor para alertar antes de que empiece el día,
 * mide el SLI de solapamiento sobre las llamadas de λ2 y detecta proveedores nuevos.
 */
export function createVerifier(deps: VerifierDeps) {
  return async (): Promise<void> => {
    const now = deps.clock.now();
    const today = utcDate(now);
    const counts = await deps.source.countByProvider();
    const states = new Map((await deps.repo.listProviders()).map((s) => [s.proveedor, s]));

    // Registros que nunca se van a despachar (body, id o endpoint fuera de los límites): hay que corregirlos en origen.
    const invalid = await deps.source.countInvalid();
    let totalInvalid = 0;
    for (const [proveedor, n] of invalid) {
      totalInvalid += n;
      metric('RegistrosInvalidos', n, { proveedor });
      log.warn('registros_invalidos', { proveedor, cantidad: n });
    }
    metric('RegistrosInvalidosTotal', totalInvalid);
    let totalOverlaps = 0;
    let infeasible = 0;

    for (const [proveedor, records] of counts) {
      const state = states.get(proveedor);
      if (!state) {
        if (await deps.repo.createProviderIfMissing(proveedor)) {
          log.warn('proveedor_nuevo_sin_configurar', { proveedor, registros: records });
          metric('ProveedoresSinConfigurar', 1);
        }
        continue;
      }

      // Tiempo de ocupación: lo que el grupo estuvo ocupado (procesando o retenido) por mensaje completado.
      const busy = await deps.repo.getDailyBusy(proveedor, today);
      const occupancyS = busy.completed > 0 ? (busy.busyMs + busy.holdMs) / busy.completed / 1_000 : state.occupancyS;
      if (busy.completed > 0) await deps.repo.setOccupancy(proveedor, occupancyS);

      const rho = utilization(records, occupancyS, state.lanes);
      const status = capacityStatus(rho);
      metric('Utilizacion', Number(rho.toFixed(3)), { proveedor }, 'None');
      if (status !== 'OK') log.warn('capacidad', { proveedor, rho: Number(rho.toFixed(2)), estado: status, registros: records });
      if (status === 'INVIABLE') infeasible++;

      if (deps.callLog) {
        const calls = await deps.callLog.listCalls(proveedor, now - 48 * 3_600_000, now);
        const overlaps = countOverlaps(calls, TIMEOUTS.lambda2MaxMs);
        totalOverlaps += overlaps;
        metric('Solapamientos', overlaps, { proveedor });
        if (overlaps > 0) log.error('solapamiento_detectado', { proveedor, cantidad: overlaps });
      }
    }

    // Totales sin dimensiones: son los que miran las alarmas.
    metric('SolapamientosTotal', totalOverlaps);
    metric('ProveedoresInviables', infeasible);
  };
}
