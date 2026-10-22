import { ADMISSION, DAY_S } from '../config';

/** Tope de mensajes en cola por proveedor: hasta 15 minutos de trabajo de su grupo. */
export function capFor(occupancyS: number, lanes = 1): number {
  const raw = Math.ceil((ADMISSION.workS * Math.max(lanes, 1)) / Math.max(occupancyS, 0.01));
  return Math.min(ADMISSION.maxCap, Math.max(ADMISSION.minCap, raw));
}

export interface Budget {
  caps: Map<string, number>;
  /** Ni con el mínimo por proveedor entra en la cota: hay que repartir proveedores en más colas. */
  overBudget: boolean;
}

/** Escala los topes en proporción para que la suma no supere la cota global por cola (60.000). */
export function fitToBudget(caps: Map<string, number>, budget: number = ADMISSION.queueBudget): Budget {
  const total = sum(caps.values());
  if (total <= budget) return { caps, overBudget: false };
  const factor = budget / total;
  const scaled = new Map([...caps].map(([p, cap]) => [p, Math.max(ADMISSION.minCap, Math.floor(cap * factor))]));
  return { caps: scaled, overBudget: sum(scaled.values()) > budget };
}

export type CapacityStatus = 'OK' | 'ALERTA' | 'INVIABLE';

/** ρ_p = N_p × L_p / (86.400 × c_p) (docs/ARQUITECTURA.md §6). */
export function utilization(records: number, occupancyS: number, lanes = 1): number {
  return (records * occupancyS) / (DAY_S * Math.max(lanes, 1));
}

export function capacityStatus(rho: number): CapacityStatus {
  if (rho > 1) return 'INVIABLE';
  if (rho > 0.7) return 'ALERTA';
  return 'OK';
}

function sum(values: Iterable<number>): number {
  let total = 0;
  for (const v of values) total += v;
  return total;
}
