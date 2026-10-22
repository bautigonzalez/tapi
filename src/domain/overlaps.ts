export interface CallInterval {
  start: number;
  /** Ausente si λ2 murió antes de registrar el fin. */
  end?: number;
}

/**
 * SLI de exclusión: cuenta las llamadas que empiezan antes de que termine alguna anterior del mismo
 * proveedor. Si una llamada no registró su fin, se imputa inicio + vida máxima de λ2, para que el
 * caso zombie también cuente (docs/ARQUITECTURA.md §4, capa 5).
 */
export function countOverlaps(calls: readonly CallInterval[], maxDurationMs: number): number {
  const sorted = [...calls].sort((a, b) => a.start - b.start);
  let maxEnd = Number.NEGATIVE_INFINITY;
  let overlaps = 0;
  for (const call of sorted) {
    if (call.start < maxEnd) overlaps++;
    maxEnd = Math.max(maxEnd, call.end ?? call.start + maxDurationMs);
  }
  return overlaps;
}
