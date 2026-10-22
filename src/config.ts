/** Parámetros del sistema. Cada valor está justificado en docs/ARQUITECTURA.md (§3 a §6). */

export const DAY_S = 86_400;
export const SLOTS_PER_DAY = 1_440;

export const TIMEOUTS = {
  /** Timeout HTTP del worker hacia la API: mayor que T_APIGW (29 s). */
  httpMs: 30_000,
  /** Cuarentena tras un resultado desconocido si la API no tiene lease: la cadena λ1→λ2 vive como mucho 72 s. */
  quarantineS: 90,
  /** Lease del claim: mayor que el peor caso de la cadena y menor que el VisibilityTimeout de la cola (240 s). */
  claimLeaseS: 120,
  /** Vida máxima de λ2: es el fin que se imputa a una llamada que nunca registró su fin. */
  lambda2MaxMs: 24_000,
} as const;

export const RETRY = {
  maxAttempts: 4,
  recordBaseS: 15,
  recordCapS: 60,
  providerBaseS: 5,
  providerCapS: 900,
  maxBackoffLevel: 20,
  /** Desde esta recepción del mismo mensaje, la retención mínima sube a receiveFloorS (≤ 820 recepciones en 24 h). */
  receiveFloorAfter: 100,
  receiveFloorS: 120,
  /** Ninguna retención supera 15 min: lejos del tope de 12 h de visibilidad de SQS. */
  maxHoldS: 900,
  /** Pausa global cuando falla la API misma (no un proveedor ni un registro). */
  apiPauseS: 60,
} as const;

export const ADMISSION = {
  /** Trabajo máximo por proveedor en cola, medido en segundos de ocupación del grupo. */
  workS: 900,
  minCap: 5,
  maxCap: 2_000,
  /** Cota global por cola: la mitad de la ventana de 120.000 mensajes que revisa SQS FIFO. */
  queueBudget: 60_000,
  /** Tiempo de ocupación supuesto hasta que el Verificador mida el real. */
  defaultOccupancyS: 1,
} as const;

export const BREAKER = {
  windowMs: 10 * 60_000,
  minCalls: 20,
  failRate: 0.5,
  minFailRecords: 10,
  minCallsGlobal: 50,
  permanentRate: 0.2,
  /** La API se da por caída solo si falla para varios registros de al menos dos proveedores. */
  minApiFailRecords: 5,
  minApiFailProviders: 2,
  /** Tope de ids distintos guardados por ventana: alcanza para decidir y acota el ítem. */
  maxTrackedRecords: 100,
} as const;

/** Bodies más grandes no se despachan (filtro en SQL): se cuentan como registros inválidos. */
export const BODY_MAX_BYTES = 256 * 1_024;
export const ID_MAX_CHARS = 256;
export const ENDPOINT_MAX_CHARS = 2_048;
/** SendMessageBatch admite 1 MiB por lote; se deja margen para atributos. */
export const SQS_BATCH_MAX_BYTES = 1_000_000;

/** Una corrida con más de 24 h de atraso se descarta: ya vence el turno del día siguiente. */
export const SUPERSEDE_AFTER_MS = DAY_S * 1_000;
/** El barrido horario cierra lo que siga abierto pasadas 25 h (por ejemplo, mensajes que cayeron a la DLQ). */
export const SWEEP_AFTER_MS = 25 * 3_600 * 1_000;

/** Respuestas más grandes van a S3: es el punto de equilibrio entre WRU de DynamoDB y PUT de S3. */
export const INLINE_RESULT_MAX_BYTES = 8 * 1_024;
/** Respuestas más grandes se rechazan para proteger la memoria del worker. */
export const RESPONSE_MAX_BYTES = 5 * 1_024 * 1_024;
/** SQS admite 1 MiB por mensaje; se deja margen para los atributos. */
export const JOB_MAX_BYTES = 900 * 1_024;
export const RESULT_TTL_DAYS = 90;

export function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Falta la variable de entorno ${name}`);
  return value;
}
