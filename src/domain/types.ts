export type TerminalStatus = 'SUCCEEDED' | 'FAILED_PERMANENT' | 'FAILED_EXHAUSTED' | 'SUPERSEDED';
export type ExecutionStatus = 'ENQUEUED' | 'IN_PROGRESS' | 'WAITING' | TerminalStatus;
export type WaitingReason = 'record' | 'unknown' | 'provider' | 'api';

/**
 * Mensaje de la cola. Lleva el body para que todos los reintentos usen exactamente los mismos
 * parámetros que la Idempotency-Key (docs/ARQUITECTURA.md §3.1).
 */
export interface Job {
  schemaVersion: 1;
  execId: string;
  recordId: string;
  proveedor: string;
  endpoint: string;
  body: unknown;
  /** Día UTC de la corrida, YYYY-MM-DD. */
  runDate: string;
  /** Minuto del día asignado al registro, 0..1439. */
  slot: number;
}

export type Outcome =
  | { kind: 'success'; httpStatus: number; body: string }
  | { kind: 'permanent'; httpStatus?: number; code: string }
  | {
      kind: 'retryable';
      reason: WaitingReason;
      code: string;
      httpStatus?: number;
      retryAfterS?: number;
      /** 401/403 del proveedor: se deshabilita hasta que alguien revise sus credenciales. */
      disableProvider?: boolean;
    };

/** Posición del Dispatcher dentro de los registros de un proveedor. */
export interface Cursor {
  date: string;
  slot: number;
  id: string;
}

export interface ProviderState {
  proveedor: string;
  enabled: boolean;
  /** Carriles concurrentes que acepta el proveedor (c_p). El MVP usa 1. */
  lanes: number;
  /** Tiempo de ocupación del grupo por mensaje completado, medido por el Verificador. */
  occupancyS: number;
  /** Prefijos de ruta permitidos para el endpoint (protección contra SSRF). */
  allowlist: string[];
  pausedUntil: number;
  backoffLevel: number;
  cursor?: Cursor;
}

export interface WindowStats {
  calls: number;
  fails: number;
  /** Registros distintos con fallas en la ventana (acotado). */
  failRecords: number;
  permanents: number;
  /** Fallas de la API misma: registros y proveedores distintos afectados (solo en GLOBAL). */
  apiFailRecords: number;
  apiFailProviders: number;
}

export const EMPTY_STATS: WindowStats = { calls: 0, fails: 0, failRecords: 0, permanents: 0, apiFailRecords: 0, apiFailProviders: 0 };
