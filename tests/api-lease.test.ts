import { describe, expect, it } from 'vitest';
import { withProviderLease, type LeaseStore } from '../src/api-lease/provider-lease';
import { TIMEOUTS } from '../src/config';
import { countOverlaps, type CallInterval } from '../src/domain/overlaps';

/** LeaseStore en memoria con la misma condición que la escritura condicional de DynamoDB. */
class MemoryLeaseStore implements LeaseStore {
  lease?: { holder: string; until: number };
  calls = new Map<string, CallInterval>();

  async acquire(_p: string, holder: string, ttlMs: number, nowMs: number) {
    if (this.lease && this.lease.until >= nowMs) {
      return { ok: false as const, retryAfterS: Math.max(1, Math.ceil((this.lease.until - nowMs) / 1_000)) };
    }
    this.lease = { holder, until: nowMs + ttlMs };
    return { ok: true as const };
  }
  async release(_p: string, holder: string) {
    if (this.lease?.holder === holder) this.lease = undefined;
  }
  async logStart(_p: string, holder: string, startMs: number) {
    this.calls.set(holder, { start: startMs });
  }
  async logEnd(_p: string, holder: string, _startMs: number, endMs: number) {
    const call = this.calls.get(holder);
    if (call) call.end = endMs;
  }
}

const TTL = TIMEOUTS.lambda2MaxMs + 5_000;

describe('lease por proveedor en λ2', () => {
  it('mientras una llamada está en curso, otra recibe retryAfter (423)', async () => {
    let now = 0;
    const store = new MemoryLeaseStore();
    let second: Awaited<ReturnType<typeof withProviderLease>> | undefined;
    const first = await withProviderLease(store, 'edenor', { ttlMs: TTL, now: () => now }, async () => {
      now += 5_000;
      second = await withProviderLease(store, 'edenor', { ttlMs: TTL, now: () => now }, async () => ({ value: 'b', providerTimedOut: false, outcome: 'ok' }));
      now += 5_000;
      return { value: 'a', providerTimedOut: false, outcome: 'ok' };
    });
    expect(first).toEqual({ acquired: true, value: 'a' });
    expect(second).toEqual({ acquired: false, retryAfterS: 24 });
    expect(store.lease).toBeUndefined();
  });

  it('si la llamada al proveedor terminó por timeout, el lease no se libera: vence solo', async () => {
    let now = 0;
    const store = new MemoryLeaseStore();
    await withProviderLease(store, 'edenor', { ttlMs: TTL, now: () => now }, async () => {
      now += 20_000;
      return { value: undefined, providerTimedOut: true, outcome: 'timeout' };
    });
    expect(store.lease?.until).toBe(TTL);
    now = TTL - 1;
    expect((await withProviderLease(store, 'edenor', { ttlMs: TTL, now: () => now }, async () => ({ value: 1, providerTimedOut: false, outcome: 'ok' }))).acquired).toBe(false);
    now = TTL + 1;
    expect((await withProviderLease(store, 'edenor', { ttlMs: TTL, now: () => now }, async () => ({ value: 1, providerTimedOut: false, outcome: 'ok' }))).acquired).toBe(true);
  });

  it('caso zombie: el worker corta a los 30 s, reintenta y nunca hay dos llamadas al proveedor', async () => {
    let now = 0;
    const store = new MemoryLeaseStore();
    let finishProvider: () => void = () => undefined;
    const providerDone = new Promise<void>((resolve) => {
      finishProvider = resolve;
    });

    // λ1 invoca a λ2 tarde (t = 26 s). λ2 sigue llamando al proveedor después de que el worker cortó a los 30 s.
    now = 26_000;
    const original = withProviderLease(store, 'edenor', { ttlMs: TTL, now: () => now }, async () => {
      await providerDone;
      return { value: 'tarde', providerTimedOut: false, outcome: 'ok' };
    });
    await new Promise((resolve) => setImmediate(resolve));

    // El worker reintenta tras el backoff (t = 42 s), con λ2 todavía viva: recibe 423.
    now = 42_000;
    const retryWhileAlive = await withProviderLease(store, 'edenor', { ttlMs: TTL, now: () => now }, async () => ({ value: 'x', providerTimedOut: false, outcome: 'ok' }));
    expect(retryWhileAlive).toEqual({ acquired: false, retryAfterS: 13 });

    // λ2 termina dentro de su vida máxima (t = 48 s) y libera el lease.
    now = 48_000;
    finishProvider();
    await original;
    now = 50_000;
    const retryAfter = await withProviderLease(store, 'edenor', { ttlMs: TTL, now: () => now }, async () => {
      now = 46_000;
      return { value: 'ok', providerTimedOut: false, outcome: 'ok' };
    });
    expect(retryAfter.acquired).toBe(true);
    expect(countOverlaps([...store.calls.values()], TIMEOUTS.lambda2MaxMs)).toBe(0);
  });

  it('si falla el registro del fin, no se pierde el resultado (el SLI imputa la vida máxima)', async () => {
    const store = new MemoryLeaseStore();
    store.logEnd = async () => {
      throw new Error('throttling');
    };
    const errors: unknown[] = [];
    const res = await withProviderLease(store, 'edenor', { ttlMs: TTL, now: () => 0, onLogError: (e) => errors.push(e) }, async () => ({ value: 'ok', providerTimedOut: false, outcome: 'ok' }));
    expect(res).toEqual({ acquired: true, value: 'ok' });
    expect(errors).toHaveLength(1);
  });

  it('el lease nunca dura menos que la vida restante de λ2', async () => {
    const store = new MemoryLeaseStore();
    await withProviderLease(store, 'edenor', { ttlMs: 1_000, now: () => 0, remainingMs: 20_000 }, async () => ({ value: 1, providerTimedOut: true, outcome: 'timeout' }));
    expect(store.lease?.until).toBe(25_000);
  });

  it('si la llamada lanza una excepción, el lease queda tomado hasta vencer (falla cerrada)', async () => {
    const store = new MemoryLeaseStore();
    await expect(
      withProviderLease(store, 'edenor', { ttlMs: TTL, now: () => 0 }, async () => {
        throw new Error('boom');
      }),
    ).rejects.toThrow('boom');
    expect(store.lease).toBeDefined();
  });
});
