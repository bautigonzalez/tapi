import { vi } from 'vitest';

// Los handlers escriben logs y métricas EMF por consola; en los tests solo hacen ruido.
vi.spyOn(console, 'log').mockImplementation(() => undefined);
vi.spyOn(console, 'error').mockImplementation(() => undefined);
