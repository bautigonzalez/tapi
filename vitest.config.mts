import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['tests/**/*.test.ts'],
    // Los de integración necesitan los emuladores: se corren con `npm run test:integration`.
    exclude: ['tests/integration/**', 'node_modules/**'],
    setupFiles: ['tests/setup.ts'],
    environment: 'node',
  },
});
