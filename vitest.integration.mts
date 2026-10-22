import { defineConfig } from 'vitest/config';

// Tests de integración: corren contra los emuladores locales (npm run local:up && npm run local:setup).
export default defineConfig({
  test: {
    include: ['tests/integration/**/*.test.ts'],
    setupFiles: ['tests/setup.ts'],
    environment: 'node',
    testTimeout: 30_000,
    fileParallelism: false,
  },
});
