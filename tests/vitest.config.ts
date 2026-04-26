import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['integration/**/*.test.ts'],
    testTimeout: 60_000,
    hookTimeout: 60_000,
    // Sequence required: tests share the local docker-compose state.
    pool: 'forks',
    poolOptions: { forks: { singleFork: true } },
  },
});
