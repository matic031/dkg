import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

beforeEach(() => {
  vi.unstubAllEnvs();
  vi.resetModules();
});

afterEach(() => {
  vi.unstubAllEnvs();
  vi.resetModules();
});

describe('sync tuning environment overrides', () => {
  it('retains upstream defaults when no override is configured', async () => {
    vi.stubEnv('DKG_SYNC_TOTAL_TIMEOUT_MS', '');
    vi.stubEnv('DKG_SYNC_PAGE_TIMEOUT_MS', '');
    vi.stubEnv('DKG_SYNC_MIN_GRAPH_BUDGET_MS', '');
    const constants = await import('../src/dkg-agent-constants.js');

    expect(constants.SYNC_TOTAL_TIMEOUT_MS).toBe(120_000);
    expect(constants.SYNC_PAGE_TIMEOUT_MS).toBe(45_000);
    expect(constants.SYNC_MIN_GRAPH_BUDGET_MS).toBe(10_000);
  });

  it('accepts the values needed for the reproduced large-SWM recovery', async () => {
    vi.stubEnv('DKG_SYNC_TOTAL_TIMEOUT_MS', '1200000');
    vi.stubEnv('DKG_SYNC_PAGE_TIMEOUT_MS', '180000');
    vi.stubEnv('DKG_SYNC_MIN_GRAPH_BUDGET_MS', '120000');
    const constants = await import('../src/dkg-agent-constants.js');

    expect(constants.SYNC_TOTAL_TIMEOUT_MS).toBe(1_200_000);
    expect(constants.SYNC_PAGE_TIMEOUT_MS).toBe(180_000);
    expect(constants.SYNC_MIN_GRAPH_BUDGET_MS).toBe(120_000);
  });
});
