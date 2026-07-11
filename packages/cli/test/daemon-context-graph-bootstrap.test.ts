import { describe, expect, it, vi } from 'vitest';
import {
  initializeStartupContextGraphs,
  type StartupContextGraphAgent,
} from '../src/daemon/lifecycle.js';

function createAgent(hasConfirmedMeta = false) {
  const ensureContextGraphLocal = vi.fn(async () => {});
  const subscribeToContextGraph = vi.fn();
  const markContextGraphSubscriptionState = vi.fn();
  const hasConfirmedMetaState = vi.fn(async () => hasConfirmedMeta);
  const agent: StartupContextGraphAgent = {
    ensureContextGraphLocal,
    subscribeToContextGraph,
    markContextGraphSubscriptionState,
    hasConfirmedMetaState,
  };
  return {
    agent,
    ensureContextGraphLocal,
    subscribeToContextGraph,
    markContextGraphSubscriptionState,
  };
}

describe('daemon configured context-graph bootstrap', () => {
  it('subscribes configured graphs without creating a false-public placeholder', async () => {
    const harness = createAgent(false);

    await initializeStartupContextGraphs({
      agent: harness.agent,
      syncContextGraphs: ['umanitek/blackbox-threats-staging', 'network-default'],
      configuredContextGraphs: ['umanitek/blackbox-threats-staging'],
      log: vi.fn(),
    });

    expect(harness.ensureContextGraphLocal).toHaveBeenCalledTimes(1);
    expect(harness.ensureContextGraphLocal).toHaveBeenCalledWith({
      id: 'network-default',
      name: 'network-default',
      description: 'Default context graph: network-default',
    });
    expect(harness.subscribeToContextGraph).toHaveBeenCalledWith(
      'umanitek/blackbox-threats-staging',
      { deferSharedMemoryGossipSubscribe: true },
    );
    expect(harness.markContextGraphSubscriptionState).toHaveBeenCalledWith(
      'umanitek/blackbox-threats-staging',
      { name: 'umanitek/blackbox-threats-staging', pendingMeta: true, metaSynced: false },
    );
  });

  it('enables SWM gossip immediately when real metadata already exists', async () => {
    const harness = createAgent(true);

    await initializeStartupContextGraphs({
      agent: harness.agent,
      syncContextGraphs: ['configured'],
      configuredContextGraphs: ['configured'],
      log: vi.fn(),
    });

    expect(harness.subscribeToContextGraph).toHaveBeenNthCalledWith(
      1,
      'configured',
      { deferSharedMemoryGossipSubscribe: true },
    );
    expect(harness.subscribeToContextGraph).toHaveBeenNthCalledWith(2, 'configured');
    expect(harness.markContextGraphSubscriptionState).toHaveBeenLastCalledWith(
      'configured',
      { name: 'configured', pendingMeta: false, metaSynced: true },
    );
  });
});
