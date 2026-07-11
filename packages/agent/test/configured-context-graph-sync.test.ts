import { afterEach, describe, expect, it, vi } from 'vitest';
import { ethers } from 'ethers';
import { MockChainAdapter } from '@origintrail-official/dkg-chain';
import {
  DKG_ONTOLOGY,
  SYSTEM_CONTEXT_GRAPHS,
  contextGraphDataGraphUri,
  contextGraphMetaGraphUri,
} from '@origintrail-official/dkg-core';
import { DKGAgent, agentFromPrivateKey, type AgentKeyRecord } from '../src/index.js';

interface AgentInternals {
  localAgents: Map<string, AgentKeyRecord>;
  defaultAgentAddress?: string;
  subscribedContextGraphs: Map<string, Record<string, unknown>>;
  isPrivateContextGraph(contextGraphId: string): Promise<boolean>;
  buildSyncRequest(...args: unknown[]): Promise<Uint8Array>;
}

const agents: DKGAgent[] = [];

async function createAgent(config: Record<string, unknown> = {}): Promise<{
  agent: DKGAgent;
  internals: AgentInternals;
}> {
  const agent = await DKGAgent.create({
    name: `ConfiguredGraphSync-${Math.random().toString(36).slice(2)}`,
    chainAdapter: new MockChainAdapter(),
    ...config,
  });
  Object.defineProperty(agent, 'peerId', {
    value: '12D3KooWConfiguredGraphSyncRequester',
    configurable: true,
  });
  agents.push(agent);
  return { agent, internals: agent as unknown as AgentInternals };
}

afterEach(async () => {
  await Promise.all(agents.splice(0).map((agent) => agent.stop().catch(() => {})));
});

describe('configured context-graph recovery', () => {
  it('authenticates a pending-meta request even when stale local state says synced and public', async () => {
    const contextGraphId = 'umanitek/blackbox-threats-staging';
    const { internals } = await createAgent();
    const local = agentFromPrivateKey(ethers.Wallet.createRandom().privateKey, 'blackbox-agent');
    internals.localAgents.set(local.agentAddress, local);
    internals.defaultAgentAddress = local.agentAddress;
    internals.subscribedContextGraphs.set(contextGraphId, {
      name: contextGraphId,
      subscribed: true,
      synced: true,
      pendingMeta: true,
      metaSynced: false,
    });
    internals.isPrivateContextGraph = vi.fn(async () => false);

    const encoded = await internals.buildSyncRequest(
      contextGraphId,
      0,
      50,
      false,
      '12D3KooWRemoteCurator',
      'meta',
    );
    const request = JSON.parse(new TextDecoder().decode(encoded));

    expect(request.requesterIdentityId).toBe('0');
    expect(request.requesterAgentAddress).toBe(local.agentAddress);
    expect(request.requesterSignatureR).toBeTruthy();
    expect(request.requesterSignatureVS).toBeTruthy();
  });

  it('does not treat a registration-only placeholder as confirmed curator metadata', async () => {
    const contextGraphId = 'registration-only-placeholder';
    const { agent, internals } = await createAgent();
    const contextGraphUri = contextGraphDataGraphUri(contextGraphId);
    const metaGraph = contextGraphMetaGraphUri(contextGraphId);
    const ontologyGraph = contextGraphDataGraphUri(SYSTEM_CONTEXT_GRAPHS.ONTOLOGY);
    internals.subscribedContextGraphs.set(contextGraphId, {
      name: contextGraphId,
      subscribed: true,
      synced: true,
      pendingMeta: true,
      metaSynced: false,
    });
    await agent.store.insert([
      {
        subject: contextGraphUri,
        predicate: DKG_ONTOLOGY.RDF_TYPE,
        object: DKG_ONTOLOGY.DKG_CONTEXT_GRAPH,
        graph: ontologyGraph,
      },
      {
        subject: contextGraphUri,
        predicate: DKG_ONTOLOGY.DKG_REGISTRATION_STATUS,
        object: '"unregistered"',
        graph: metaGraph,
      },
    ]);

    await expect(agent.hasConfirmedMetaState(contextGraphId)).resolves.toBe(false);

    await agent.store.insert([{
      subject: contextGraphUri,
      predicate: DKG_ONTOLOGY.DKG_ACCESS_POLICY,
      object: '"private"',
      graph: metaGraph,
    }]);
    await expect(agent.hasConfirmedMetaState(contextGraphId)).resolves.toBe(true);
  });

  it('re-confirms configured persisted graphs without suppressing unrelated subscriptions', async () => {
    const rows = [
      {
        id: 'configured', name: 'configured', subscribed: true,
        synced: true, metaSynced: true, sharedMemorySynced: false, syncScoped: true,
      },
      {
        id: 'unrelated', name: 'unrelated', subscribed: true,
        synced: true, metaSynced: true, sharedMemorySynced: false, syncScoped: true,
      },
    ];
    const { agent } = await createAgent({
      syncContextGraphs: ['configured'],
      contextGraphSubscriptionStore: {
        loadAll: async () => rows,
        save: async () => {},
        delete: async () => {},
      },
    });

    await agent.start();

    expect(agent.getSubscribedContextGraphs().get('configured')).toMatchObject({
      synced: true,
      metaSynced: false,
      pendingMeta: true,
    });
    expect(agent.getSubscribedContextGraphs().get('unrelated')).toMatchObject({
      synced: true,
      metaSynced: true,
    });
  });
});
