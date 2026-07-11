import { describe, expect, it } from 'vitest';
import type { TripleStore } from '@origintrail-official/dkg-storage';
import { contextGraphDataGraphUri } from '@origintrail-official/dkg-core';
import {
  createResponderSyncRowListMemo,
  readSwmMetaPage,
} from '../src/sync/responder/graph-plan.js';

describe('sync responder SWM metadata snapshot query', () => {
  it('reads each named graph explicitly instead of using the stalling variable-graph join', async () => {
    const contextGraphId = 'umanitek/blackbox-threats-staging';
    const contextGraphUri = contextGraphDataGraphUri(contextGraphId);
    const baseGraph = `${contextGraphUri}/_shared_memory_meta`;
    const childGraph = `${contextGraphUri}/alerts/_shared_memory_meta`;
    const queries: string[] = [];
    const store = {
      async query(sparql: string) {
        queries.push(sparql);
        if (sparql.includes(`GRAPH <${baseGraph}>`)) {
          return {
            type: 'bindings' as const,
            bindings: [{ s: 'urn:z', p: 'http://schema.org/name', o: '"base"' }],
          };
        }
        if (sparql.includes(`GRAPH <${childGraph}>`)) {
          return {
            type: 'bindings' as const,
            bindings: [{ s: 'urn:a', p: 'http://schema.org/name', o: '"child"' }],
          };
        }
        throw new Error(`Unexpected query: ${sparql}`);
      },
    } as unknown as TripleStore;

    const rows = await readSwmMetaPage({
      store,
      graphList: [childGraph, baseGraph],
      registeredSubGraphNames: ['alerts'],
      contextGraphId,
      cutoffIso: '2026-01-01T00:00:00.000Z',
      offset: 0,
      limit: 10,
      rowListMemo: createResponderSyncRowListMemo(60_000, 4),
      rowListCacheKey: `${contextGraphId}:swm-meta`,
      refreshRowList: true,
    });

    expect(queries).toHaveLength(2);
    expect(queries.every((query) => query.includes('FILTER EXISTS'))).toBe(true);
    expect(queries.every((query) => !query.includes('VALUES ?g') && !query.includes('GRAPH ?g'))).toBe(true);
    expect(rows).toEqual([
      { g: baseGraph, s: 'urn:z', p: 'http://schema.org/name', o: '"base"' },
      { g: childGraph, s: 'urn:a', p: 'http://schema.org/name', o: '"child"' },
    ]);
  });
});
