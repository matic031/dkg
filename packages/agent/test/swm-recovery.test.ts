import { afterEach, describe, expect, it } from 'vitest';
import { OxigraphStore } from '@origintrail-official/dkg-storage';
import {
  contextGraphWorkspaceGraphUri,
  contextGraphWorkspaceMetaGraphUri,
  type OperationContext,
} from '@origintrail-official/dkg-core';
import type { Quad } from '@origintrail-official/dkg-storage';
import type { SyncPageResult } from '../src/sync/requester/page-fetch.js';
import { recoverContextGraphSwm } from '../src/sync/requester/swm-recovery.js';

/**
 * integration. `recoverContextGraphSwm` fetches a CG's
 * full current state from a peer and applies it via REPLACE (not the shared
 * incremental path's blind union), so a stale local store converges to the
 * source's value rather than accumulating a corrupt `{v1,v2}` superset.
 * Transport + verifier are mocked (no libp2p); the apply hits a real store.
 */
const CG = 'ws00-recovery';
const WS = contextGraphWorkspaceGraphUri(CG);
const WS_META = contextGraphWorkspaceMetaGraphUri(CG);
const SUBJ = 'urn:ws00r:shipment';
const STATUS = 'http://schema.org/status';
const ctx: OperationContext = { operationId: 'test', operationName: 'sync' } as never;

function page(quads: Quad[], completed = true): SyncPageResult {
  return { quads, bytesReceived: 0, resumedFromOffset: 0, nextOffset: quads.length, checkpointKey: 'k', completed, timedOut: !completed };
}
async function statusValues(store: OxigraphStore): Promise<string[]> {
  const r = await store.query(`SELECT ?o WHERE { GRAPH <${WS}> { <${SUBJ}> <${STATUS}> ?o } }`);
  return r.type === 'bindings' ? r.bindings.map((b) => b['o']) : [];
}

describe('recoverContextGraphSwm (fetch → verify → replace)', () => {
  const stores: OxigraphStore[] = [];
  afterEach(async () => { await Promise.all(stores.splice(0).map((s) => s.close().catch(() => {}))); });

  function makeDeps(store: OxigraphStore, sourceData: Quad[], sourceMeta: Quad[] = []) {
    return {
      ctx,
      remotePeerId: 'peer-source',
      contextGraphId: CG,
      deadline: Number.MAX_SAFE_INTEGER,
      fetchSyncPages: async (
        _c: OperationContext, _p: string, _cg: string, _inc: boolean,
        phase: 'data' | 'meta',
      ): Promise<SyncPageResult> => page(phase === 'data' ? sourceData : sourceMeta),
      processSharedMemoryBatch: async (dataQuads: Quad[], metaQuads: Quad[]) => ({
        verifiedData: dataQuads,
        verifiedMeta: metaQuads,
        entityCreators: [...new Set(dataQuads.map((q) => q.subject))].map((entity) => ({
          dataGraph: WS, entity, creator: 'peer-source',
        })),
        droppedDataTriples: 0,
      }),
      store,
      ensureContextGraph: async () => {},
      setCheckpoint: () => {},
      deleteCheckpoint: () => {},
    };
  }

  it('replaces a stale local value with the source value (no union corruption)', async () => {
    const store = new OxigraphStore();
    stores.push(store);
    await store.insert([{ subject: SUBJ, predicate: STATUS, object: '"v1"', graph: WS }]);

    const result = await recoverContextGraphSwm(makeDeps(store, [
      { subject: SUBJ, predicate: STATUS, object: '"v2"', graph: WS },
    ]));

    expect(result.completed).toBe(true);
    expect(result.replacedRoots).toBe(1);
    expect(await statusValues(store)).toEqual(['"v2"']); // ONLY v2 — the bug would leave {v1,v2}
  });

  it('is a clean recovery into an empty store (cold-start parity)', async () => {
    const store = new OxigraphStore();
    stores.push(store);
    const result = await recoverContextGraphSwm(makeDeps(store, [
      { subject: SUBJ, predicate: STATUS, object: '"v2"', graph: WS },
    ]));
    expect(result.insertedDataQuads).toBe(1);
    expect(await statusValues(store)).toEqual(['"v2"']);
  });

  it('inserts verified meta and reports it', async () => {
    const store = new OxigraphStore();
    stores.push(store);
    const meta: Quad[] = [{ subject: 'urn:op:1', predicate: 'http://dkg.io/ontology/shareOperationId', object: '"op1"', graph: WS_META }];
    const result = await recoverContextGraphSwm(makeDeps(store, [
      { subject: SUBJ, predicate: STATUS, object: '"v2"', graph: WS },
    ], meta));
    expect(result.insertedMetaQuads).toBe(1);
    const r = await store.query(`SELECT ?s WHERE { GRAPH <${WS_META}> { ?s ?p ?o } }`);
    expect(r.type === 'bindings' && r.bindings.length).toBe(1);
  });

  it('does NOT apply when a phase never completes — leaves the store untouched for a clean retry', async () => {
    // Row-based pagination can cut a root mid-stream. Applying a REPLACE over an
    // incomplete fetch would clear the root and reinsert only the fetched prefix,
    // truncating the entity until a later retry. So an incomplete fetch must mutate
    // NOTHING: the pre-existing state stays intact and the caller retries from scratch.
    const store = new OxigraphStore();
    stores.push(store);
    await store.insert([{ subject: SUBJ, predicate: STATUS, object: '"v1"', graph: WS }]);
    const deps = makeDeps(store, [{ subject: SUBJ, predicate: STATUS, object: '"v2"', graph: WS }]);
    // data phase never completes and makes no progress → loop stops, partial.
    const partialDeps = {
      ...deps,
      fetchSyncPages: async (
        _c: OperationContext, _p: string, _cg: string, _inc: boolean, phase: 'data' | 'meta',
      ): Promise<SyncPageResult> =>
        phase === 'data'
          ? { ...page([{ subject: SUBJ, predicate: STATUS, object: '"v2"', graph: WS }], false), nextOffset: 0, resumedFromOffset: 0 }
          : page([]),
    };
    const result = await recoverContextGraphSwm(partialDeps);
    expect(result.completed).toBe(false);
    expect(result.replacedRoots).toBe(0);
    // incomplete fetch → no mutation at all; the prior v1 is untouched (no truncation, no partial replace)
    expect(await statusValues(store)).toEqual(['"v1"']);
  });

  it('retains completed metadata and resumes data across recovery rounds', async () => {
    const store = new OxigraphStore();
    stores.push(store);
    await store.insert([{ subject: SUBJ, predicate: STATUS, object: '"v1"', graph: WS }]);

    const sourceData: Quad[] = [
      { subject: SUBJ, predicate: STATUS, object: '"v2"', graph: WS },
      { subject: SUBJ, predicate: 'http://schema.org/name', object: '"shipment"', graph: WS },
    ];
    const sourceMeta: Quad[] = [
      { subject: 'urn:op:resume', predicate: 'http://dkg.io/ontology/shareOperationId', object: '"resume"', graph: WS_META },
    ];
    const checkpoints = new Map<string, number>();
    let metaFetches = 0;
    let dataFetches = 0;
    const deps = {
      ...makeDeps(store, sourceData, sourceMeta),
      fetchSyncPages: async (
        _c: OperationContext, _p: string, _cg: string, _inc: boolean, phase: 'data' | 'meta',
      ): Promise<SyncPageResult> => {
        if (phase === 'meta') {
          metaFetches += 1;
          return {
            ...page(sourceMeta),
            checkpointKey: 'meta-k',
            timedOut: false,
          };
        }
        dataFetches += 1;
        if (dataFetches === 1) {
          return {
            ...page([sourceData[0]], false),
            resumedFromOffset: 0,
            nextOffset: 1,
            checkpointKey: 'data-k',
            timedOut: true,
          };
        }
        return {
          ...page([sourceData[1]]),
          resumedFromOffset: checkpoints.get('data-k') ?? 0,
          nextOffset: 2,
          checkpointKey: 'data-k',
          timedOut: false,
        };
      },
      setCheckpoint: (key: string, offset: number) => { checkpoints.set(key, offset); },
      deleteCheckpoint: (key: string) => { checkpoints.delete(key); },
    };

    const first = await recoverContextGraphSwm(deps);
    expect(first.completed).toBe(false);
    expect(await statusValues(store)).toEqual(['"v1"']);
    expect(checkpoints.get('data-k')).toBe(1);

    const second = await recoverContextGraphSwm(deps);
    expect(second.completed).toBe(true);
    expect(metaFetches).toBe(1);
    expect(dataFetches).toBe(2);
    expect(second.insertedDataQuads).toBe(2);
    expect(await statusValues(store)).toEqual(['"v2"']);
    expect(checkpoints.has('data-k')).toBe(false);
  });

  it('retains recovery staging when production recreates the store hook wrapper', async () => {
    const store = new OxigraphStore();
    stores.push(store);
    const sourceData: Quad[] = [
      { subject: SUBJ, predicate: STATUS, object: '"v2"', graph: WS },
      { subject: SUBJ, predicate: 'http://schema.org/name', object: '"shipment"', graph: WS },
    ];
    const checkpoints = new Map<string, number>();
    let dataFetches = 0;
    const base = {
      ...makeDeps(store, sourceData),
      recoveryStateScope: store,
      fetchSyncPages: async (
        _c: OperationContext, _p: string, _cg: string, _inc: boolean, phase: 'data' | 'meta',
      ): Promise<SyncPageResult> => {
        if (phase === 'meta') return { ...page([]), checkpointKey: 'meta-wrapper-k' };
        dataFetches += 1;
        return dataFetches === 1
          ? {
              ...page([sourceData[0]], false),
              checkpointKey: 'data-wrapper-k',
              resumedFromOffset: 0,
              nextOffset: 1,
            }
          : {
              ...page([sourceData[1]]),
              checkpointKey: 'data-wrapper-k',
              resumedFromOffset: checkpoints.get('data-wrapper-k') ?? 0,
              nextOffset: 2,
            };
      },
      setCheckpoint: (key: string, offset: number) => { checkpoints.set(key, offset); },
      deleteCheckpoint: (key: string) => { checkpoints.delete(key); },
    };
    const wrapper = () => ({
      insert: (quads: Quad[]) => store.insert(quads),
      deleteByPattern: (pattern: { graph: string; subject: string }) => store.deleteByPattern(pattern),
      deleteBySubjectPrefix: (graph: string, prefix: string) => store.deleteBySubjectPrefix(graph, prefix),
    });

    const first = await recoverContextGraphSwm({ ...base, store: wrapper() });
    expect(first.completed).toBe(false);
    const second = await recoverContextGraphSwm({ ...base, store: wrapper() });

    expect(second.completed).toBe(true);
    expect(second.insertedDataQuads).toBe(2);
  });
});
