/**
 * Auto-wipe per-node chain-state derived files when the maintainer-set
 * `network/<env>.json#chainResetMarker` differs from the one persisted on
 * the previous boot.
 *
 * Why this exists
 * ---------------
 * Testnet resets (e.g. PR #357 V10 staking consolidation) require every
 * operator to wipe their oxigraph store, publish journal, and random
 * sampling WAL because those files reference chain entities (KC ids,
 * merkle roots, challenge periods) that no longer exist after the chain
 * is redeployed. Without this auto-wipe, every operator has to do it by
 * hand — see docs/TESTNET_RESET.md Phase C for the manual drill.
 *
 * With this hook, the maintainer simply bumps
 * `network/testnet.json#chainResetMarker` to a fresh value as part of the
 * reset commit. Each operator's daemon picks up the new commit via
 * auto-update (5 min on testnet), sees the marker change on next boot,
 * wipes the affected files, and continues. Operator does nothing.
 *
 * Why not reuse `networkId`?
 * --------------------------
 * `networkId` is a SHA256 of the bundled genesis TriG (see
 * `core/src/genesis.ts:computeNetworkId`). It only changes when the
 * genesis document itself is edited — that's a much rarer event than a
 * chain redeploy. Using it as the chain-reset signal would either never
 * trigger (genesis not bumped) or trip the FATAL genesis-mismatch guard
 * (genesis bumped but state out of sync). Hence a dedicated marker.
 *
 * Safety properties
 * -----------------
 * - No marker in network config → hook is a no-op (back-compat for
 *   networks that haven't opted in).
 * - First boot with marker present, no persisted state → wipe, save.
 *   Rationale: the only way to reach this branch on an existing install
 *   is "operator was running before this hook landed, now upgraded into
 *   a release with a marker present". That release necessarily ships in
 *   the chain-reset window, so wiping is the correct behaviour. Fresh
 *   installs hit this branch too but have nothing to wipe → no harm.
 * - Persisted == current → no wipe, idempotent.
 * - Persisted != current → wipe + save new marker.
 *
 * Files wiped: `store.nq`, `store.nq.tmp`, `random-sampling.wal`,
 *              `publish-journal.*` (all variants from publisher-runner).
 *
 * Files preserved: `wallets.json` (operator identity), `auth.token`,
 *              `config.json`, `node-ui.db` (dashboard state),
 *              `files/` (uploaded files), auto-update markers.
 *
 * Per the runbook contract: keystore stays so the wallet identity is
 * constant across resets, and `ensureProfile` re-derives the on-chain
 * identityId on the new chain cleanly.
 */
import { existsSync, readFileSync, writeFileSync, readdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { isExternalBackend, getSparqlEndpoint } from '@origintrail-official/dkg-storage';

const STATE_FILE = '.network-state.json';

interface PersistedNetworkState {
  /** Last chainResetMarker value the daemon booted on. */
  chainResetMarker: string | null;
  /**
   * Last triple-store backend the daemon booted on. Used by
   * `detectBackendSwitch` to warn loudly when an operator hand-edits
   * `config.store.backend` between boots — the new backend is fresh
   * and empty, so silently booting would mean stale SWM/VM data is
   * inaccessible. `null` on legacy state files (pre-RFC 120) and on
   * first boot. (RFC 120 review point #6.)
   */
  lastBackend?: string | null;
  savedAt: number;
}

/**
 * Subset of `DkgConfig['store']` used by the wipe step to talk to an
 * external SPARQL endpoint. Decoupled from the CLI's config types so
 * this module stays free of upward dependencies.
 */
export interface ChainResetWipeStoreConfig {
  backend: string;
  options?: {
    url?: string;
    queryEndpoint?: string;
    updateEndpoint?: string;
    auth?: string;
    /**
     * True when the namespace was provisioned by the CLI (PR 3 Docker
     * convenience path). Operator-provided URLs default to false; the
     * wipe then scopes deletes to the V10 named-graph prefix to avoid
     * clobbering V6/V8 data sharing the same Blazegraph instance.
     */
    managedByDkg?: boolean;
  };
}

/**
 * V10 named-graph prefix. Every context-graph the agent writes — meta,
 * shared-memory, finalisation — is rooted at `did:dkg:context-graph:`
 * (confirmed in core/genesis.ts + finalization-handler.ts + dkg-agent.ts).
 * Scoped DELETE for operator-provided external endpoints filters on this
 * prefix to leave non-V10 data (V6/V8 assertions, operator side
 * projects) alone.
 */
const V10_GRAPH_PREFIX = 'did:dkg:context-graph:';

const SPARQL_DROP_ALL = 'DROP ALL';
const SPARQL_SCOPED_DELETE =
  'DELETE { GRAPH ?g { ?s ?p ?o } } ' +
  'WHERE { GRAPH ?g { ?s ?p ?o } ' +
  `FILTER(strstarts(str(?g), "${V10_GRAPH_PREFIX}")) }`;

export interface ChainResetWipeResult {
  /** True when a wipe was performed. */
  wiped: boolean;
  /** The marker we had persisted before this boot, or null on first boot / no persisted state. */
  prevMarker: string | null;
  /** Files removed during the wipe (relative to dataDir). Empty when `wiped=false`. */
  removedFiles: string[];
  /**
   * Files we attempted to wipe but could not remove. When non-empty, the
   * marker is intentionally not persisted so the wipe retries on next boot.
   */
  failedFiles: Array<{ file: string; error: string }>;
}

export interface ChainResetWipeOptions {
  /** Node data directory (e.g. `~/.dkg`). */
  dataDir: string;
  /**
   * Bundled network config's `chainResetMarker`. `undefined` means the
   * network has not opted into the auto-wipe protocol — the hook is then
   * a no-op (no state file written, no wipe).
   */
  currentMarker: string | undefined;
  /**
   * Resolved runtime path of the random-sampling WAL. When the operator
   * sets `randomSampling.walPath` in their config, the prover writes to
   * that path instead of the default `dataDir/random-sampling.wal`. We
   * have to wipe whichever path is actually in use; the default-path
   * wipe alone would leave a stale WAL under operator-supplied paths.
   * Falsy → fall back to `dataDir/random-sampling.wal` (the default).
   */
  randomSamplingWalPath?: string;
  /**
   * Operator's `config.store` block. Required to wipe an external SPARQL
   * endpoint when the backend is `blazegraph` / `sparql-http`. Local
   * backends ignore this field.
   */
  storeConfig?: ChainResetWipeStoreConfig;
  /**
   * Override for the SPARQL HTTP transport. Tests inject a mock to
   * assert the issued UPDATE body; defaults to `globalThis.fetch`.
   * Kept on the options surface (rather than module-scope monkey-patch)
   * so parallel test cases don't race.
   */
  fetch?: typeof globalThis.fetch;
  /** Optional logger. Defaults to no-op so the function is silent in tests by default. */
  log?: (msg: string) => void;
}

function loadState(dataDir: string): PersistedNetworkState | null {
  try {
    const raw = readFileSync(join(dataDir, STATE_FILE), 'utf8');
    const obj = JSON.parse(raw) as PersistedNetworkState;
    if (typeof obj?.chainResetMarker !== 'string' && obj?.chainResetMarker !== null) return null;
    return obj;
  } catch {
    return null;
  }
}

function saveState(dataDir: string, marker: string | null): void {
  // Preserve any sibling fields (lastBackend) that `detectBackendSwitch`
  // may have written. Otherwise a chain-reset wipe would clobber a
  // freshly-recorded backend tag and the next boot would re-warn.
  const existing = loadState(dataDir) ?? { chainResetMarker: null, savedAt: 0 };
  writeFileSync(
    join(dataDir, STATE_FILE),
    JSON.stringify(
      {
        ...existing,
        chainResetMarker: marker,
        savedAt: Date.now(),
      } satisfies PersistedNetworkState,
      null,
      2,
    ),
  );
}

function saveBackendTag(dataDir: string, backend: string): void {
  const existing = loadState(dataDir) ?? { chainResetMarker: null, savedAt: 0 };
  writeFileSync(
    join(dataDir, STATE_FILE),
    JSON.stringify(
      {
        ...existing,
        lastBackend: backend,
        savedAt: Date.now(),
      } satisfies PersistedNetworkState,
      null,
      2,
    ),
  );
}

/**
 * Wipe the V10 data sitting in an external SPARQL endpoint. Runs after
 * the local file wipe so we don't strand the operator with a wiped FS
 * but a populated remote namespace (or vice versa).
 *
 * - `managedByDkg === true` → `DROP ALL`. Safe because the namespace
 *   was provisioned by the CLI and nobody else writes to it.
 * - otherwise → scoped DELETE filtered by `did:dkg:context-graph:`. The
 *   operator may be sharing the instance with V6/V8 nodes or unrelated
 *   data; the wipe must leave anything that isn't V10 alone.
 */
async function performExternalWipe(
  storeConfig: ChainResetWipeStoreConfig,
  fetchImpl: typeof globalThis.fetch,
  log: (msg: string) => void,
): Promise<{ label: string; ok: boolean; error?: string }> {
  const { updateUrl, headers } = getSparqlEndpoint({
    backend: storeConfig.backend,
    options: storeConfig.options,
  });
  const managed = storeConfig.options?.managedByDkg === true;
  const update = managed ? SPARQL_DROP_ALL : SPARQL_SCOPED_DELETE;
  const label = managed
    ? `<sparql:drop-all ${updateUrl}>`
    : `<sparql:scoped-delete ${updateUrl}>`;

  log(
    managed
      ? `  external store (DKG-managed namespace): issuing DROP ALL against ${updateUrl}`
      : `  external store (operator-provided URL): issuing scoped DELETE for "${V10_GRAPH_PREFIX}…" graphs against ${updateUrl}`,
  );

  try {
    const res = await fetchImpl(updateUrl, {
      method: 'POST',
      headers: {
        ...headers,
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: `update=${encodeURIComponent(update)}`,
    });
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      const error = `${res.status} ${res.statusText}: ${text.slice(0, 200)}`;
      log(`  WARN: external wipe failed: ${error}`);
      return { label, ok: false, error };
    }
    log(`  removed: ${label}`);
    return { label, ok: true };
  } catch (err) {
    const error = (err as Error).message;
    log(`  WARN: external wipe transport error: ${error}`);
    return { label, ok: false, error };
  }
}

function performWipe(
  dataDir: string,
  walPath: string | undefined,
  log: (msg: string) => void,
): { removedFiles: string[]; failedFiles: Array<{ file: string; error: string }> } {
  const removedFiles: string[] = [];
  const failedFiles: Array<{ file: string; error: string }> = [];

  // wipeAbs: wipe an absolute path, log under a display label. We log the
  // display label (relative when inside dataDir, absolute when not) so
  // operator-readable runbook output stays consistent regardless of
  // whether the WAL lives inside or outside the data dir.
  const wipeAbs = (abs: string, displayLabel: string) => {
    try {
      if (existsSync(abs)) {
        rmSync(abs, { recursive: true, force: true });
        removedFiles.push(displayLabel);
      }
    } catch (err) {
      const message = (err as Error).message;
      failedFiles.push({ file: displayLabel, error: message });
      log(`  WARN: failed to wipe ${displayLabel}: ${message}`);
    }
  };

  wipeAbs(join(dataDir, 'store.nq'), 'store.nq');
  wipeAbs(join(dataDir, 'store.nq.tmp'), 'store.nq.tmp');

  // Random sampling WAL: wipe the resolved runtime path (which the
  // operator may have moved out of dataDir via `randomSampling.walPath`).
  // Defaulting to dataDir/random-sampling.wal keeps the historical
  // behaviour for operators who never set the config knob.
  const walAbs = walPath && walPath.length > 0
    ? walPath
    : join(dataDir, 'random-sampling.wal');
  const walLabel = walAbs.startsWith(dataDir)
    ? walAbs.slice(dataDir.length).replace(/^\/+/, '')
    : walAbs;
  wipeAbs(walAbs, walLabel || 'random-sampling.wal');

  try {
    for (const f of readdirSync(dataDir)) {
      if (f.startsWith('publish-journal.')) {
        try {
          rmSync(join(dataDir, f), { force: true });
          removedFiles.push(f);
        } catch (err) {
          const message = (err as Error).message;
          failedFiles.push({ file: f, error: message });
          log(`  WARN: failed to wipe ${f}: ${message}`);
        }
      }
    }
  } catch (err) {
    const message = (err as Error).message;
    failedFiles.push({ file: dataDir, error: message });
    log(`  WARN: failed to list publish journals in ${dataDir}: ${message}`);
  }

  for (const f of removedFiles) log(`  removed: ${f}`);
  return { removedFiles, failedFiles };
}

export async function chainResetWipe(
  opts: ChainResetWipeOptions,
): Promise<ChainResetWipeResult> {
  const log = opts.log ?? (() => {});
  const fetchImpl = opts.fetch ?? globalThis.fetch;

  // Networks that haven't opted in: hook is a no-op. No state file is
  // touched so we don't accidentally turn on the protocol later just
  // because some leftover state file made the comparison non-trivial.
  if (opts.currentMarker === undefined) {
    return { wiped: false, prevMarker: null, removedFiles: [], failedFiles: [] };
  }

  const prev = loadState(opts.dataDir);
  const prevMarker = prev?.chainResetMarker ?? null;

  if (prevMarker === opts.currentMarker) {
    return { wiped: false, prevMarker, removedFiles: [], failedFiles: [] };
  }

  // Mismatch (including "first boot with marker present"): wipe.
  // First-boot wipe is a deliberate choice: the only way an existing
  // install reaches this branch is by upgrading INTO a release that
  // carries a marker — which means the maintainer just bumped the
  // marker as part of a chain reset, and stale state must go.
  if (prevMarker === null) {
    log(
      `Chain reset marker first detected: ${opts.currentMarker}. Wiping per-node chain-state derived files (operator identity preserved)...`,
    );
  } else {
    log(
      `Chain reset detected: marker ${prevMarker} → ${opts.currentMarker}. Wiping per-node chain-state derived files (operator identity preserved)...`,
    );
  }

  // Wipe failures are logged but do not crash boot. Crucially, we only
  // persist the marker after every targeted file was removed cleanly; a
  // partial wipe must retry on next boot instead of being masked forever.
  let removedFiles: string[] = [];
  let failedFiles: Array<{ file: string; error: string }> = [];
  let markerPersisted = false;
  try {
    ({ removedFiles, failedFiles } = performWipe(opts.dataDir, opts.randomSamplingWalPath, log));
  } catch (err) {
    const message = (err as Error).message;
    failedFiles.push({ file: '<chain-state-wipe>', error: message });
    log(
      `WARN: chain-state wipe encountered unexpected error: ${message}. Continuing boot on stale state.`,
    );
  }

  // External SPARQL wipe runs AFTER local file wipe. We don't gate one
  // on the other — both wipes attempt independently so an operator with
  // a flaky external endpoint still gets a clean local state and a
  // failedFiles entry that retries on next boot. Wrapped in try/catch
  // because helper construction (URL extraction) can throw on malformed
  // config; we want to surface that as a failedFile, not crash the boot.
  if (opts.storeConfig && isExternalBackend(opts.storeConfig.backend)) {
    try {
      const result = await performExternalWipe(opts.storeConfig, fetchImpl, log);
      if (result.ok) {
        removedFiles.push(result.label);
      } else {
        failedFiles.push({ file: result.label, error: result.error ?? 'unknown' });
      }
    } catch (err) {
      const message = (err as Error).message;
      failedFiles.push({ file: '<external-wipe>', error: message });
      log(`WARN: external SPARQL wipe failed to start: ${message}.`);
    }
  }

  if (failedFiles.length === 0) {
    try {
      saveState(opts.dataDir, opts.currentMarker);
      markerPersisted = true;
    } catch (err) {
      log(
        `WARN: failed to persist chain reset marker (${opts.currentMarker}): ${(err as Error).message}. Wipe will retry on next boot.`,
      );
    }
  } else {
    log(
      `WARN: chain-state wipe incomplete (${failedFiles.length} failure${failedFiles.length === 1 ? '' : 's'}). ` +
      'Chain reset marker was not persisted; wipe will retry on next boot.',
    );
  }
  if (failedFiles.length === 0 && markerPersisted) {
    log('Chain-state wipe complete. Continuing boot.');
  } else if (failedFiles.length === 0) {
    log('Chain-state wipe complete, but marker was not persisted. Continuing boot; wipe will retry on next boot.');
  } else {
    log('Chain-state wipe incomplete. Continuing boot so operator can repair filesystem state.');
  }

  return { wiped: true, prevMarker, removedFiles, failedFiles };
}

// =====================================================================
// Backend switch detection (RFC 120 review point #6)
// =====================================================================
//
// Switching from Oxigraph to Blazegraph (or vice versa) mid-flight means
// the new backend is fresh and empty — all SWM / VM data from the
// previous backend is unreachable. The chain-reset-wipe marker doesn't
// move when only the backend changes, so without a separate signal the
// daemon would silently boot on an empty store and the operator would
// see vanished context graphs with no explanation.
//
// This check runs at boot, BEFORE config validation / health probe /
// chain-reset wipe. Outcomes:
//   - First boot (no persisted lastBackend): silently record current.
//   - Match: silently re-record (handles legacy state files that lacked
//     the field).
//   - Mismatch + `acceptStoreReset === true`: log warning, record new.
//   - Mismatch + no override: log multi-line warning, return aborted.
//     Caller (lifecycle) exits the process.
//
// `acceptStoreReset` is controlled by the env var
// `DKG_ACCEPT_STORE_RESET=1`. A CLI flag on `dkg start` would also work
// but env keeps the boot entrypoint flat; operators set the env once,
// restart the daemon, then unset.

export interface BackendSwitchDetectOptions {
  dataDir: string;
  /**
   * Backend name from the current config. Pass the effective value
   * including the default — e.g. when `config.store?.backend` is
   * undefined, callers should pass `'oxigraph-server'` so the check
   * is symmetric across "no store block" ↔ "explicit store block".
   */
  currentBackend: string;
  /**
   * Operator opt-in to proceed despite a backend change. Sourced from
   * `process.env.DKG_ACCEPT_STORE_RESET === '1'` in production; tests
   * inject explicitly.
   */
  acceptStoreReset: boolean;
  log?: (msg: string) => void;
}

export interface BackendSwitchDetectResult {
  /** True when `lastBackend` was recorded and differs from `currentBackend`. */
  changed: boolean;
  /** Previously-recorded backend, or null if none / legacy state file. */
  previous: string | null;
  /** Effective current backend (passed through for callers). */
  current: string;
  /**
   * True when the daemon should abort boot. Set on `changed && !acceptStoreReset`.
   * Caller exits the process so the operator can either flip the env
   * var or revert their config edit.
   */
  aborted: boolean;
}

export function detectBackendSwitch(
  opts: BackendSwitchDetectOptions,
): BackendSwitchDetectResult {
  const log = opts.log ?? (() => {});
  const prev = loadState(opts.dataDir);
  const previous =
    typeof prev?.lastBackend === 'string' && prev.lastBackend.length > 0
      ? prev.lastBackend
      : null;

  // First boot or legacy state file: silently record and move on. We
  // explicitly do NOT treat null-previous as a "switch from the old
  // implicit backend"; that would re-warn every operator who upgrades
  // into this release without ever having touched their store
  // configuration. Only operator-visible config changes between two
  // recorded backends count as a switch.
  if (previous === null) {
    try {
      saveBackendTag(opts.dataDir, opts.currentBackend);
    } catch {
      // Non-fatal: if we can't write the tag now, we'll try again next
      // boot. The downside is one missed early-warning window.
    }
    return { changed: false, previous: null, current: opts.currentBackend, aborted: false };
  }

  if (previous === opts.currentBackend) {
    return { changed: false, previous, current: opts.currentBackend, aborted: false };
  }

  // Mismatch.
  const warningHeader = [
    `[STORE-SWITCH] triple-store backend changed since last boot:`,
    `  previous: ${previous}`,
    `  current:  ${opts.currentBackend}`,
    ``,
    `The new backend is a fresh store. Any context graphs, shared`,
    `memory, or finalised assertions held only in the previous backend`,
    `are NOT migrated and will be inaccessible until you either:`,
    `  - revert config.store.backend to "${previous}", or`,
    `  - accept the data loss by setting DKG_ACCEPT_STORE_RESET=1 in`,
    `    the environment and restarting.`,
  ].join('\n');

  if (!opts.acceptStoreReset) {
    log(warningHeader);
    log(``);
    log(`Refusing to start: set DKG_ACCEPT_STORE_RESET=1 to proceed.`);
    return { changed: true, previous, current: opts.currentBackend, aborted: true };
  }

  log(warningHeader);
  log(``);
  log(`DKG_ACCEPT_STORE_RESET=1 set — proceeding with the new backend.`);
  try {
    saveBackendTag(opts.dataDir, opts.currentBackend);
  } catch (err) {
    log(`WARN: failed to persist new backend tag: ${(err as Error).message}. Will re-warn on next boot.`);
  }
  return { changed: true, previous, current: opts.currentBackend, aborted: false };
}
