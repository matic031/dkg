import { Command } from 'commander';
import { readFileSync, existsSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { spawn, execSync } from 'node:child_process';
import { createReadStream } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { homedir } from 'node:os';
import { readFile, writeFile, unlink, appendFile } from 'node:fs/promises';
import { ethers } from 'ethers';
import { resolveRpcUrls } from '@origintrail-official/dkg-chain';
import {
  dkgAuthTokenPath,
  FAUCET_WALLETS_PER_REQUEST,
  getFundableWalletAddresses,
  requestFaucetFunding,
  resolveDkgConfigHome,
  toErrorMessage,
  hasErrorCode,
} from '@origintrail-official/dkg-core';
import yaml from 'js-yaml';
import {
  loadConfig, saveConfig, configExists, configPath,
  readPid, readApiPort, isProcessRunning, dkgDir, logPath, ensureDkgDir, removeApiPort,
  apiPortPath,
  loadNetworkConfig, loadProjectConfig, resolveAutoUpdateConfig, resolveAutoUpdateSource, resolveChainConfig, validateNetworkConfigReadiness,
  releasesDir, activeSlot, swapSlot,
  slotEntryPoint, isStandaloneInstall, repoDir, isDkgMonorepo, classifyMonorepoInit, sharedHomeInitGate,
  resolveContextGraphs, resolveNetworkDefaultContextGraphs,
  readNodeRoleFromConfigSync,
  type AutoUpdateConfig,
} from '../config.js';
import { ApiClient } from '../api-client.js';
import { parsePositiveIntegerOption, parsePositiveMsOption } from '../publisher-runner.js';
import { promptStoreBackend, applyStoreFlagsToConfig } from '../store-wizard.js';
import { runConfiguredSourceWorker } from '../source-worker-runner.js';
import { batchEntityQuads } from '../batching.js';
import {
  runDaemon,
  checkForNpmVersionUpdate,
  performNpmUpdate,
  performNpmUpdateEdge,
  getCurrentCliVersion,
  DAEMON_EXIT_CODE_RESTART,
  resolveStandaloneInstall,
  decodeForcedExitCode,
} from '../daemon.js';
import {
  isLivenessProbeEnabled,
  startLivenessWatcher,
  LIVENESS_CONSECUTIVE_FAILURES_TO_KILL,
} from '../daemon/supervisor-liveness.js';
import { migrateToBlueGreen, noteEdgeLegacyReleases } from '../migration.js';
import { ensureRollbackNodeUiBundle } from '../rollback-node-ui.js';
import {
  isDaemonUnreachable,
  cliSleep,
  cliErrorMessage,
  STARTUP_BANNER,
  normalizeVersionTagRef,
  getCliVersion,
  parseOptionalVerifyTimeoutOption,
  loadStructuredFile,
  loadQuadsFromInput,
  resolveDaemonEntryPoint,
  probeHostForApiHost,
  selectedDkgHomeForEnv,
  withSelectedDkgHome,
  VERIFY_COLLECTION_TIMEOUT_MIN_MS,
  VERIFY_COLLECTION_TIMEOUT_MAX_MS,
  printCatchupStatus,
  runCatchupStatusCommand,
  printMessage,
  shortId,
  formatUptime,
  publishEntityBatches,
  formatPublisherJobOutput,
  formatPublisherJobValue,
  stripQuotes,
  formatQuadObject,
  sleep,
  stopDaemonIfRunning,
} from '../cli-helpers.js';
import type { ActionOpts, CatchupStatusCommandOptions } from '../cli-helpers.js';
import {
  cliWithTimeout,
  isCliKnownTransactionError,
  isCliRetryableRpcError,
  createCliEvmProviders,
  getCliReceiptWithFailover,
  assertCliSuccessfulReceipt,
  sendCliRawTransactionWithFailover,
  CLI_RPC_READ_STALL_TIMEOUT_MS,
  CLI_RPC_BROADCAST_TIMEOUT_MS,
  CLI_RPC_RECEIPT_ATTEMPT_TIMEOUT_MS,
  CLI_RPC_RECEIPT_POLL_INTERVAL_MS,
  CLI_RPC_RECEIPT_TIMEOUT_MS,
} from '../cli-rpc.js';
import {
  appendSupervisorLog,
  supervisorWarn,
  maybeStartSupervisorLivenessWatcher,
  runDaemonSupervisor,
  runForegroundSupervisor,
} from '../cli-supervisor.js';

/**
 * Pure builder for the `autoUpdate` block `dkg init` persists. Extracted from
 * the interactive wizard so it is unit-testable — the decline path (must write
 * `{ enabled: false }`, NOT fall through to the enabled network default) and
 * channel/advanced-field preservation across reruns have each regressed before.
 * The wizard gathers `answers` interactively and passes them in.
 */
export function buildInitAutoUpdate(opts: {
  enableAutoUpdate: boolean;
  existingAutoUpdate: AutoUpdateConfig | undefined;
  networkAutoUpdate?: {
    repo?: string;
    branch?: string;
    allowPrerelease?: boolean;
    sshKeyPath?: string;
    checkIntervalMinutes?: number;
  };
  projRepo: string;
  projDefaultBranch: string;
  answers?: {
    repo: string;
    branch: string;
    allowPrerelease: boolean;
    sshKeyPath: string;
    interval: number;
  };
}): AutoUpdateConfig {
  const { enableAutoUpdate, existingAutoUpdate, networkAutoUpdate, projRepo, projDefaultBranch, answers } = opts;
  if (!enableAutoUpdate) {
    // Explicit decline: persist `enabled: false` so resolveAutoUpdateConfig
    // does NOT fall through to the enabled network default. Keep any existing
    // advanced fields (channel, etc.) — the operator only toggled auto-apply.
    return existingAutoUpdate
      ? { ...existingAutoUpdate, enabled: false }
      : { enabled: false };
  }
  const a = answers ?? { repo: '', branch: '', allowPrerelease: true, sshKeyPath: '', interval: NaN };
  // Effective upstream defaults — what the node would use with nothing
  // persisted. We persist a field only when it differs, so future changes to
  // the shipped network/project config propagate without a config rewrite.
  const effectiveRepo = networkAutoUpdate?.repo ?? projRepo;
  const effectiveBranch = networkAutoUpdate?.branch ?? projDefaultBranch;
  const effectiveAllowPrerelease = networkAutoUpdate?.allowPrerelease ?? true;
  const effectiveSshKeyPath = networkAutoUpdate?.sshKeyPath ?? '';
  const effectiveInterval = networkAutoUpdate?.checkIntervalMinutes ?? 30;
  return {
    enabled: true,
    // OT-RFC-41 Bundle B1d: explicit npm source for fresh installs.
    source: 'npm' as const,
    ...(a.repo && a.repo !== effectiveRepo ? { repo: a.repo } : {}),
    ...(a.branch && a.branch !== effectiveBranch ? { branch: a.branch } : {}),
    ...(a.allowPrerelease !== effectiveAllowPrerelease ? { allowPrerelease: a.allowPrerelease } : {}),
    ...(a.sshKeyPath && a.sshKeyPath !== effectiveSshKeyPath ? { sshKeyPath: a.sshKeyPath } : {}),
    ...(Number.isFinite(a.interval) && a.interval !== effectiveInterval ? { checkIntervalMinutes: a.interval } : {}),
    // Preserve advanced fields the wizard does not prompt for so a rerun
    // doesn't silently revert operator tuning: build timeouts, custom SSH
    // command, and the per-node `channel` cohort pin.
    ...(existingAutoUpdate?.buildTimeoutMs ? { buildTimeoutMs: existingAutoUpdate.buildTimeoutMs } : {}),
    ...(existingAutoUpdate?.sshCommand ? { sshCommand: existingAutoUpdate.sshCommand } : {}),
    ...(existingAutoUpdate?.channel ? { channel: existingAutoUpdate.channel } : {}),
  } as AutoUpdateConfig;
}

export function registerInitCommand(program: Command): void {
// ─── dkg init ────────────────────────────────────────────────────────

program
  .command('init')
  .description('Interactive setup — set node name, role, and relay')
  .option('--role <role>', "Node role: 'edge' (default; personal laptop / behind NAT) or 'core' (24/7 relay / SLA)")
  .option(
    '--store <backend>',
    'Pre-fill the triple-store backend prompt (oxigraph-server | oxigraph | blazegraph | sparql-http).',
  )
  .option(
    '--store-url <url>',
    'Pre-fill the SPARQL endpoint URL prompt for external backends.',
  )
  .option(
    '-y, --yes',
    'Skip the confirmation prompt when running from a monorepo checkout into an existing ~/.dkg home (non-interactive opt-in to modifying it).',
  )
  .action(async (opts: ActionOpts) => {
    // OT-RFC-41 follow-up (issue #960): `dkg init` runs normally from a monorepo
    // checkout, exactly like an npm install — the only difference is the home
    // directory. The CLI's home resolver (`dkgDir()` → `resolveDkgConfigHome`)
    // routes a clone to `~/.dkg-dev` (kept separate from an npm install's
    // `~/.dkg`), the SAME home the local dev daemon resolves. (PR #753 /
    // Bundle B1c hard-refused all monorepo inits; that was over-strict.)
    //
    // The one risky case is `shared-npm-home`: a clone with no `DKG_HOME` where
    // the resolver fell back to a pre-existing `~/.dkg` (which *might* be an
    // npm-installed node). We must neither silently proceed (a missed warning
    // would mutate that node's config) nor hard-block (config presence isn't
    // proof of ownership, and a dev may target `~/.dkg` on purpose), so we
    // require an explicit opt-in via `sharedHomeInitGate`: confirm interactively
    // or pass `--yes`. See `classifyMonorepoInit` + `sharedHomeInitGate`.
    switch (classifyMonorepoInit({
      isMonorepo: isDkgMonorepo(),
      dkgHomeEnv: process.env.DKG_HOME,
      resolvedHome: dkgDir(),
      npmHome: join(homedir(), '.dkg'),
    })) {
      case 'dev-home':
        console.log(
          `[dkg init] Monorepo checkout detected — writing dev config to ${dkgDir()} ` +
            `(set DKG_HOME to override).`,
        );
        break;
      case 'shared-npm-home': {
        const home = dkgDir();
        const gate = sharedHomeInitGate({
          yes: opts.yes === true,
          isTty: Boolean(process.stdin.isTTY),
        });
        if (gate === 'refuse') {
          console.error(
            `\n[dkg init] Refusing to modify the existing config home ${home} non-interactively.\n` +
              `  A config already exists there and this is a monorepo checkout, so it may belong\n` +
              `  to an npm-installed node. To proceed, choose one:\n` +
              `    • re-run interactively and confirm at the prompt, or\n` +
              `    • pass --yes to opt in explicitly, or\n` +
              `    • keep this checkout isolated:  DKG_HOME=~/.dkg-dev dkg init\n`,
          );
          process.exit(1);
        }
        if (gate === 'prompt') {
          const confirmRl = createInterface({ input: process.stdin, output: process.stdout });
          const confirmed = await new Promise<boolean>(resolve => {
            confirmRl.question(
              `\n[dkg init] ⚠️  ${home} already contains a DKG config and this is a monorepo\n` +
                `  checkout — it may belong to an npm-installed node. Continuing will read and\n` +
                `  may OVERWRITE it. (Use DKG_HOME=~/.dkg-dev to keep this checkout isolated.)\n` +
                `  Continue and modify ${home}? [y/N]: `,
              answer => resolve(/^y(es)?$/i.test(answer.trim())),
            );
          }).finally(() => confirmRl.close());
          if (!confirmed) {
            console.error(`\n[dkg init] Aborted — ${home} left unchanged.\n`);
            process.exit(1);
          }
        }
        console.warn(
          `[dkg init] Proceeding into existing ${home}` +
            `${opts.yes === true ? ' (--yes)' : ' (confirmed)'}.`,
        );
        break;
      }
      default:
        break;
    }

    await ensureDkgDir();
    const existing = await loadConfig();
    const network = await loadNetworkConfig(existing.networkConfig);
    const readiness = validateNetworkConfigReadiness(network);
    if (!readiness.ok) {
      for (const message of readiness.messages) console.error(message);
      process.exit(1);
    }
    const rl = createInterface({ input: process.stdin, output: process.stdout });
    const ask = (q: string, def?: string): Promise<string> =>
      new Promise(resolve => {
        const suffix = def ? ` (${def})` : '';
        rl.question(`${q}${suffix}: `, answer => resolve(answer.trim() || def || ''));
      });

    if (network) {
      console.log(`DKG Node Setup — ${network.networkName}\n`);
    } else {
      console.log('DKG Node Setup\n');
    }

    const name = await ask('Node name', existing.name !== 'dkg-node' ? existing.name : undefined);
    // OT-RFC-41 Bundle B1d: `--role <edge|core>` flag short-circuits
    // the interactive role prompt. Default precedence:
    //   1. `--role` flag (explicit operator intent)
    //   2. existing config (re-running `dkg init` on an existing node)
    //   3. network default (per-network preferred role)
    //   4. 'edge' (safe default — RFC §1)
    const defaultRole = existing.nodeRole ?? network?.defaultNodeRole ?? 'edge';
    let nodeRole: 'edge' | 'core';
    if (opts.role === 'edge' || opts.role === 'core') {
      nodeRole = opts.role;
      console.log(`Node role: ${nodeRole} (from --role flag)`);
    } else if (opts.role !== undefined) {
      console.error(`Invalid --role value: ${JSON.stringify(opts.role)}. Expected 'edge' or 'core'.`);
      process.exit(1);
    } else {
      const roleAnswer = await ask('Node role (edge / core)', defaultRole);
      nodeRole = roleAnswer === 'core' ? 'core' : 'edge';
    }

    // Triple-store backend (RFC 120, plan PR 2 item 1 + PR 3 Docker
    // branch). Default: the daemon-managed local Oxigraph server
    // (`oxigraph-server`) — see promptStoreBackend. Operators selecting
    // "blazegraph" with a blank URL get the Docker convenience path
    // (if Docker is installed) — the namespace name defaults to the
    // node name so each operator gets their own DKG-owned namespace.
    const { storeBlock } = await promptStoreBackend({
      ask,
      existingStore: existing.store,
      flagBackend: opts.store,
      flagUrl: opts.storeUrl,
      nodeName: name || existing.name,
    });

    // Pre-fill relay from network config if user hasn't set one.
    // Show the first relay as the default, but only persist to config if the
    // user overrides it — otherwise the daemon will use the full relay list
    // from network/testnet.json, which stays current across updates.
    const networkDefaultRelay = network?.relays?.[0];
    const defaultRelay = existing.relay ?? networkDefaultRelay;
    const relay = nodeRole === 'edge'
      ? await ask('Relay multiaddr', defaultRelay)
      : await ask('Relay multiaddr (optional for core)', defaultRelay);

    const existingContextGraphs = resolveContextGraphs(existing);
    const defaultContextGraphs = existingContextGraphs.length
      ? existingContextGraphs.join(',')
      : resolveNetworkDefaultContextGraphs(network).length
        ? resolveNetworkDefaultContextGraphs(network).join(',')
        : undefined;
    const contextGraphsStr = await ask(
      'Context graphs to subscribe (comma-separated)',
      defaultContextGraphs,
    );
    const contextGraphs = contextGraphsStr ? contextGraphsStr.split(',').map(s => s.trim()).filter(Boolean) : [];
    const apiPort = parseInt(await ask('API port', String(existing.apiPort)), 10);

    // OT-RFC-41 §4.3 Bundle B1d: post-rc.12, auto-update is npm-only.
    // The prompt wording is updated; the persisted config carries an
    // explicit `source: 'npm'` so the daemon's resolution is
    // unambiguous (no implicit `isStandaloneInstall()` probe).
    const autoUpdateDefault = existing.autoUpdate?.enabled ?? network?.autoUpdate?.enabled ?? false;
    const enableAutoUpdate = (await ask(
      'Enable auto-update (npm; y/n)',
      autoUpdateDefault ? 'y' : 'n',
    )).toLowerCase() === 'y';

    const proj = loadProjectConfig();
    // Gather answers interactively only when enabling; the pure
    // `buildInitAutoUpdate` (above) does the persist decision for both paths so
    // it is unit-tested. Prompt defaults show existing value, else the upstream
    // (network → project) default.
    let autoUpdateAnswers:
      | { repo: string; branch: string; allowPrerelease: boolean; sshKeyPath: string; interval: number }
      | undefined;
    if (enableAutoUpdate) {
      const effectiveRepo = network?.autoUpdate?.repo ?? proj.repo;
      const effectiveBranch = network?.autoUpdate?.branch ?? proj.defaultBranch;
      const effectiveAllowPrerelease = network?.autoUpdate?.allowPrerelease ?? true;
      const effectiveSshKeyPath = network?.autoUpdate?.sshKeyPath ?? '';
      const effectiveInterval = network?.autoUpdate?.checkIntervalMinutes ?? 30;

      const defaultRepo = existing.autoUpdate?.repo ?? effectiveRepo;
      const defaultBranch = existing.autoUpdate?.branch ?? effectiveBranch;
      const defaultAllowPrerelease = existing.autoUpdate?.allowPrerelease ?? effectiveAllowPrerelease;
      const defaultSshKeyPath = existing.autoUpdate?.sshKeyPath ?? effectiveSshKeyPath;
      const defaultInterval = existing.autoUpdate?.checkIntervalMinutes ?? effectiveInterval;

      const repo = await ask('Git repo/path (owner/name, URL, or git@host:org/repo.git)', defaultRepo);
      const branch = await ask('Branch', defaultBranch);
      const allowPrerelease = (await ask(
        'Allow pre-release versions? (y/n)',
        defaultAllowPrerelease ? 'y' : 'n',
      )).toLowerCase() === 'y';
      const sshKeyPath = (await ask('SSH private key path (optional; blank uses agent/default SSH config)', defaultSshKeyPath)).trim();
      const interval = parseInt(await ask('Check interval (minutes)', String(defaultInterval)), 10);

      autoUpdateAnswers = { repo, branch, allowPrerelease, sshKeyPath, interval };
    }
    const autoUpdate = buildInitAutoUpdate({
      enableAutoUpdate,
      existingAutoUpdate: existing.autoUpdate,
      networkAutoUpdate: network?.autoUpdate,
      projRepo: proj.repo,
      projDefaultBranch: proj.defaultBranch,
      answers: autoUpdateAnswers,
    });

    // Chain configuration. Field-merge: existing config wins per-field over
    // network defaults so an operator who's only customised RPC keeps that
    // override even after `dkg init` re-prompts.
    const chainDefaults = resolveChainConfig(existing, network);
    const defaultRpcUrl = chainDefaults?.rpcUrl;
    const defaultRpcUrls = chainDefaults?.rpcUrls?.join(', ') ?? '';
    const defaultHubAddress = chainDefaults?.hubAddress;
    const defaultChainId = chainDefaults?.chainId;

    console.log('\nBlockchain Configuration:');
    const rpcUrl = await ask('RPC URL', defaultRpcUrl);
    const rpcUrlsInput = await ask('Backup RPC URLs (comma-separated, optional; type "none" to clear)', defaultRpcUrls);
    const clearRpcUrls = rpcUrlsInput.trim().toLowerCase() === 'none';
    const rpcUrls = clearRpcUrls ? [] : rpcUrlsInput.split(',').map((s) => s.trim()).filter(Boolean);
    const hubAddress = await ask('Hub contract address', defaultHubAddress);
    const chainIdStr = await ask('Chain ID', defaultChainId);

    const chainSection = rpcUrl && hubAddress ? {
      type: 'evm' as const,
      rpcUrl,
      ...(clearRpcUrls || rpcUrls.length ? { rpcUrls } : {}),
      hubAddress,
      chainId: chainIdStr || undefined,
    } : undefined;

    // API authentication
    console.log('\nAPI Authentication:');
    const existingAuthEnabled = existing.auth?.enabled !== false;
    const enableAuth = (await ask(
      'Enable API authentication (y/n)',
      existingAuthEnabled ? 'y' : 'n',
    )).toLowerCase() === 'y';

    rl.close();

    const config = {
      ...existing,
      name: name || 'dkg-node',
      relay: (!existing.relay && relay === networkDefaultRelay) ? undefined : (relay || undefined),
      apiPort,
      nodeRole,
      contextGraphs,
      // `autoUpdate` already holds the correct value for BOTH branches: the
      // fully-built block when enabled, or `{ enabled: false }` (preserving
      // existing advanced fields) when declined. The old ternary here re-read
      // `existing.autoUpdate` on decline, silently discarding the persisted
      // disable — so a fresh-config operator who said "no" still auto-updated.
      autoUpdate,
      chain: chainSection ?? existing.chain,
      auth: { enabled: enableAuth, tokens: existing.auth?.tokens },
      // Persist the chosen backend. `storeBlock === null` from the
      // wizard means "leave the store block omitted"; daemon boot treats
      // that as the managed `oxigraph-server` default.
      store: storeBlock ?? undefined,
    };
    await saveConfig(config);

    // Generate wallets eagerly so they're available for faucet funding
    let walletAddresses: string[] = [];
    try {
      const { loadOpWallets } = await import('@origintrail-official/dkg-agent');
      const opWallets = await loadOpWallets(dkgDir());
      walletAddresses = getFundableWalletAddresses(opWallets);
    } catch (err: any) {
      console.warn(`\nWarning: could not generate wallets (${err?.message ?? String(err)}).`);
      console.warn('Wallets will be auto-generated on first "dkg start".');
    }

    console.log(`\nConfig saved to ${configPath()}`);
    console.log(`  name:       ${config.name}`);
    console.log(`  role:       ${config.nodeRole}`);
    const relayDisplay = config.relay
      ?? (network?.relays?.length ? `(network default — ${network.relays.length} relays)` : '(none)');
    console.log(`  relay:      ${relayDisplay}`);
    console.log(`  context graphs: ${contextGraphs.length ? contextGraphs.join(', ') : '(none)'}`);
    console.log(`  apiPort:    ${config.apiPort}`);
    console.log(`  auth:       ${enableAuth ? `enabled (token in ${dkgAuthTokenPath(dkgDir())})` : 'disabled'}`);
    console.log(
      `  store:      ${
        storeBlock
          // Endpoint shape varies by backend: blazegraph uses `options.url`,
          // sparql-http uses `options.queryEndpoint`, and oxigraph-server / a
          // preserved local block have no endpoint — render the backend name
          // alone in that case rather than dropping the configured endpoint.
          ? (() => {
              const o = storeBlock.options as { url?: string; queryEndpoint?: string } | undefined;
              const endpoint = o?.url ?? o?.queryEndpoint;
              return `${storeBlock.backend}${endpoint ? ` (${endpoint})` : ''}`;
            })()
          : 'oxigraph-server (default)'
      }`,
    );
    {
      const resolved = resolveAutoUpdateConfig(config, network);
      console.log(
        `  autoUpdate: ${
          resolved
            ? `${resolved.repo}@${resolved.branch}` +
              `${resolved.allowPrerelease ? ' (pre-release allowed)' : ''}` +
              `${resolved.sshKeyPath ? ` (ssh key: ${resolved.sshKeyPath})` : ''}`
            : 'disabled'
        }`,
      );
    }
    {
      // Display the effective (config + network) merged view, so an operator
      // who only set rpcUrl still sees the inherited hub from the network.
      const effective = resolveChainConfig(config, network);
      console.log(`  chain:      ${effective?.rpcUrl && effective?.hubAddress
        ? `${effective.rpcUrl}${effective.rpcUrls?.length ? ` (+${effective.rpcUrls.length} backups)` : ''} (hub: ${effective.hubAddress.slice(0, 10)}...)`
        : '(not configured)'}`);
    }
    if (network) {
      console.log(`  network:    ${network.networkName}`);
    }
    if (walletAddresses.length) {
      console.log(`  wallets:    ${walletAddresses.join(', ')}`);
    }

    // Auto-fund from testnet faucet if available
    if (network?.faucet?.url && walletAddresses.length > 0) {
      if (walletAddresses.length > FAUCET_WALLETS_PER_REQUEST) {
        console.log(`\nNote: faucet supports up to ${FAUCET_WALLETS_PER_REQUEST} wallets per request; funding wallets in batches.`);
      }
      console.log(`\nRequesting testnet tokens from faucet...`);
      try {
        const result = await requestFaucetFunding(
          network.faucet.url, network.faucet.mode, walletAddresses, config.name,
        );
        if (result.success) {
          console.log(`  Funded: ${result.funded.join(', ')}`);
          if (result.error) {
            console.log(`  Faucet partially completed (${result.error}). Retry later for any remaining wallets.`);
            if (result.failedWallets?.length) {
              console.log(`  Remaining: ${result.failedWallets.join(', ')}`);
            }
          }
        } else if (result.error) {
          console.log(`  Faucet request failed (${result.error}). Fund manually or retry later.`);
        } else {
          console.log('  Faucet returned no successful transactions (you may already have tokens or hit a cooldown).');
        }
      } catch (err: any) {
        console.log(`  Faucet unavailable: ${err?.message ?? String(err)}. Fund your wallet manually.`);
      }
    }

    console.log(`\nRun "dkg start" to start the node.`);
  });
}
