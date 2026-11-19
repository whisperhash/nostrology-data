// Sidecar configuration, all from env, parsed ONCE at startup and frozen.
// Every value has a default that serves the local test/demo path (file store,
// testnut allowlisted); the compose environment overrides for the box. A bad
// value fails startup loudly - a redeemer running with a half-parsed config is
// a money bug, not a degraded mode.

import { canonicalMintUrl } from './canonical.ts';

export interface Config {
  /** Bind port inside the container netns. NEVER published to a host port. */
  port: number;
  /** Price of one call, in sats: a token must carry exactly this plus the swap fee (verify.ts). */
  priceSats: number;
  /** The only accepted unit. */
  unit: string;
  /** Canonicalized mint allowlist, exact-match after canonicalization. */
  mints: string[];
  /** "file" (local dir; tests + pre-Terraform demo) or "s3". */
  store: 'file' | 's3';
  storeDir: string;
  s3Bucket: string;
  s3Prefix: string;
  s3Region: string;
  /** Outbound-to-mint rate limits (Security: unauthenticated inbound must not
   * become unlimited outbound against the one dependency we cannot control). */
  maxConcurrentMintCalls: number;
  maxMintCallsPerMinute: number;
  maxMintCallsPerMinutePerIp: number;
  /** Per-attempt fetch timeout for every mint call. The RETRY count is NOT
   * configurable: cashu-ts's request layer hardcodes up to 9 NUT-19 replays
   * within the mint's advertised ttl, and pretending to offer a knob it does
   * not have would be dead config. Worst-case mint conversation is therefore
   * ~10 x this value + ~6s backoff for the swap phase, plus one restore and
   * one checkstate attempt - the MCP-side client timeout is derived from this
   * arithmetic (cashu.py REDEEMER_TIMEOUT_SECONDS). */
  mintTimeoutMs: number;
  /** How long after answering `redeemed` to wait for the MCP server's
   * delivery report before feeding the delivery-failed alert. */
  outcomeWatchdogMs: number;
}

function intEnv(env: NodeJS.ProcessEnv, name: string, fallback: number): number {
  const raw = env[name];
  if (raw === undefined || raw === '') return fallback;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 0) {
    throw new Error(`${name} must be a non-negative integer, got ${JSON.stringify(raw)}`);
  }
  return n;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const rawMints =
    env.CASHU_MINTS ?? 'https://mint.minibits.cash/Bitcoin,https://testnut.cashu.space';
  const mints = rawMints
    .split(',')
    .map((m) => m.trim())
    .filter(Boolean)
    .map(canonicalMintUrl);
  if (mints.length === 0) throw new Error('CASHU_MINTS must list at least one mint URL');

  const store = env.CASHU_STORE ?? 'file';
  if (store !== 'file' && store !== 's3') {
    throw new Error(`CASHU_STORE must be "file" or "s3", got ${JSON.stringify(store)}`);
  }
  if (store === 's3' && !env.CASHU_S3_BUCKET) {
    throw new Error('CASHU_STORE=s3 requires CASHU_S3_BUCKET');
  }

  return Object.freeze({
    port: intEnv(env, 'CASHU_PORT', 8081),
    priceSats: intEnv(env, 'CASHU_PRICE_SATS', 21),
    unit: env.CASHU_UNIT ?? 'sat',
    mints,
    store,
    storeDir: env.CASHU_STORE_DIR ?? './redemptions',
    s3Bucket: env.CASHU_S3_BUCKET ?? '',
    s3Prefix: env.CASHU_S3_PREFIX ?? 'redemptions/',
    s3Region: env.AWS_REGION ?? 'us-east-1',
    maxConcurrentMintCalls: intEnv(env, 'CASHU_MAX_CONCURRENT_MINT_CALLS', 2),
    maxMintCallsPerMinute: intEnv(env, 'CASHU_MAX_MINT_CALLS_PER_MINUTE', 30),
    maxMintCallsPerMinutePerIp: intEnv(env, 'CASHU_MAX_MINT_CALLS_PER_MINUTE_PER_IP', 6),
    mintTimeoutMs: intEnv(env, 'CASHU_MINT_TIMEOUT_MS', 15_000),
    outcomeWatchdogMs: intEnv(env, 'CASHU_OUTCOME_WATCHDOG_MS', 300_000),
  });
}
