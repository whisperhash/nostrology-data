// Offline unit tests - no network, no mint. Run: npm test
// (node's built-in test runner executes the .ts sources directly under type
// stripping; tsconfig's erasableSyntaxOnly keeps the sources inside that
// subset.)

import { strict as assert } from 'node:assert';
import { test } from 'node:test';
import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { canonicalMintUrl, isAllowlistedMint } from '../src/canonical.ts';
import { loadConfig } from '../src/config.ts';
import { nut02Fee } from '../src/mints.ts';
import { RateLimiter } from '../src/ratelimit.ts';
import { Redeemer, tokenHash } from '../src/redeem.ts';
import { FileStore } from '../src/store.ts';
import { StructuralError, parseCredential, precheck } from '../src/verify.ts';
import { exposeMetrics } from '../src/metrics.ts';

// ---------------------------------------------------------------- canonical

test('canonicalization: the draft-cashu-charge profile', () => {
  // lowercase scheme+host, default port dropped, trailing slashes stripped,
  // path case preserved.
  assert.equal(canonicalMintUrl('HTTPS://Mint.Minibits.cash/Bitcoin/'), 'https://mint.minibits.cash/Bitcoin');
  assert.equal(canonicalMintUrl('https://mint.minibits.cash:443/Bitcoin'), 'https://mint.minibits.cash/Bitcoin');
  assert.equal(canonicalMintUrl('http://localhost:3338/'), 'http://localhost:3338');
  // NUT-00's own example carries a non-default port; it must survive.
  assert.equal(canonicalMintUrl('https://8333.space:3338'), 'https://8333.space:3338');
  // Path case is NOT folded (RFC 3986: only scheme/host are case-insensitive).
  assert.notEqual(canonicalMintUrl('https://mint.minibits.cash/bitcoin'), canonicalMintUrl('https://mint.minibits.cash/Bitcoin'));
});

test('allowlist match is exact after canonicalization', () => {
  const allow = [canonicalMintUrl('https://mint.minibits.cash/Bitcoin')];
  assert.equal(isAllowlistedMint('HTTPS://MINT.MINIBITS.CASH:443/Bitcoin/', allow), true);
  assert.equal(isAllowlistedMint('https://mint.minibits.cash/bitcoin', allow), false);
  assert.equal(isAllowlistedMint('https://evil.example/Bitcoin', allow), false);
  assert.equal(isAllowlistedMint('not a url', allow), false);
});

// ---------------------------------------------------------------- NUT-02 fee

test('nut02Fee: canonical integer expression, summed per proof', () => {
  assert.equal(nut02Fee([]), 0);
  assert.equal(nut02Fee([0, 0, 0]), 0);
  // testnut's 100 ppk: 1..10 proofs -> 1 sat; 11 proofs -> 2.
  assert.equal(nut02Fee([100]), 1);
  assert.equal(nut02Fee(Array(10).fill(100)), 1);
  assert.equal(nut02Fee(Array(11).fill(100)), 2);
  // The float-form trap NUT-02 warns about: (sum+999)//1000, not ceil.
  assert.equal(nut02Fee([1]), 1);
  assert.equal(nut02Fee([999]), 1);
  assert.equal(nut02Fee([1000]), 1);
  assert.equal(nut02Fee([1001]), 2);
});

// ---------------------------------------------------------------- config

test('config: defaults serve the file-store test path; bad values fail loud', () => {
  const cfg = loadConfig({} as NodeJS.ProcessEnv);
  assert.equal(cfg.priceSats, 21);
  assert.equal(cfg.unit, 'sat');
  assert.equal(cfg.store, 'file');
  assert.deepEqual(cfg.mints, ['https://mint.minibits.cash/Bitcoin', 'https://testnut.cashu.space']);
  assert.throws(() => loadConfig({ CASHU_PRICE_SATS: 'lots' } as NodeJS.ProcessEnv));
  assert.throws(() => loadConfig({ CASHU_STORE: 's3' } as NodeJS.ProcessEnv), /CASHU_S3_BUCKET/);
  assert.throws(() => loadConfig({ CASHU_MINTS: ' ' } as NodeJS.ProcessEnv), /at least one/);
});

// ---------------------------------------------------------------- credential parsing

test('parseCredential: cashuA is rejected with its own reason', () => {
  assert.throws(
    () => parseCredential('cashuAeyJ0b2tlbiI6W119'),
    (e: unknown) => e instanceof StructuralError && e.reason === 'tokenv3_unsupported',
  );
});

test('parseCredential: a payment request is named for what it is', () => {
  assert.throws(
    () => parseCredential('creqAgaRhdA=='),
    (e: unknown) => e instanceof StructuralError && e.reason === 'payment_request_not_token',
  );
});

test('parseCredential: garbage and malformed payloads are not_a_token', () => {
  for (const bad of [
    'hello',
    'cashuB!!!not-base64!!!',
    '{"mint": 5}',
    '{"mint":"https://x","unit":"sat","proofs":[]}',
    '{"mint":"https://x","unit":"sat","proofs":[{"amount":"1"}]}',
    // NUT-00 amounts are u64: negative, fractional and zero amounts must die
    // here, not flow into the sum check (review finding).
    '{"mint":"https://x","unit":"sat","proofs":[{"amount":-21,"id":"00aa","secret":"s","C":"02aa"}]}',
    '{"mint":"https://x","unit":"sat","proofs":[{"amount":10.5,"id":"00aa","secret":"s","C":"02aa"}]}',
    '{"mint":"https://x","unit":"sat","proofs":[{"amount":0,"id":"00aa","secret":"s","C":"02aa"}]}',
  ]) {
    assert.throws(
      () => parseCredential(bad),
      (e: unknown) => e instanceof StructuralError && e.reason === 'not_a_token',
      `expected not_a_token for ${JSON.stringify(bad)}`,
    );
  }
});

test('parseCredential: a NUT-18 PaymentRequestPayload is accepted', () => {
  const payload = JSON.stringify({
    id: 'x',
    unit: 'sat',
    mint: 'https://testnut.cashu.space',
    proofs: [{ amount: 21, id: '00ad268c4d1f5826', secret: 's', C: '02aa' }],
  });
  const parsed = parseCredential(payload);
  assert.equal(parsed.mint, 'https://testnut.cashu.space');
  assert.equal(parsed.proofs?.length, 1);
});

test('precheck: unit and allowlist are enforced before any network', () => {
  const allow = ['https://testnut.cashu.space'];
  const usd = JSON.stringify({
    unit: 'usd',
    mint: 'https://testnut.cashu.space',
    proofs: [{ amount: 21, id: '00ad268c4d1f5826', secret: 's', C: '02aa' }],
  });
  assert.throws(
    () => precheck(usd, 'sat', allow),
    (e: unknown) => e instanceof StructuralError && e.reason === 'unit_mismatch',
  );
  const foreignMint = JSON.stringify({
    unit: 'sat',
    mint: 'https://not-on-the-list.example',
    proofs: [{ amount: 21, id: '00ad268c4d1f5826', secret: 's', C: '02aa' }],
  });
  assert.throws(
    () => precheck(foreignMint, 'sat', allow),
    (e: unknown) => e instanceof StructuralError && e.reason === 'mint_not_allowlisted',
  );
});

// ---------------------------------------------------------------- rate limiter

test('a rate-limited redemption logs the token hash and never the caller address', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'redeemer-ratelimit-'));
  const r = new Redeemer(loadConfig({} as NodeJS.ProcessEnv), new FileStore(dir));
  (r as unknown as { limiter: { acquire: () => null } }).limiter = { acquire: () => null };
  const token = JSON.stringify({
    unit: 'sat',
    mint: 'https://testnut.cashu.space',
    proofs: [{ amount: 21, id: '00ad268c4d1f5826', secret: 's', C: '02aa' }],
  });
  const printed: string[] = [];
  const original = console.log;
  console.log = (line: unknown) => {
    printed.push(String(line));
  };
  try {
    const result = await r.redeem(token, '203.0.113.9');
    assert.equal(result.outcome, 'rate_limited');
  } finally {
    console.log = original;
  }
  const line = printed.map((l) => JSON.parse(l)).find((l) => l.msg === 'rate limited');
  assert.ok(line, 'no rate-limited line');
  assert.deepEqual(Object.keys(line).sort(), ['id', 'level', 'msg', 'service', 'ts']);
  assert.ok(!printed.join('\n').includes('203.0.113.9'));
});

test('rate limiter: concurrency, global window, per-ip window', () => {
  const rl = new RateLimiter(2, 4, 2);
  const t0 = 1_000_000_000_000; // fixed epoch; window math is minute-bucketed
  const a = rl.acquire('1.1.1.1', t0);
  const b = rl.acquire('2.2.2.2', t0);
  assert.ok(a && b);
  // concurrency cap of 2 bites
  assert.equal(rl.acquire('3.3.3.3', t0), null);
  a!();
  // released slot frees concurrency; per-ip cap: second call for 1.1.1.1 ok...
  const c = rl.acquire('1.1.1.1', t0);
  assert.ok(c);
  c!();
  // ...third for 1.1.1.1 in the same minute refused (per-ip cap 2)
  assert.equal(rl.acquire('1.1.1.1', t0), null);
  // global cap: 3 grants so far, one more allowed, then refused
  const d = rl.acquire('4.4.4.4', t0);
  assert.ok(d);
  d!();
  assert.equal(rl.acquire('5.5.5.5', t0), null);
  // a new minute resets both windows
  const e = rl.acquire('1.1.1.1', t0 + 60_001);
  assert.ok(e);
  e!();
  // double release must not free two slots
  b!();
  b!();
  const f = rl.acquire('6.6.6.6', t0 + 60_001);
  const g = rl.acquire('7.7.7.7', t0 + 60_001);
  assert.ok(f && g);
  assert.equal(rl.acquire('8.8.8.8', t0 + 60_001), null);
});

// ---------------------------------------------------------------- store

test('FileStore: append-only (second put of the same key fails), traversal refused', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'cashu-store-'));
  const store = new FileStore(dir);
  await store.put('abc/outputs.json', '{"v":1}');
  assert.equal(await store.get('abc/outputs.json'), '{"v":1}');
  await assert.rejects(store.put('abc/outputs.json', '{"v":2}'));
  assert.equal(await store.get('abc/outputs.json'), '{"v":1}'); // unchanged
  assert.equal(await store.get('missing/outputs.json'), null);
  await assert.rejects(store.put('../escape.json', '{}'), /escapes/);
  // and the escape attempt wrote nothing outside the dir
  await assert.rejects(readFile(join(dir, '..', 'escape.json')));
});

// ---------------------------------------------------------------- receipts

test('tokenHash: sha256 over the trimmed credential, hex', () => {
  // Pinned vector - the Python side (nostrology_mcp) computes the same hash
  // for its receipt line; its test pins this exact vector too, which is what
  // keeps the two sides' receipt references joinable.
  assert.equal(
    tokenHash('  cashuBpin-vector  '),
    'e393fa9463b78c9ddb52391fb1bcad7ee40d862ec3722698bb048577b35e376a',
  );
});

// ---------------------------------------------------------------- metrics

test('metrics exposition is valid Prometheus text with zero-valued families', () => {
  const text = exposeMetrics();
  assert.match(text, /# TYPE cashu_redeem_requests_total counter/);
  assert.match(text, /# TYPE cashu_delivery_failed_total counter/);
  // families with no increments still expose a 0 sample (absent series read as
  // "never scraped" in the alert rules otherwise)
  assert.match(text, /cashu_delivery_failed_total 0/);
  assert.ok(text.endsWith('\n'));
});

test('labeled alert-feeding series are PREALLOCATED at zero', () => {
  // Review finding: a labeled series
  // born at 1 on its first increment gives increase() nothing to count, so
  // the FIRST persist failure - the exact event the alert exists for - would
  // never fire it. Every closed label set is exposed at 0 from process start.
  const text = exposeMetrics();
  for (const phase of ['outputs', 'signatures', 'outcome']) {
    assert.match(text, new RegExp(`cashu_persist_failures_total\\{phase="${phase}"\\} \\d`));
  }
  for (const reason of [
    'not_a_token',
    'tokenv3_unsupported',
    'payment_request_not_token',
    'unit_mismatch',
    'mint_not_allowlisted',
    'unknown_keyset',
    'duplicate_proofs',
    'amount_mismatch',
  ]) {
    assert.match(text, new RegExp(`cashu_rejected_structural_total\\{reason="${reason}"\\} \\d`));
  }
  assert.match(text, /cashu_mint_rejected_total \d/);
});
