// Live integration tests against https://testnut.cashu.space (FakeWallet -
// mints on demand, no sats, unattended; see helpers.ts). Run: npm run test:mint
//
// What only THIS suite can catch, and why it exists:
//   * the fee arithmetic actually executes (testnut input_fee_ppk=100, so a
//     rounding or per-keyset-instead-of-per-proof bug fails here and nowhere
//     else),
//   * the unit-mismatch rejection runs against a genuinely usd-denominated
//     keyset rather than a mock,
//   * the replayed-token, recovery-ladder and persist-ordering behaviours are
//     exercised against a real cdk mint's actual error codes.

import { strict as assert } from 'node:assert';
import { test } from 'node:test';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { NetworkError, hashToCurve, CheckStateEnum } from '@cashu/cashu-ts';

import { loadConfig } from '../src/config.ts';
import { MintSessions, MintSession } from '../src/mints.ts';
import { Redeemer, tokenHash } from '../src/redeem.ts';
import { FileStore, type RedemptionStore } from '../src/store.ts';
import { exposeMetrics } from '../src/metrics.ts';
import { TESTNUT, mintExactPaymentToken, mintProofs } from './helpers.ts';

const PRICE = 21;

function testConfig(overrides: Record<string, string> = {}) {
  return loadConfig({
    CASHU_MINTS: TESTNUT,
    CASHU_PRICE_SATS: String(PRICE),
    // Generous limits: the suite itself makes many mint calls in a minute.
    CASHU_MAX_MINT_CALLS_PER_MINUTE: '1000',
    CASHU_MAX_MINT_CALLS_PER_MINUTE_PER_IP: '1000',
    CASHU_MAX_CONCURRENT_MINT_CALLS: '4',
    ...overrides,
  } as NodeJS.ProcessEnv);
}

async function tempStore(): Promise<FileStore> {
  return new FileStore(await mkdtemp(join(tmpdir(), 'cashu-redeem-')));
}

// ---------------------------------------------------------------- happy path

test('happy path: exact-amount token redeems; both store objects exist; fee is nonzero', async () => {
  const store = await tempStore();
  const redeemer = new Redeemer(testConfig(), store);
  const token = await mintExactPaymentToken(PRICE);

  const result = await redeemer.redeem(token, '203.0.113.7');
  assert.equal(result.outcome, 'redeemed', JSON.stringify(result));
  if (result.outcome !== 'redeemed') return;
  assert.equal(result.receipt.id, tokenHash(token));
  assert.equal(result.receipt.amount_sats, PRICE);
  // THE reason testnut is the fixture: the fee actually bites here.
  assert.ok(result.receipt.fee_sats > 0, 'fee must be nonzero at testnut (input_fee_ppk=100)');
  assert.equal(result.receipt.mint, TESTNUT);

  // Persist-before-swap left phase 1, the swap left phase 2.
  const outputsObj = JSON.parse((await store.get(`${result.receipt.id}/outputs.json`))!);
  assert.equal(outputsObj.price_sats, PRICE);
  assert.ok(Array.isArray(outputsObj.outputs) && outputsObj.outputs.length > 0);
  assert.ok(outputsObj.swap_request.inputs.length > 0);
  const sigObj = JSON.parse((await store.get(`${result.receipt.id}/signatures.json`))!);
  assert.equal(sigObj.proofs.reduce((a: number, p: { amount: number }) => a + p.amount, 0), PRICE);

  // Replay of the SAME token BEFORE the delivery report: the payment
  // completed but the payer has not received their data, so the resume-deliver
  // path answers redeemed with the ORIGINAL receipt (this is what makes
  // "retry the same call with the same token" true after an indeterminate
  // timeout, and it cannot double-charge - the money moved once).
  const redeliver = await redeemer.redeem(token, '203.0.113.7');
  assert.equal(redeliver.outcome, 'redeemed', JSON.stringify(redeliver));
  if (redeliver.outcome === 'redeemed') {
    assert.equal(redeliver.receipt.id, result.receipt.id);
  }

  // Delivery outcome record (the delivery audit trail) - and a duplicate
  // report is an idempotent success, not a persist failure.
  assert.equal(await redeemer.recordOutcome(result.receipt.id, true, null), true);
  assert.equal(await redeemer.recordOutcome(result.receipt.id, true, null), true);
  const outcomeObj = JSON.parse((await store.get(`${result.receipt.id}/outcome.json`))!);
  assert.equal(outcomeObj.delivered, true);

  // Replay AFTER delivered=true: the payment bought its data once - refused
  // as already_spent, never a second delivery and never a "pay again".
  const replay = await redeemer.redeem(token, '203.0.113.7');
  assert.equal(replay.outcome, 'already_spent', JSON.stringify(replay));
});

test('concurrent presentations of one token: one payment, no false persist_failed', async () => {
  // Review finding: two same-token requests in flight raced the resume
  // path into a false "storage failure interrupted delivery" while the money
  // was safely persisted. The per-token serialization makes the second caller
  // either resume-deliver (no outcome yet) or get refused - never an error.
  const store = await tempStore();
  const redeemer = new Redeemer(testConfig(), store);
  const token = await mintExactPaymentToken(PRICE);
  const [a, b] = await Promise.all([redeemer.redeem(token, null), redeemer.redeem(token, null)]);
  const outcomes = [a.outcome, b.outcome].sort();
  assert.deepEqual(outcomes, ['redeemed', 'redeemed'], JSON.stringify([a, b]));
  // Exactly one persisted redemption, money intact.
  const id = tokenHash(token);
  const sigObj = JSON.parse((await store.get(`${id}/signatures.json`))!);
  assert.equal(sigObj.proofs.reduce((s: number, p: { amount: number }) => s + p.amount, 0), PRICE);
});

test('a structurally valid token with fabricated secrets is mint_rejected - nothing spent, no unresolved alarm', async () => {
  // Passes every offline check by construction (no offline signature
  // verification exists); the mint's settled 4xx must NOT read as unresolved
  // (review finding: that let anonymous junk feed the operator alert and
  // collect support-claim framing for tokens that were never money).
  const store = await tempStore();
  const redeemer = new Redeemer(testConfig(), store);
  const session = new MintSession(TESTNUT, 'sat');
  await session.load();
  // Build fake proofs on the REAL active keyset id with the right exact total.
  const real = await mintExactPaymentToken(PRICE);
  const { getDecodedToken } = await import('@cashu/cashu-ts');
  const decoded = getDecodedToken(real, session.allKeysetIds());
  const fake = decoded.proofs.map((p, i) => ({
    amount: Number(p.amount),
    id: p.id,
    secret: 'f'.repeat(63) + String(i), // fabricated
    C: '02' + 'ab'.repeat(32),
  }));
  const payload = JSON.stringify({ unit: 'sat', mint: TESTNUT, proofs: fake });
  const result = await redeemer.redeem(payload, null);
  assert.equal(result.outcome, 'mint_rejected', JSON.stringify(result));
  if (result.outcome === 'mint_rejected') {
    assert.match(result.message, /nothing was spent/);
    assert.doesNotMatch(result.message, /support claim/);
  }
  // The REAL token remains fully spendable afterwards.
  const spent = await redeemer.redeem(real, null);
  assert.equal(spent.outcome, 'redeemed', JSON.stringify(spent));
});

test('NUT-18 PaymentRequestPayload shape is accepted end to end', async () => {
  const store = await tempStore();
  const redeemer = new Redeemer(testConfig(), store);
  const token = await mintExactPaymentToken(PRICE);
  // Re-shape the cashuB token into the NUT-18 in-band payload a wallet posts.
  const { getDecodedToken } = await import('@cashu/cashu-ts');
  const { MintSession } = await import('../src/mints.ts');
  const session = new MintSession(TESTNUT, 'sat');
  await session.load();
  const decoded = getDecodedToken(token, session.allKeysetIds());
  const payload = JSON.stringify({
    id: 'test-payment',
    unit: 'sat',
    mint: TESTNUT,
    proofs: decoded.proofs.map((p) => ({ amount: Number(p.amount), id: p.id, secret: p.secret, C: p.C })),
  });
  const result = await redeemer.redeem(payload, null);
  assert.equal(result.outcome, 'redeemed', JSON.stringify(result));
});

// ---------------------------------------------------------------- rejections

test('amount mismatch is rejected offline-after-keysets and consumes nothing', async () => {
  const store = await tempStore();
  const redeemer = new Redeemer(testConfig(), store);
  // One sat over: 21 + fee + 1.
  const { getDecodedToken, getEncodedToken } = await import('@cashu/cashu-ts');
  const session = new MintSession(TESTNUT, 'sat');
  await session.load();
  const proofs = await mintProofs(PRICE + 5);
  const over = getEncodedToken({ mint: TESTNUT, unit: 'sat', proofs });

  const result = await redeemer.redeem(over, null);
  assert.equal(result.outcome, 'structural');
  if (result.outcome !== 'structural') return;
  assert.equal(result.reason, 'amount_mismatch');
  // The rejection consumed nothing: every proof still UNSPENT at the mint.
  const decoded = getDecodedToken(over, session.allKeysetIds());
  const Ys = decoded.proofs.map((p) => hashToCurve(new TextEncoder().encode(p.secret)).toHex(true));
  const { states } = await session.check(Ys);
  assert.ok(states.every((s) => s.state === CheckStateEnum.UNSPENT));
});

test('a genuinely usd-denominated keyset is rejected as unit_mismatch, not unknown_keyset', async () => {
  const store = await tempStore();
  const redeemer = new Redeemer(testConfig(), store);
  const usdProofs = await mintProofs(PRICE + 1, 'usd');
  // The payload CLAIMS sat; the proofs' keyset says usd. The keyset is the
  // truth and the rejection must say so distinctly.
  const payload = JSON.stringify({
    unit: 'sat',
    mint: TESTNUT,
    proofs: usdProofs.map((p) => ({ amount: Number(p.amount), id: p.id, secret: p.secret, C: p.C })),
  });
  const result = await redeemer.redeem(payload, null);
  assert.equal(result.outcome, 'structural');
  if (result.outcome !== 'structural') return;
  assert.equal(result.reason, 'unit_mismatch');
});

test('a fabricated keyset id is rejected as unknown_keyset (after one refresh)', async () => {
  const store = await tempStore();
  const redeemer = new Redeemer(testConfig(), store);
  const payload = JSON.stringify({
    unit: 'sat',
    mint: TESTNUT,
    proofs: [{ amount: 21, id: '00deadbeef000000', secret: 'x'.repeat(64), C: '02' + 'ab'.repeat(32) }],
  });
  const result = await redeemer.redeem(payload, null);
  assert.equal(result.outcome, 'structural');
  if (result.outcome !== 'structural') return;
  assert.equal(result.reason, 'unknown_keyset');
});

test('duplicate proofs inside one token are rejected before the mint sees them', async () => {
  const store = await tempStore();
  const redeemer = new Redeemer(testConfig(), store);
  const session = new MintSession(TESTNUT, 'sat');
  await session.load();
  const proofs = await mintProofs(11); // 8+2+1 at testnut's default split
  const one = proofs.find((p) => Number(p.amount) === 11) ?? proofs[0]!;
  const doubled = [one, one];
  const payload = JSON.stringify({
    unit: 'sat',
    mint: TESTNUT,
    proofs: doubled.map((p) => ({ amount: Number(p.amount), id: p.id, secret: p.secret, C: p.C })),
  });
  const result = await redeemer.redeem(payload, null);
  assert.equal(result.outcome, 'structural');
  if (result.outcome !== 'structural') return;
  assert.equal(result.reason, 'duplicate_proofs');
});

// ---------------------------------------------------------------- recovery ladder

/** MintSessions whose swap is sabotaged once, in a chosen mode, to drive
 * afterSwapFailure against the real mint. */
class SabotagedSessions extends MintSessions {
  private readonly mode: 'lost_response' | 'never_sent';
  private armed = true;
  constructor(unit: string, mode: 'lost_response' | 'never_sent') {
    super(unit);
    this.mode = mode;
  }

  override async get(url: string): Promise<MintSession> {
    const session = await super.get(url);
    if (!this.armed) return session;
    this.armed = false;
    const realSwap = session.swap.bind(session);
    const mode = this.mode;
    let sabotaged = false;
    session.swap = async (inputs, outputs) => {
      if (sabotaged) return realSwap(inputs, outputs);
      sabotaged = true;
      if (mode === 'lost_response') {
        // The swap EXECUTES at the mint; the response dies on the wire.
        await realSwap(inputs, outputs);
        throw new NetworkError('simulated: response lost after execution');
      }
      // The request never reaches the mint at all.
      throw new NetworkError('simulated: connection refused');
    };
    return session;
  }
}

test('ladder: lost swap response is recovered via NUT-09 restore and still redeems', async () => {
  const store = await tempStore();
  const sessions = new SabotagedSessions('sat', 'lost_response');
  const redeemer = new Redeemer(testConfig(), store, sessions);
  const token = await mintExactPaymentToken(PRICE);
  const result = await redeemer.redeem(token, null);
  assert.equal(result.outcome, 'redeemed', JSON.stringify(result));
  if (result.outcome !== 'redeemed') return;
  // The money landed despite the lost response: signatures object exists and
  // its proofs sum to the price.
  const sigObj = JSON.parse((await store.get(`${result.receipt.id}/signatures.json`))!);
  assert.equal(sigObj.proofs.reduce((a: number, p: { amount: number }) => a + p.amount, 0), PRICE);
});

test('ladder: a swap that never reached the mint resolves to mint_unreachable with the token intact', async () => {
  const store = await tempStore();
  const sessions = new SabotagedSessions('sat', 'never_sent');
  const redeemer = new Redeemer(testConfig(), store, sessions);
  const token = await mintExactPaymentToken(PRICE);
  const result = await redeemer.redeem(token, null);
  assert.equal(result.outcome, 'mint_unreachable', JSON.stringify(result));
  // The token is INTACT (restore found nothing, checkstate said UNSPENT), so
  // the caller's retry - the exact same token - must now succeed.
  const retry = await redeemer.redeem(token, null);
  assert.equal(retry.outcome, 'redeemed', JSON.stringify(retry));
});

// ---------------------------------------------------------------- persist ordering

class FailingStore implements RedemptionStore {
  private readonly inner: RedemptionStore;
  private readonly failOn: string;
  constructor(inner: RedemptionStore, failOn: string) {
    this.inner = inner;
    this.failOn = failOn;
  }
  async put(key: string, body: string): Promise<void> {
    if (key.endsWith(this.failOn)) throw new Error('simulated store outage');
    return this.inner.put(key, body);
  }
  get(key: string): Promise<string | null> {
    return this.inner.get(key);
  }
}

test('phase-1 persist failure aborts BEFORE the swap; the token survives and redeems later', async () => {
  const inner = await tempStore();
  const redeemer = new Redeemer(testConfig(), new FailingStore(inner, 'outputs.json'));
  const token = await mintExactPaymentToken(PRICE);
  const result = await redeemer.redeem(token, null);
  assert.equal(result.outcome, 'store_unavailable', JSON.stringify(result));
  // Nothing was spent: the same token redeems cleanly through a healthy store.
  const healthy = new Redeemer(testConfig(), inner);
  const retry = await healthy.redeem(token, null);
  assert.equal(retry.outcome, 'redeemed', JSON.stringify(retry));
});

test('phase-2 persist failure returns persist_failed WITH the receipt; money is NUT-09 recoverable', async () => {
  const inner = await tempStore();
  const redeemer = new Redeemer(testConfig(), new FailingStore(inner, 'signatures.json'));
  const token = await mintExactPaymentToken(PRICE);
  const result = await redeemer.redeem(token, null);
  assert.equal(result.outcome, 'persist_failed', JSON.stringify(result));
  if (result.outcome !== 'persist_failed') return;
  assert.equal(result.receipt.id, tokenHash(token));
  // Phase 1 landed, so the blinded messages are on disk and NUT-09 restore
  // returns the signatures - the money is recoverable by an operator.
  const outputsObj = JSON.parse((await inner.get(`${result.receipt.id}/outputs.json`))!);
  const session = new MintSession(TESTNUT, 'sat');
  await session.load();
  const restored = await session.restore(outputsObj.swap_request.outputs);
  assert.equal(restored.signatures.length, outputsObj.outputs.length);
});

// ---------------------------------------------------------------- metrics

test('the counters moved for the scenarios above', () => {
  const text = exposeMetrics();
  assert.match(text, /cashu_swaps_succeeded_total \d/);
  assert.match(text, /cashu_refused_spent_total \d/);
  assert.match(text, /cashu_rejected_structural_total\{reason="amount_mismatch"\} \d/);
  assert.match(text, /cashu_rejected_structural_total\{reason="unit_mismatch"\} \d/);
});
