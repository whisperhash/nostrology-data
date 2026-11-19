// Test helpers against https://testnut.cashu.space - the FakeWallet mint whose
// /v1/info says "all your Lightning invoices will always be marked paid", which
// is what lets this suite mint real tokens on every run with no sats and no
// manual step. It is a BETTER fixture than
// Minibits, not a cheaper one: its sat keyset carries input_fee_ppk=100, so the
// fee arithmetic actually executes here and a rounding bug is caught in CI
// rather than shipped invisible against Minibits' fee of 0.

import {
  Mint,
  OutputData,
  getEncodedToken,
  type MintKeys,
  type Proof,
} from '@cashu/cashu-ts';
import { nut02Fee } from '../src/mints.ts';

export const TESTNUT = 'https://testnut.cashu.space';

export interface TestMint {
  mint: Mint;
  keys: MintKeys;
  feePpk: number;
}

const cache = new Map<string, TestMint>();

/** Load the active keyset (with keys) for a unit on testnut. */
export async function testMint(unit = 'sat'): Promise<TestMint> {
  const hit = cache.get(unit);
  if (hit) return hit;
  const mint = new Mint(TESTNUT);
  await mint.getLazyMintInfo();
  const { keysets } = await mint.getKeySets();
  const active = keysets.find((k) => k.active && k.unit === unit);
  if (!active) throw new Error(`testnut has no active ${unit} keyset`);
  const { keysets: keys } = await mint.getKeys(active.id);
  const withKeys = keys.find((k) => k.id === active.id);
  if (!withKeys) throw new Error(`testnut returned no keys for ${active.id}`);
  const loaded = { mint, keys: withKeys, feePpk: active.input_fee_ppk ?? 0 };
  cache.set(unit, loaded);
  return loaded;
}

/** Mint `amount` into fresh proofs via the FakeWallet bolt11 flow. */
export async function mintProofs(amount: number, unit = 'sat'): Promise<Proof[]> {
  const { mint, keys } = await testMint(unit);
  const quote = await mint.createMintQuoteBolt11({ amount, unit });
  // FakeWallet pays instantly, but poll a few times in case it is async.
  let state = quote.state;
  for (let i = 0; i < 20 && state !== 'PAID' && state !== 'ISSUED'; i++) {
    await new Promise((r) => setTimeout(r, 500));
    state = (await mint.checkMintQuoteBolt11(quote.quote)).state;
  }
  if (state !== 'PAID') throw new Error(`testnut quote never became PAID (state=${state})`);
  const outputs = OutputData.createRandomData(amount, keys);
  const res = await mint.mintBolt11({
    quote: quote.quote,
    outputs: outputs.map((o) => o.blindedMessage),
  });
  return res.signatures.map((sig, i) => outputs[i]!.toProof(sig, keys));
}

/** Mint a token whose value is EXACTLY price + NUT-02 fee for its own proof
 * count - the fixpoint iteration a real payer's wallet performs when asked for
 * exact-amount + fees. */
export async function mintExactPaymentToken(price: number, unit = 'sat'): Promise<string> {
  const { keys, feePpk } = await testMint(unit);
  // Fixpoint: guess a fee, split price+fee into denominations on this keyset,
  // recompute the fee for that proof count, repeat until stable.
  let fee = 0;
  let split: number[] = [];
  for (let i = 0; i < 10; i++) {
    split = powersOfTwoSplit(price + fee);
    const nextFee = nut02Fee(split.map(() => feePpk));
    if (nextFee === fee) break;
    fee = nextFee;
  }
  const proofs = await mintProofsWithSplit(price + fee, split, keys);
  return getEncodedToken({ mint: TESTNUT, unit, proofs });
}

function powersOfTwoSplit(amount: number): number[] {
  const parts: number[] = [];
  for (let bit = 1; bit <= amount; bit <<= 1) {
    if (amount & bit) parts.push(bit);
  }
  return parts;
}

async function mintProofsWithSplit(amount: number, split: number[], keys: MintKeys): Promise<Proof[]> {
  const { mint } = await testMint(keys.unit);
  const quote = await mint.createMintQuoteBolt11({ amount, unit: keys.unit });
  let state = quote.state;
  for (let i = 0; i < 20 && state !== 'PAID' && state !== 'ISSUED'; i++) {
    await new Promise((r) => setTimeout(r, 500));
    state = (await mint.checkMintQuoteBolt11(quote.quote)).state;
  }
  if (state !== 'PAID') throw new Error(`testnut quote never became PAID (state=${state})`);
  const outputs = OutputData.createRandomData(amount, keys, split);
  const res = await mint.mintBolt11({
    quote: quote.quote,
    outputs: outputs.map((o) => o.blindedMessage),
  });
  return res.signatures.map((sig, i) => outputs[i]!.toProof(sig, keys));
}
