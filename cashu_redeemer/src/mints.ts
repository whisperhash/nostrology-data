// Per-mint session: one cashu-ts Mint client + the keyset knowledge the
// structural checks need (ALL units, not just ours - a distinct unit_mismatch
// rejection requires knowing that a foreign keyset exists), cached per process
// with an explicit refresh path for the unknown-keyset retry.
//
// NUT-19 replay note, verified against the vendored cashu-ts 4.8.0 source:
// once `getLazyMintInfo()` has run, `Mint.requestWithAuth` passes the mint's
// advertised `{ttl, cached_endpoints}` (NUT-06 nuts.19) into the request
// layer, whose retry loop re-sends the BYTE-IDENTICAL body (same serialized
// requestBody) with exponential backoff on NetworkError/5xx, up to 9 retries
// bounded by the ttl. So the in-process half of the swap's recovery
// ladder - retrying the byte-identical swap request - is the library's own
// maintained implementation; loading mint info at session start is what turns
// it on, and is therefore NOT an optimization.

import {
  Mint,
  OutputData,
  type CheckStateResponse,
  type MintKeys,
  type MintKeyset,
  type Proof,
  type PostRestoreResponse,
  type SerializedBlindedMessage,
  type SwapResponse,
} from '@cashu/cashu-ts';

/** NUT-02's fee rule (nuts/02.md:43-53): sum input_fee_ppk PER PROOF, then
 * the canonical integer expression (sum + 999) // 1000 - deliberately NOT the
 * float form ceil(sum/1000), which NUT-02 itself warns against "due to the
 * non-deterministic behavior of floating point division". */
export function nut02Fee(perProofPpk: readonly number[]): number {
  const sumPpk = perProofPpk.reduce((a, b) => a + b, 0);
  return Math.floor((sumPpk + 999) / 1000);
}

export class MintSession {
  readonly mint: Mint;
  readonly url: string;
  readonly unit: string;
  private keysetsById = new Map<string, MintKeyset>();
  private activeKeys: MintKeys | null = null;

  constructor(url: string, unit: string) {
    this.url = url;
    this.mint = new Mint(url);
    this.unit = unit;
  }

  /** Fetch mint info (enables NUT-19 replay - see module header), all keysets
   * (all units), and the keys of the active keyset our outputs will use. */
  async load(): Promise<void> {
    await this.mint.getLazyMintInfo();
    await this.refreshKeysets();
  }

  async refreshKeysets(): Promise<void> {
    const { keysets } = await this.mint.getKeySets();
    this.keysetsById = new Map(keysets.map((k) => [k.id, k]));
    const active = keysets
      .filter((k) => k.active && k.unit === this.unit)
      // Prefer the cheapest active keyset for our outputs; ties broken by id
      // for determinism.
      .sort((a, b) => (a.input_fee_ppk ?? 0) - (b.input_fee_ppk ?? 0) || a.id.localeCompare(b.id));
    const chosen = active[0];
    if (!chosen) throw new Error(`mint ${this.url} advertises no active ${this.unit} keyset`);
    const { keysets: keys } = await this.mint.getKeys(chosen.id);
    const withKeys = keys.find((k) => k.id === chosen.id);
    if (!withKeys) throw new Error(`mint ${this.url} did not return keys for keyset ${chosen.id}`);
    this.activeKeys = withKeys;
  }

  allKeysetIds(): string[] {
    return [...this.keysetsById.keys()];
  }

  /** Keys (with public keys) for a keyset id, fetching + caching on miss.
   * Load-bearing for the resume path (review finding): outputs persisted
   * before a crash were blinded under THAT day's active keyset; unblinding
   * them with whatever keyset is active at resume time mis-keys the unblind
   * and throws inside OutputData.toProof. */
  private keysCache = new Map<string, MintKeys>();

  async keysFor(id: string): Promise<MintKeys> {
    if (this.activeKeys?.id === id) return this.activeKeys;
    const cached = this.keysCache.get(id);
    if (cached) return cached;
    const { keysets } = await this.mint.getKeys(id);
    const withKeys = keysets.find((k) => k.id === id);
    if (!withKeys) throw new Error(`mint ${this.url} did not return keys for keyset ${id}`);
    this.keysCache.set(id, withKeys);
    return withKeys;
  }

  /** The unit of a keyset id, or null if this mint does not publish it. */
  keysetUnit(id: string): string | null {
    return this.keysetsById.get(id)?.unit ?? null;
  }

  /** NUT-02 input fee for these proofs at this mint. Callers must have
   * resolved every keyset first (verifyAgainstMint does); an unknown id here
   * is a programming error. */
  feeForProofs(proofs: readonly Pick<Proof, 'id'>[]): number {
    const ppks = proofs.map((proof) => {
      const keyset = this.keysetsById.get(proof.id);
      if (!keyset) throw new Error(`feeForProofs: unresolved keyset ${proof.id}`);
      return keyset.input_fee_ppk ?? 0;
    });
    return nut02Fee(ppks);
  }

  /** Fresh random blinded outputs for `amount` on the active keyset - the
   * (secret, r, B_) triples that must be persisted BEFORE the swap. */
  createOutputs(amount: number): OutputData[] {
    if (!this.activeKeys) throw new Error('MintSession not loaded');
    return OutputData.createRandomData(amount, this.activeKeys);
  }

  /** Unblind ONE signature against the keyset the output was blinded under
   * (carried in its blinded message), NOT the currently-active keyset - the
   * two differ after a rotation, and toProof both unblinds and DLEQ-verifies
   * against whatever keys it is given. */
  async toProof(output: OutputData, signature: SwapResponse['signatures'][number]): Promise<Proof> {
    return output.toProof(signature, await this.keysFor(output.blindedMessage.id));
  }

  swap(inputs: Proof[], outputs: SerializedBlindedMessage[]): Promise<SwapResponse> {
    return this.mint.swap({ inputs, outputs });
  }

  restore(outputs: SerializedBlindedMessage[]): Promise<PostRestoreResponse> {
    return this.mint.restore({ outputs });
  }

  check(Ys: string[]): Promise<CheckStateResponse> {
    return this.mint.check({ Ys });
  }
}

/** Session cache: one session per canonical mint URL, loaded on first use.
 * A load failure is NOT cached - the next request retries. */
export class MintSessions {
  private sessions = new Map<string, MintSession>();
  private readonly unit: string;
  constructor(unit: string) {
    this.unit = unit;
  }

  async get(canonicalUrl: string): Promise<MintSession> {
    const existing = this.sessions.get(canonicalUrl);
    if (existing) return existing;
    const session = new MintSession(canonicalUrl, this.unit);
    await session.load();
    this.sessions.set(canonicalUrl, session);
    return session;
  }
}
