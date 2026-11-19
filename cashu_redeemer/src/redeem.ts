// The redemption core - the paid tool's redemption flow, implemented in this
// exact order and with this failure taxonomy:
//
//   1. structural checks, no network      -> outcome "structural"
//   2. rate-limit gate                    -> outcome "rate_limited"
//   3. mint session (info + keysets)      -> outcome "mint_unreachable"
//   4. PERSIST outputs BEFORE the swap    -> outcome "store_unavailable" (nothing spent)
//   5. NUT-03 swap (NUT-19 replay inside) -> "already_spent" | ladder below
//   6. persist the returned signatures    -> outcome "persist_failed" (paid; NUT-09 recoverable)
//   7. return the receipt                 -> outcome "redeemed"
//
// Ladder for a swap that failed ambiguously (network death AFTER the library's
// own NUT-19 replay window): NUT-09 restore against our persisted
// blinded messages; if the mint signed them, the money is ours and we resume
// at step 6. Else NUT-07 checkstate on the INPUTS: all UNSPENT = the swap
// never executed (token intact, caller may retry -> "mint_unreachable");
// anything SPENT with our restore empty = consumed by someone else ->
// "already_spent"; anything else -> "unresolved": this sidecar answers the MCP
// server 503 (service unavailable), and the caller must NOT pay again.
//
// Log-safety contract: no branch below ever logs or returns token text, proof
// secrets, or signatures - identifiers are the sha256 token hash and counts.

import { createHash } from 'node:crypto';
import {
  MintOperationError,
  NetworkError,
  HttpResponseError,
  OutputData,
  hashToCurve,
  CheckStateEnum,
  type Proof,
} from '@cashu/cashu-ts';
import { canonicalMintUrl } from './canonical.ts';
import type { Config } from './config.ts';
import { log } from './log.ts';
import { metrics } from './metrics.ts';
import { MintSessions, type MintSession } from './mints.ts';
import { RateLimiter } from './ratelimit.ts';
import { isAlreadyExists, type RedemptionStore } from './store.ts';
import { precheck, verifyAgainstMint, StructuralError } from './verify.ts';

// cashubtc/nuts error_codes.md, the two codes this flow must distinguish:
//   11001 "Token is already spent"
//   10002 "Blinded message of output already signed" - our own outputs were
//         signed by a swap whose response we lost; the restore ladder recovers.
const CODE_TOKEN_ALREADY_SPENT = 11001;
const CODE_OUTPUT_ALREADY_SIGNED = 10002;

export interface Receipt {
  /** sha256 hex of the exact credential string as received (trimmed) - the
   * reference a caller quotes in a support claim, and the join key to the
   * redemption store. The MCP server computes the same hash for its receipt
   * line; the same vector is pinned on both sides (the tokenHash test in
   * test/unit.test.ts and test_token_hash_pins_the_sidecar_vector in the
   * Python suite). NOTE the hash is over the raw STRING: the same proofs
   * re-sent in the other accepted shape (cashuB vs PaymentRequestPayload)
   * hash differently, so recovery guidance always says to re-present the
   * token byte-identically. */
  id: string;
  mint: string;
  amount_sats: number;
  fee_sats: number;
  ts: string;
}

export type RedeemResult =
  | { outcome: 'redeemed'; receipt: Receipt }
  | { outcome: 'structural'; reason: string; message: string }
  | { outcome: 'rate_limited'; message: string }
  | { outcome: 'mint_unreachable'; message: string }
  | { outcome: 'already_spent'; message: string; id: string }
  | { outcome: 'mint_rejected'; message: string; id: string }
  | { outcome: 'store_unavailable'; message: string }
  | { outcome: 'persist_failed'; message: string; receipt: Receipt }
  | { outcome: 'unresolved'; message: string; id: string };

export function tokenHash(raw: string): string {
  return createHash('sha256').update(raw.trim(), 'utf8').digest('hex');
}

// Wire-canonical normalizers. cashu-ts's Proof/SerializedBlindedSignature
// carry `Amount` value objects whose toJSON() renders a STRING; NUT-00 wire
// JSON (and therefore everything we persist for replay/restore) needs plain
// u64 numbers. Key order is fixed here so a re-serialization is byte-stable.
function wireProof(p: Proof): Proof {
  const out: Record<string, unknown> = {
    amount: Number(p.amount),
    id: p.id,
    secret: p.secret,
    C: p.C,
  };
  if (p.witness !== undefined) out.witness = p.witness;
  if (p.dleq !== undefined) out.dleq = p.dleq;
  return out as unknown as Proof;
}

type WireBlindedMessage = { amount: number; id: string; B_: string };
function wireBlindedMessage(m: { amount: unknown; id: string; B_: string }): WireBlindedMessage {
  return { amount: Number(m.amount), id: m.id, B_: m.B_ };
}

function wireSignature(s: { id: string; amount: unknown; C_: string; dleq?: unknown }): Record<string, unknown> {
  const out: Record<string, unknown> = { id: s.id, amount: Number(s.amount), C_: s.C_ };
  if (s.dleq !== undefined) out.dleq = s.dleq;
  return out;
}

function isMintDown(e: unknown): boolean {
  return e instanceof NetworkError || (e instanceof HttpResponseError && e.status >= 500);
}

export class Redeemer {
  private readonly sessions: MintSessions;
  private readonly limiter: RateLimiter;
  private readonly config: Config;
  private readonly store: RedemptionStore;
  // Per-token-hash serialization (review finding: two same-token requests
  // in flight race the resume path - B resumes A's persisted attempt, both
  // swap via the mint's NUT-19 cache, and the loser's signatures.json put
  // read as a storage outage). Node is single-process, so an in-memory chain
  // per hash removes the race entirely; the store-level append-only contract
  // stays as the cross-process backstop.
  private readonly perToken = new Map<string, Promise<RedeemResult>>();

  // `sessions` is injectable for the fault-injection tests (a session whose
  // swap fails once, to drive the recovery ladder against a real mint).
  constructor(config: Config, store: RedemptionStore, sessions?: MintSessions) {
    this.config = config;
    this.store = store;
    this.sessions = sessions ?? new MintSessions(config.unit);
    this.limiter = new RateLimiter(
      config.maxConcurrentMintCalls,
      config.maxMintCallsPerMinute,
      config.maxMintCallsPerMinutePerIp,
    );
  }

  async redeem(rawCredential: string, clientIp: string | null): Promise<RedeemResult> {
    const id = tokenHash(rawCredential);
    const prev = this.perToken.get(id);
    const run =
      prev === undefined
        ? this.redeemUnserialized(rawCredential, clientIp, id)
        : prev.then(
            () => this.redeemUnserialized(rawCredential, clientIp, id),
            () => this.redeemUnserialized(rawCredential, clientIp, id),
          );
    this.perToken.set(id, run);
    void run.finally(() => {
      if (this.perToken.get(id) === run) this.perToken.delete(id);
    });
    return run;
  }

  private async redeemUnserialized(
    rawCredential: string,
    clientIp: string | null,
    id: string,
  ): Promise<RedeemResult> {
    metrics.redeemRequests.inc();

    // 1. Offline structural checks (shape, version, unit, allowlist).
    let parsed;
    try {
      parsed = precheck(rawCredential, this.config.unit, this.config.mints);
    } catch (e) {
      if (e instanceof StructuralError) {
        metrics.rejectedStructural.inc({ reason: e.reason });
        log.info('rejected structurally', { id, reason: e.reason });
        return { outcome: 'structural', reason: e.reason, message: e.message };
      }
      throw e;
    }

    // 2. Rate-limit gate - BEFORE any mint call, session loading included.
    const release = this.limiter.acquire(clientIp);
    if (release === null) {
      metrics.rateLimited.inc();
      log.warn('rate limited', { id });
      return {
        outcome: 'rate_limited',
        message: 'too many payment attempts right now; the token was not consumed - retry shortly.',
      };
    }
    try {
      return await this.redeemInner(rawCredential, parsed, id);
    } finally {
      release();
    }
  }

  private async redeemInner(
    rawCredential: string,
    parsed: ReturnType<typeof precheck>,
    id: string,
  ): Promise<RedeemResult> {
    // 3. Mint session (info fetch enables the library's NUT-19 replay; keyset
    // fetch feeds the fee + unit checks).
    let session: MintSession;
    try {
      // precheck confirmed allowlist membership, so canonicalization succeeds.
      session = await this.sessions.get(canonicalMintUrl(parsed.mint));
    } catch (e) {
      if (isMintDown(e)) {
        metrics.mintUnreachable.inc();
        log.warn('mint unreachable while loading session', { id, error: String((e as Error).name) });
        return {
          outcome: 'mint_unreachable',
          message: 'the mint is unreachable; the token was not consumed - retry later.',
        };
      }
      throw e;
    }

    // Structural checks that need the keysets: full decode, per-proof keyset
    // unit, fee, EXACT amount. One keyset refresh on unknown-keyset, because a
    // rotation since our cache is legitimate; a second miss is the caller's.
    let verified;
    try {
      verified = verifyAgainstMint(parsed, session, this.config.priceSats);
    } catch (e) {
      if (e instanceof StructuralError && e.reason === 'unknown_keyset') {
        try {
          await session.refreshKeysets();
        } catch {
          // Refresh failing is a mint hiccup; the original structural verdict
          // still stands for this request.
        }
        try {
          verified = verifyAgainstMint(parsed, session, this.config.priceSats);
        } catch (e2) {
          if (e2 instanceof StructuralError) {
            metrics.rejectedStructural.inc({ reason: e2.reason });
            log.info('rejected structurally', { id, reason: e2.reason });
            return { outcome: 'structural', reason: e2.reason, message: e2.message };
          }
          throw e2;
        }
      } else if (e instanceof StructuralError) {
        metrics.rejectedStructural.inc({ reason: e.reason });
        log.info('rejected structurally', { id, reason: e.reason });
        return { outcome: 'structural', reason: e.reason, message: e.message };
      } else {
        throw e;
      }
    }

    // 4. Generate outputs and PERSIST them BEFORE the swap (this
    // is what makes the NUT-09 restore unconditional; it is not optional).
    // Outputs total price exactly: NUT-02's balance check requires
    // sum(inputs) - fee == sum(outputs), and verify pinned
    // sum(inputs) == price + fee.
    //
    // Everything persisted (and everything sent) is normalized to WIRE-
    // canonical JSON first - plain numeric amounts, fixed key order. cashu-ts
    // types amounts as Amount objects whose toJSON() is a STRING, so
    // JSON.stringify of the raw objects would write amounts a NUT-09 replay
    // (or the mint itself) rejects as "invalid type: string, expected u64".
    // Building the request from the normalized objects also means a later
    // re-send serializes identically, which is what the mint's NUT-19 cache
    // keys on.
    let outputs = session.createOutputs(this.config.priceSats);
    let swapRequest = {
      inputs: verified.proofs.map(wireProof),
      outputs: outputs.map((o) => wireBlindedMessage(o.blindedMessage)),
    };
    let ts = new Date().toISOString();
    try {
      await this.store.put(
        `${id}/outputs.json`,
        JSON.stringify({
          v: 1,
          id,
          ts,
          mint: verified.mintUrl,
          unit: this.config.unit,
          price_sats: this.config.priceSats,
          fee_sats: verified.fee,
          // The exact request we send: what a post-crash resolver replays
          // byte-identically within the mint's NUT-19 ttl, or feeds to NUT-09
          // restore after it.
          swap_request: swapRequest,
          outputs: outputs.map((o) => OutputData.serialize(o)),
        }),
      );
    } catch (e) {
      if (isAlreadyExists(e)) {
        // This token already has a persisted attempt. THREE cases now, not
        // two (the third came out of the review):
        //
        // COMPLETED AND DELIVERED (signatures.json + outcome.json with
        // delivered=true): the payment bought its data once already - refuse
        // as already_spent. Without this check, the byte-identical replay
        // below would hit the mint's NUT-19 cache inside its ttl and
        // "succeed" again - handing out the paid data twice for one payment.
        // The mint's 11001 refusal only exists OUTSIDE the cache window, so
        // within it the store is the discriminator. (This is not spentness
        // bookkeeping, which is the mint's alone - it is reading the redemption
        // record we must keep anyway.)
        //
        // COMPLETED BUT NOT DELIVERED (signatures.json, no delivered=true
        // outcome): the swap finished but the caller never got their data -
        // the MCP server timed out mid-call, or the delivery failed. A
        // re-presentation of the SAME token is that caller coming back for
        // what they paid for, so answer `redeemed` with the ORIGINAL receipt
        // and let the data flow. This is what makes "retry the same call
        // with the same token" true after an indeterminate timeout, and it
        // can never double-charge: the money moved exactly once.
        try {
          if ((await this.store.get(`${id}/signatures.json`)) !== null) {
            let deliveredOnce = false;
            try {
              const outcome = JSON.parse((await this.store.get(`${id}/outcome.json`)) ?? 'null');
              deliveredOnce = outcome?.delivered === true;
            } catch {
              // Unreadable outcome record: treat as not-delivered - handing
              // the payer their data again is the safe direction.
            }
            if (deliveredOnce) {
              metrics.refusedSpent.inc();
              log.info('refused: token already redeemed and delivered here', { id });
              return {
                outcome: 'already_spent',
                // Attribution matters here: THIS refusal is ours, from the
                // redemption record - inside the mint's NUT-19 ttl the mint
                // itself would have answered the replay from cache. Saying
                // "the mint refused" would be a plain lie about which party
                // stopped the double-serve.
                //
                // But no "redeemed HERE" either: the mint is the
                // replay-protection state (we remember nothing
                // about spentness), and this record is the DELIVERY audit
                // trail we must keep. "Here" reads as a claim to be the
                // authority on whether a token is spent, which is the one
                // thing we deliberately are not. State the delivery fact; let
                // spentness stay the mint's.
                message: 'this token was already redeemed, and the data it paid for was delivered.',
                id,
              };
            }
            try {
              const prior = JSON.parse((await this.store.get(`${id}/outputs.json`))!) as {
                ts: string;
                mint: string;
                fee_sats: number;
              };
              log.info('resuming delivery of a completed but undelivered redemption', { id });
              return {
                outcome: 'redeemed',
                receipt: {
                  id,
                  mint: prior.mint,
                  amount_sats: this.config.priceSats,
                  fee_sats: prior.fee_sats,
                  ts: prior.ts,
                },
              };
            } catch {
              // outputs.json unreadable but signatures exist: fall through to
              // the resume path, whose own read classifies the failure.
            }
          }
        } catch {
          // Store read failed; fall through to the resume attempt, whose own
          // read will classify the failure.
        }
        // UNRESOLVED attempt: an earlier try (or an earlier PROCESS) died
        // between persist and resolution. Resume THAT attempt: reuse its
        // outputs and its exact swap request, so the mint's NUT-19 cache can
        // replay and NUT-09 restore stays anchored to the blinded messages
        // that may already be signed. Generating fresh outputs here would
        // orphan money.
        try {
          const prior = JSON.parse((await this.store.get(`${id}/outputs.json`))!) as {
            ts: string;
            swap_request: typeof swapRequest;
            outputs: Parameters<typeof OutputData.deserialize>[0][];
          };
          outputs = prior.outputs.map((o) => OutputData.deserialize(o));
          swapRequest = prior.swap_request;
          ts = prior.ts;
          log.info('resuming persisted redemption attempt', { id });
        } catch (readErr) {
          metrics.persistFailures.inc({ phase: 'outputs' });
          log.error('persisted attempt exists but could not be read', {
            id,
            error: String((readErr as Error).name ?? 'Error'),
          });
          return {
            outcome: 'unresolved',
            message: 'an earlier payment attempt with this token could not be resolved; do not pay again - quote the receipt reference in a support claim.',
            id,
          };
        }
      } else {
        metrics.persistFailures.inc({ phase: 'outputs' });
        log.error('persist failed before swap; aborting with token unconsumed', {
          id,
          phase: 'outputs',
          error: String((e as Error).name ?? 'Error'),
        });
        return {
          outcome: 'store_unavailable',
          message: 'server-side storage is unavailable; the token was not consumed - retry later.',
        };
      }
    }

    // 5. The swap. cashu-ts replays the byte-identical request internally per
    // NUT-19 while the mint's ttl window allows; what escapes here is terminal
    // for this attempt.
    metrics.swapsAttempted.inc();
    let signatures;
    try {
      const res = await session.swap(
        swapRequest.inputs,
        swapRequest.outputs as unknown as Parameters<MintSession['swap']>[1],
      );
      signatures = res.signatures;
    } catch (e) {
      return await this.afterSwapFailure(e, session, outputs, verified.proofs, id, verified, ts);
    }

    metrics.swapsSucceeded.inc();
    return await this.persistAndReceipt(session, outputs, signatures, id, verified, ts);
  }

  private async persistAndReceipt(
    session: MintSession,
    outputs: OutputData[],
    signatures: Awaited<ReturnType<MintSession['swap']>>['signatures'],
    id: string,
    verified: { mintUrl: string; fee: number },
    ts: string,
  ): Promise<RedeemResult> {
    const receipt: Receipt = {
      id,
      mint: verified.mintUrl,
      amount_sats: this.config.priceSats,
      fee_sats: verified.fee,
      ts,
    };
    // 6. Persist the money. Two attempts (a retry is required); on
    // final failure the phase-1 object still makes the proofs NUT-09
    // recoverable, but the caller gets the delivery-failed class, not a clean
    // receipt.
    //
    // Proof construction is INSIDE a try (review finding: OutputData.toProof
    // verifies DLEQ when the mint supplies it and THROWS on mismatch, and an
    // uncaught throw here became a 500 the MCP side read as "token never
    // presented"). A DLEQ failure indicates a misbehaving mint,
    // not a payment failure - THE PAYMENT STANDS: persist the raw signatures
    // (money recoverable from outputs + signatures regardless) and proceed.
    let proofs: Proof[] | null = null;
    try {
      proofs = await Promise.all(
        signatures.map(async (sig, i) => wireProof(await session.toProof(outputs[i]!, sig))),
      );
    } catch (e) {
      log.warn('proof construction failed (DLEQ or keyset lookup); signatures persisted, payment stands', {
        id,
        error: String((e as Error).name ?? 'Error'),
      });
    }
    const body = JSON.stringify({
      v: 1,
      id,
      ts: new Date().toISOString(),
      signatures: signatures.map((s) => wireSignature(s)),
      proofs,
    });
    for (let attempt = 0; ; attempt++) {
      try {
        await this.store.put(`${id}/signatures.json`, body);
        break;
      } catch (e) {
        if (isAlreadyExists(e)) {
          // A concurrent or resumed attempt already persisted this exact
          // redemption's signatures - the money is durably stored, which is
          // what this phase exists to guarantee. Success, not an outage
          // (review finding: treating EEXIST as a storage failure withheld
          // data from a paying caller while everything was healthy).
          break;
        }
        if (attempt >= 1) {
          metrics.persistFailures.inc({ phase: 'signatures' });
          // The caller is about to be told delivery failed, and the MCP
          // server's outcome report may not reach a store that is failing -
          // so this path feeds the delivery-failed alert DIRECTLY too.
          metrics.deliveryFailed.inc();
          log.error('persist failed after successful swap; proofs recoverable via NUT-09', {
            id,
            phase: 'signatures',
            error: String((e as Error).name ?? 'Error'),
          });
          return {
            outcome: 'persist_failed',
            message: 'payment was received but a server-side storage failure interrupted delivery.',
            receipt,
          };
        }
      }
    }
    this.armOutcomeWatchdog(id);
    log.info('redeemed', { id, mint: verified.mintUrl, amount_sats: this.config.priceSats, fee_sats: verified.fee });
    return { outcome: 'redeemed', receipt };
  }

  /** Review finding: if the MCP server dies (or times out) between our
   * `redeemed` answer and its delivery report, nobody paid attention to a
   * paid-but-undelivered call and no metric moved. This timer checks for the
   * outcome record after a grace window and feeds the delivery-failed alert
   * if it never arrived. In-process only (lost on restart - the persisted
   * record still exists for a manual sweep); unref'd so it never holds the
   * process open. */
  private armOutcomeWatchdog(id: string): void {
    const timer = setTimeout(() => {
      void this.store
        .get(`${id}/outcome.json`)
        .then((outcome) => {
          if (outcome === null) {
            metrics.deliveryFailed.inc();
            log.error('redemption completed but no delivery outcome was reported', {
              id,
              window_ms: this.config.outcomeWatchdogMs,
            });
          }
        })
        .catch(() => {
          /* store read failure has its own alerting */
        });
    }, this.config.outcomeWatchdogMs);
    timer.unref?.();
  }

  private async afterSwapFailure(
    e: unknown,
    session: MintSession,
    outputs: OutputData[],
    inputs: Proof[],
    id: string,
    verified: { mintUrl: string; fee: number },
    ts: string,
  ): Promise<RedeemResult> {
    // Clean refusal on the FIRST attempt: inputs already spent, nothing
    // ambiguous. The distinct discriminator required here - telling a
    // caller whose payment was consumed to "pay again" is exactly wrong, so
    // the message says what happened, not what to pay.
    if (e instanceof MintOperationError && e.code === CODE_TOKEN_ALREADY_SPENT) {
      metrics.refusedSpent.inc();
      log.info('refused: already spent', { id });
      return {
        outcome: 'already_spent',
        message: 'the mint refused this token as already spent.',
        id,
      };
    }

    // Our own outputs already signed: the swap DID execute (a lost response
    // beyond the NUT-19 window) - restore recovers the signatures.
    const ambiguous = isMintDown(e) || (e instanceof MintOperationError && e.code === CODE_OUTPUT_ALREADY_SIGNED);
    if (!ambiguous) {
      if (e instanceof MintOperationError) {
        // Any OTHER MintOperationError is a SETTLED 4xx refusal: the mint
        // answered, rejected the transaction, and nothing was spent - e.g.
        // cdk 10003 "proof could not be verified" for a token with fabricated
        // secrets, which passes every offline check by construction (no
        // signature verification is possible offline). Review finding:
        // classifying this as `unresolved` let an anonymous caller feed the
        // operator alert forever with free junk tokens, and handed them a
        // "quote this in a support claim" framing for a token that was never
        // money. Its own outcome, its own metric, no alarm. The code (a
        // number) is safe to surface; the mint's detail text is not (it can
        // echo request content).
        metrics.mintRejected.inc();
        log.info('mint refused the swap as invalid; nothing spent', { id, code: e.code });
        return {
          outcome: 'mint_rejected',
          message: `the mint rejected this token as invalid (mint error code ${e.code}); nothing was spent.`,
          id,
        };
      }
      // A non-HTTP exception mid-swap: the request MAY have been sent, so the
      // conservative classification is unresolved.
      metrics.unresolved.inc();
      log.error('swap failed with an unexpected exception', { id, error: String((e as Error).name ?? 'Error') });
      return {
        outcome: 'unresolved',
        message: 'the swap failed in an unknown state; do not pay again - retry this call with the SAME token, or quote the receipt reference in a support claim.',
        id,
      };
    }

    log.warn('swap ambiguous; running recovery ladder', { id, error: String((e as Error).name ?? 'Error') });

    // Ladder step 2: NUT-09 restore with our persisted blinded messages.
    try {
      const restored = await session.restore(outputs.map((o) => o.blindedMessage));
      if (restored.signatures.length === outputs.length) {
        metrics.swapsSucceeded.inc();
        log.info('recovered via NUT-09 restore', { id });
        return await this.persistAndReceipt(session, outputs, restored.signatures, id, verified, ts);
      }
      if (restored.signatures.length > 0) {
        // Partially signed outputs should be impossible for an atomic swap.
        metrics.unresolved.inc();
        log.error('restore returned a partial signature set', {
          id,
          restored: restored.signatures.length,
          expected: outputs.length,
        });
        return {
          outcome: 'unresolved',
          message: 'payment state could not be resolved; do not pay again - quote the receipt reference in a support claim.',
          id,
        };
      }
    } catch (restoreErr) {
      metrics.unresolved.inc();
      log.error('restore unreachable after ambiguous swap', {
        id,
        error: String((restoreErr as Error).name ?? 'Error'),
      });
      return {
        outcome: 'unresolved',
        message: 'the mint is unreachable and the payment state is unknown; do not pay again until it resolves.',
        id,
      };
    }

    // Restore says our outputs were never signed. Ladder step 3: NUT-07
    // checkstate on the INPUTS decides whether the token is intact.
    try {
      const Ys = inputs.map((p) => hashToCurve(new TextEncoder().encode(p.secret)).toHex(true));
      const { states } = await session.check(Ys);
      const allUnspent = states.length === Ys.length && states.every((s) => s.state === CheckStateEnum.UNSPENT);
      if (allUnspent) {
        metrics.mintUnreachable.inc();
        log.warn('swap never executed; token intact', { id });
        return {
          outcome: 'mint_unreachable',
          message: 'the mint could not complete the swap; the token was not consumed - retry later.',
        };
      }
      const anySpent = states.some((s) => s.state === CheckStateEnum.SPENT);
      if (anySpent) {
        // Inputs consumed, and NOT by our outputs (restore was empty): a
        // genuine double-spend race lost to someone else.
        metrics.refusedSpent.inc();
        log.info('inputs spent elsewhere during ambiguous swap', { id });
        return {
          outcome: 'already_spent',
          message:
            'the mint reports these proofs as already spent, and they were not spent by this payment.',
          id,
        };
      }
    } catch (checkErr) {
      log.error('checkstate unreachable after ambiguous swap', {
        id,
        error: String((checkErr as Error).name ?? 'Error'),
      });
    }

    metrics.unresolved.inc();
    log.error('payment state unresolved after full ladder', { id });
    return {
      outcome: 'unresolved',
      message: 'payment state could not be resolved; do not pay again - quote the receipt reference in a support claim.',
      id,
    };
  }

  /** The delivery audit record: the MCP server reports whether the paid-for
   * data was actually delivered; written as its own immutable object. A
   * duplicate report finds the key already written and is a no-op success -
   * the FIRST outcome is the audit truth, and a re-delivery after the
   * resume-deliver path legitimately reports twice. */
  async recordOutcome(id: string, delivered: boolean, note: string | null): Promise<boolean> {
    if (!delivered) metrics.deliveryFailed.inc();
    try {
      await this.store.put(
        `${id}/outcome.json`,
        JSON.stringify({ v: 1, id, ts: new Date().toISOString(), delivered, note }),
      );
      return true;
    } catch (e) {
      if (isAlreadyExists(e)) return true;
      metrics.persistFailures.inc({ phase: 'outcome' });
      log.error('outcome record persist failed', { id, error: String((e as Error).name ?? 'Error') });
      return false;
    }
  }
}
