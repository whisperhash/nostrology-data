// Structural verification - EVERYTHING that can be checked without a network
// call, in this order: shape, version, unit, mint
// allowlist, keyset resolution, fee arithmetic, exact amount. A malformed
// token never reaches the mint.
//
// Failure contract, load-bearing for the log-safety requirement:
// every rejection is a StructuralError carrying a
// fixed `reason` code and a fixed human message. NOTHING here ever
// interpolates the token or any proof field into an error string - these
// messages cross into the MCP tool result AND the call log, both of which ship
// to Loki.

import {
  getDecodedToken,
  getTokenMetadata,
  sumProofs,
  type Proof,
  type Token,
} from '@cashu/cashu-ts';
import { canonicalMintUrl, isAllowlistedMint } from './canonical.ts';
import type { MintSession } from './mints.ts';

export type StructuralReason =
  | 'not_a_token'
  | 'tokenv3_unsupported'
  | 'payment_request_not_token'
  | 'unit_mismatch'
  | 'mint_not_allowlisted'
  | 'unknown_keyset'
  | 'duplicate_proofs'
  | 'amount_mismatch';

export class StructuralError extends Error {
  readonly reason: StructuralReason;
  constructor(reason: StructuralReason, message: string) {
    super(message);
    this.reason = reason;
    this.name = 'StructuralError';
  }
}

export interface VerifiedToken {
  /** The bare cashuB string (extracted from a PaymentRequestPayload if that is
   * what arrived). */
  encoded: string;
  mintUrl: string; // canonical
  proofs: Proof[];
  amount: number; // sum of proof amounts
  fee: number; // NUT-02 input fee for these proofs at this mint
}

/** The accepted credential shapes: a bare cashuB TokenV4
 * string, or a NUT-18 PaymentRequestPayload object ({id?, memo?, unit, mint,
 * proofs}) serialized as JSON. Returns the extracted parts without any network.
 */
export function parseCredential(raw: string): { mint: string; unit: string; encoded: string | null; proofs: Proof[] | null } {
  const trimmed = raw.trim();
  if (trimmed.startsWith('cashuA')) {
    throw new StructuralError(
      'tokenv3_unsupported',
      'cashuA (TokenV3) is not accepted: it carries a multi-mint array. Re-encode as cashuB (TokenV4).',
    );
  }
  if (trimmed.startsWith('creq')) {
    throw new StructuralError(
      'payment_request_not_token',
      'that is a Cashu payment REQUEST (creq...), not a token - pay it with a wallet and send the resulting cashuB token.',
    );
  }
  if (trimmed.startsWith('cashuB')) {
    let meta;
    try {
      meta = getTokenMetadata(trimmed);
    } catch {
      throw new StructuralError('not_a_token', 'the cashu_token value did not decode as a cashuB (TokenV4) token.');
    }
    return { mint: meta.mint, unit: meta.unit, encoded: trimmed, proofs: null };
  }
  if (trimmed.startsWith('{')) {
    // NUT-18 PaymentRequestPayload: {id?, memo?, unit, mint, proofs}
    let payload: unknown;
    try {
      payload = JSON.parse(trimmed);
    } catch {
      throw new StructuralError('not_a_token', 'the cashu_token value is neither a cashuB string nor valid JSON.');
    }
    const p = payload as { unit?: unknown; mint?: unknown; proofs?: unknown };
    if (
      typeof p.mint !== 'string' ||
      typeof p.unit !== 'string' ||
      !Array.isArray(p.proofs) ||
      p.proofs.length === 0
    ) {
      throw new StructuralError(
        'not_a_token',
        'a NUT-18 PaymentRequestPayload needs string `mint`, string `unit` and a non-empty `proofs` array.',
      );
    }
    for (const proof of p.proofs as unknown[]) {
      const pr = proof as { amount?: unknown; id?: unknown; secret?: unknown; C?: unknown };
      if (
        // Positive INTEGER, not merely number: a negative or fractional amount
        // would flow into the sum check and then to the mint (review
        // finding); NUT-00 amounts are u64.
        typeof pr.amount !== 'number' ||
        !Number.isInteger(pr.amount) ||
        pr.amount <= 0 ||
        typeof pr.id !== 'string' ||
        typeof pr.secret !== 'string' ||
        typeof pr.C !== 'string'
      ) {
        throw new StructuralError('not_a_token', 'every proof needs a positive integer `amount` and string `id`, `secret`, `C`.');
      }
    }
    return { mint: p.mint, unit: p.unit, encoded: null, proofs: p.proofs as Proof[] };
  }
  throw new StructuralError(
    'not_a_token',
    'the cashu_token value must be a cashuB (TokenV4) string or a NUT-18 PaymentRequestPayload JSON object.',
  );
}

/** Offline checks only - everything before the mint session exists. */
export function precheck(raw: string, unit: string, allowlist: readonly string[]): ReturnType<typeof parseCredential> {
  const parsed = parseCredential(raw);
  // TokenV4 omits `unit` for sat in some encoders; getTokenMetadata defaults
  // it. An explicit foreign unit is rejected here, a foreign-unit KEYSET (unit
  // omitted or lied about) is caught in verifyAgainstMint below.
  if (parsed.unit !== unit) {
    throw new StructuralError('unit_mismatch', `only unit "${unit}" is accepted.`);
  }
  if (!isAllowlistedMint(parsed.mint, allowlist)) {
    throw new StructuralError('mint_not_allowlisted', 'that mint is not on the accepted-mints list; the challenge names the mints we advertise.');
  }
  return parsed;
}

/** Checks that need the mint's keysets (already fetched by the session):
 * resolve every proof's keyset, reject foreign-unit keysets distinctly,
 * compute the NUT-02 fee, require the exact amount. */
export function verifyAgainstMint(
  parsed: ReturnType<typeof parseCredential>,
  session: MintSession,
  priceSats: number,
): VerifiedToken {
  let proofs: Proof[];
  let token: Token | null = null;
  if (parsed.proofs) {
    proofs = parsed.proofs;
  } else {
    try {
      token = getDecodedToken(parsed.encoded!, session.allKeysetIds());
    } catch {
      // TokenV4 carries keyset ids in binary; an id that matches none of the
      // mint's keysets fails the decode's id-completion step.
      throw new StructuralError('unknown_keyset', 'a proof references a keyset this mint does not publish.');
    }
    proofs = token.proofs;
  }

  for (const proof of proofs) {
    const keysetUnit = session.keysetUnit(proof.id);
    if (keysetUnit === null) {
      throw new StructuralError('unknown_keyset', 'a proof references a keyset this mint does not publish.');
    }
    if (keysetUnit !== session.unit) {
      throw new StructuralError('unit_mismatch', `a proof belongs to a non-"${session.unit}" keyset; only unit "${session.unit}" is accepted.`);
    }
  }

  // Same secret twice would double-count in the sum below (the mint would
  // reject the swap anyway, but a structural check must not depend on it).
  const secrets = new Set(proofs.map((p) => p.secret));
  if (secrets.size !== proofs.length) {
    throw new StructuralError('duplicate_proofs', 'the token lists the same proof more than once.');
  }

  const amount = sumProofs(proofs).toNumber();
  const fee = session.feeForProofs(proofs);
  // The exact-amount rule. NUT-18's `a` is NET of input
  // fees (post-2026-07-21), so the challenge advertises a=21 and the token
  // must carry 21 + swap_fee. Under- AND over-payment are rejected: any >=
  // policy forces a change computation NUT-02's balance check makes impossible
  // without a denomination split.
  if (amount !== priceSats + fee) {
    throw new StructuralError(
      'amount_mismatch',
      `the token must carry exactly ${priceSats} + swap_fee sats for this mint (fee for this token's proofs: ${fee}); got a different total. No change is made and overpayment is not kept.`,
    );
  }

  return {
    encoded: parsed.encoded ?? '',
    mintUrl: canonicalMintUrl(parsed.mint),
    proofs,
    amount,
    fee,
  };
}
