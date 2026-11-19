# cashu-redeemer - the Cashu redemption sidecar

The only process in the nostrology stack that talks to a Cashu mint or keeps
proof secrets (the MCP server only passes the caller's credential through, in
memory). The public MCP server (`nostrology_mcp/`) forwards a presented
credential here over the compose bridge; this service verifies it structurally,
swaps it at an allowlisted mint (NUT-03), persists the resulting proofs
durably, and answers with a typed outcome.

TypeScript on `@cashu/cashu-ts` **4.8.0 pinned** (the reference implementation,
maintained by the protocol authors; v5 is still a release candidate and money
code does not ride RCs). Runtime deps are cashu-ts (+4.4 MB) and the AWS S3
client; the HTTP server is `node:http`, the Prometheus counters are hand-rolled
- every dependency in this container is a dependency in the thing that holds
the money.

## Surface

| Route | What |
|---|---|
| `POST /redeem` | `{token, client_ip?}` -> a typed `RedeemResult` (`redeemed` with a receipt, or `structural` / `rate_limited` / `mint_unreachable` / `already_spent` / `mint_rejected` / `store_unavailable` / `persist_failed` / `unresolved`) |
| `POST /redemptions/outcome` | `{id, delivered, note?}` - the MCP server's delivery report, kept as the audit record |
| `GET /health` | liveness (no mint, no store) |
| `GET /metrics` | Prometheus text, for the deployment's metrics scraper |

**Never published to a host port, and Caddy has no path here.** No route
authenticates, and `client_ip` is taken from the request body as given, so the
service belongs on a network that only the MCP server and a metrics scraper can
reach.

**Counter semantics** (`/metrics`): `cashu_delivery_failed_total` is not one per
event - a `persist_failed` counts twice (this service's own increment plus the
MCP server's `delivered=false` report), and every duplicate `delivered=false`
report counts again. `cashu_refused_spent_total` counts the store's "already
delivered" refusals and the ladder's "spent elsewhere" as well as the mint's
11001. An alert on any increase is unaffected by either.

## The redemption flow (redeem.ts)

1. **Offline structural checks** - shape (cashuB TokenV4 or NUT-18
   `PaymentRequestPayload`; cashuA and `creq` refused by name), unit, mint
   allowlist (canonicalized: lowercase scheme/host, default port dropped,
   trailing slashes stripped, exact match).
2. **Rate-limit gate** - concurrency (2) + global (30/minute) + per-source-IP
   (6/minute), the IP taken from the request body (the MCP server forwards the
   caller's; the TCP peer here is always a container). Everything below counts
   against it.
3. **Checks against the mint's keysets** - the mint session (NUT-06 info, all
   keysets, the active keyset's keys: three GETs, cached per process, retried
   on the next request if loading failed) feeds keyset resolution (one refresh
   on an unknown keyset) with a distinct `unit_mismatch` for foreign-unit
   keysets, the duplicate-proof check, the NUT-02 fee (`(sum_ppk + 999) //
   1000`, per proof), and the **exact amount** rule: `sum(proofs) == price +
   fee`, no change, overpayment rejected. Network on a cold session, but the
   token itself is never sent.
4. **Persist the blinded outputs BEFORE the swap** (`<id>/outputs.json`:
   serialized `(secret, r, B_)` + the exact swap request, which includes the
   payer's input proofs). This is what makes the recovery ladder unconditional;
   it is not optional. If the object already exists, this is a
   re-presentation - see below.
5. **NUT-03 swap.** cashu-ts replays the byte-identical request internally per
   NUT-19 - for `POST /v1/swap` only, on a network error or 5xx only, at most 9
   retries with jittered backoff capped at 1 s, within the mint's advertised ttl
   (enabled by loading mint info at session start - see mints.ts).
6. **What a failed swap means.** A network error or 5xx that outlasted those
   retries runs the **ladder**: NUT-09 restore against our persisted blinded
   messages (all signed = the money is ours, resume at 7), else NUT-07
   checkstate on the inputs (all UNSPENT = token intact -> `mint_unreachable`,
   retryable; any SPENT with restore empty = spent elsewhere ->
   `already_spent`; anything else -> `unresolved`, do not pay again). A mint
   refusal skips the ladder: 11001 -> `already_spent`; any other Cashu-coded
   400 -> `mint_rejected`; any other 4xx -> `unresolved`.
7. **Persist the signatures** (`<id>/signatures.json` - this object IS the
   money), answer with the receipt (`id` = sha256 of the trimmed credential).

Re-presentation semantics (per-token-hash serialized in-process, so concurrent
presentations cannot race each other into a false storage error):

- attempt COMPLETED **and delivered** (outcome.json says delivered=true) ->
  `already_spent` from the store - a byte-identical replay could otherwise
  "succeed" from the mint's NUT-19 cache and hand out the data twice;
- attempt COMPLETED **but not delivered** -> `redeemed` again with the
  ORIGINAL receipt (resume-deliver: the payer coming back for what they paid
  for after a timeout; cannot double-charge - the money moved once);
- attempt UNRESOLVED -> resume it (same outputs, byte-identical request),
  which is also the crash-recovery path.

Re-presentation is keyed on the sha256 of the raw credential STRING, so it
must be byte-identical - the same proofs re-sent in the other accepted shape
hash differently (every caller-facing recovery message says so).

A refusal the mint answers with a Cashu error code (HTTP 400 + `code`) other
than 11001 and 10002 is `mint_rejected` - e.g. 10001 (`TokenNotVerified`) for
fabricated secrets: nothing spent, its own metric, no unresolved alarm and no
support-claim framing - free junk tokens must not be an alert-flooding
primitive.

After answering a fresh `redeemed`, an in-process watchdog
(CASHU_OUTCOME_WATCHDOG_MS, default 5 min) checks that the MCP server's
delivery report arrived and feeds `cashu_delivery_failed_total` if it never
did - the paid-but-nobody-reported case an MCP-side crash would otherwise make
invisible. It is not armed for a resume-deliver, and it lives in memory, so a
sidecar restart inside the window loses it.

## Storage ("the proofs ARE the money")

Three immutable keys per redemption - `outputs.json`, `signatures.json`,
`outcome.json` - append-only by construction (`wx` on file, `IfNoneMatch: '*'`
on S3; an overwrite is always a bug). `outcome.json` exists only for
redemptions that took money (it is the MCP server's delivery report); every
attempt that gets past step 3 leaves an `outputs.json`, and that object holds
the payer's input proofs - so after an outcome that told the payer their token
is intact, we still hold a spendable copy of it.

Backends:

- `CASHU_STORE=file` - a directory only the service's own user can read: mode 700,
  files 0600. Whoever can read it, or a backup of it, holds spendable proofs
  (above), so keep both as private as a wallet.
- `CASHU_STORE=s3` - `$CASHU_S3_BUCKET`, meant for SSE-KMS, versioning and denied
  deletes: implemented and tested here; the bucket and its policy are the
  deployment's to provide.

## Config (env)

All parsed once at startup (config.ts), fail-loud. The two the MCP server must
agree on ride one compose block (`x-cashu-env`): `CASHU_PRICE_SATS` (default
21) and `CASHU_MINTS` (default Minibits + testnut; **drop testnut after the
demo** - it is a FakeWallet mint, so leaving it allowlisted means anyone can
pay in nothing). The rest: `CASHU_PORT`, `CASHU_STORE*`, `CASHU_S3_*`,
`CASHU_MAX_*` (rate limits), `CASHU_MINT_TIMEOUT_MS` (per-attempt fetch
timeout; the NUT-19 retry count is cashu-ts's own, not configurable),
`CASHU_OUTCOME_WATCHDOG_MS`. The MCP-side client timeout
(`cashu.py DEFAULT_REDEEMER_TIMEOUT_SECONDS`, 240 s) is DERIVED from the
mint-timeout arithmetic - raise `CASHU_MINT_TIMEOUT_MS` and re-check that
derivation, or slow-but-completing payments start answering "indeterminate".
At today's values the derivation is short of its own claim: the contrived worst
case is ~231 s (swap phase ~156 s, restore and checkstate 15 s each, a cold
session's three GETs 45 s), ~261 s with an unknown-keyset refresh. Running past
240 s is safe - the MCP server then says the payment state is unknown and tells
the caller to retry the same call with the SAME token - but the margin is not
what the code comments say.

## Tests

```sh
npm test           # offline unit tests (canonicalization, NUT-02 fee, limiter, store, parsing)
npm run test:mint  # LIVE against https://testnut.cashu.space - mints real tokens on
                   # every run (FakeWallet marks every invoice paid; no sats, no
                   # manual step), exercises nonzero fees (input_fee_ppk=100),
                   # usd-keyset rejection, replay, the full recovery ladder via
                   # fault injection, and persist-ordering
```

The suite runs the `.ts` sources directly under node's type stripping
(`erasableSyntaxOnly` keeps the sources inside that subset); the container
builds with `tsc` (`npm run build`).
