// Hand-rolled Prometheus counters - the whole client_* ecosystem is overkill
// for a dozen monotonic counters, and every dependency in this container is a
// dependency in the thing that holds the money. Exposition format per
// https://prometheus.io/docs/instrumenting/exposition_formats/ (text 0.0.4).
//
// The counter set: swaps attempted / succeeded /
// refused-as-spent / rejected-structurally / rate-limited, plus the three
// alert feeds (delivery-failed-after-payment, unresolved, persist-failure).

type LabelValues = Record<string, string>;

class Counter {
  readonly name: string;
  readonly help: string;
  private readonly values = new Map<string, { labels: LabelValues; value: number }>();

  constructor(name: string, help: string) {
    this.name = name;
    this.help = help;
  }

  inc(labels: LabelValues = {}, by = 1): void {
    const key = JSON.stringify(labels);
    const entry = this.values.get(key);
    if (entry) entry.value += by;
    else this.values.set(key, { labels, value: by });
  }

  /** Expose these label combinations at 0 from process start. Load-bearing for
   * alerting, not cosmetic (review finding): a labeled series born at value 1
   * on its first increment gives Prometheus `increase()` nothing to count, so a
   * first-occurrence event - exactly what the persist-failure alert exists for -
   * would never fire. */
  preallocate(labelSets: LabelValues[]): this {
    for (const labels of labelSets) {
      const key = JSON.stringify(labels);
      if (!this.values.has(key)) this.values.set(key, { labels, value: 0 });
    }
    return this;
  }

  expose(): string {
    const lines = [`# HELP ${this.name} ${this.help}`, `# TYPE ${this.name} counter`];
    if (this.values.size === 0) {
      lines.push(`${this.name} 0`);
    }
    for (const { labels, value } of this.values.values()) {
      const pairs = Object.entries(labels)
        .map(([k, v]) => `${k}="${v.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`)
        .join(',');
      lines.push(pairs ? `${this.name}{${pairs}} ${value}` : `${this.name} ${value}`);
    }
    return lines.join('\n');
  }
}

export const metrics = {
  redeemRequests: new Counter(
    'cashu_redeem_requests_total',
    'Inbound /redeem requests, before any checks',
  ),
  rejectedStructural: new Counter(
    'cashu_rejected_structural_total',
    'Tokens rejected offline, before any mint call, by reason',
  ).preallocate(
    // Mirrors verify.ts's StructuralReason union; the pin test keeps them in step.
    [
      'not_a_token',
      'tokenv3_unsupported',
      'payment_request_not_token',
      'unit_mismatch',
      'mint_not_allowlisted',
      'unknown_keyset',
      'duplicate_proofs',
      'amount_mismatch',
    ].map((reason) => ({ reason })),
  ),
  mintRejected: new Counter(
    'cashu_mint_rejected_total',
    'Swaps the mint refused with a settled non-spent 4xx (e.g. fabricated secrets) - nothing spent, no alarm',
  ),
  rateLimited: new Counter(
    'cashu_rate_limited_total',
    'Requests refused by the outbound-mint-call rate limiter',
  ),
  swapsAttempted: new Counter('cashu_swaps_attempted_total', 'NUT-03 swap requests sent to a mint'),
  swapsSucceeded: new Counter('cashu_swaps_succeeded_total', 'Swaps that returned signatures'),
  refusedSpent: new Counter(
    'cashu_refused_spent_total',
    'Swaps the mint refused because the inputs were already spent',
  ),
  mintUnreachable: new Counter(
    'cashu_mint_unreachable_total',
    'Redemptions failed because the mint could not be reached before any state changed',
  ),
  unresolved: new Counter(
    'cashu_unresolved_total',
    'Redemptions left in an unknown state after the full NUT-19/NUT-09/NUT-07 recovery ladder',
  ),
  persistFailures: new Counter(
    'cashu_persist_failures_total',
    'Failed writes to the redemption store, by phase (outputs|signatures|outcome)',
  ).preallocate([{ phase: 'outputs' }, { phase: 'signatures' }, { phase: 'outcome' }]),
  deliveryFailed: new Counter(
    'cashu_delivery_failed_total',
    'Redemptions where the swap succeeded but the MCP server reported the data delivery failed - the one alert that matters most',
  ),
};

export function exposeMetrics(): string {
  return (
    Object.values(metrics)
      .map((c) => c.expose())
      .join('\n') + '\n'
  );
}
