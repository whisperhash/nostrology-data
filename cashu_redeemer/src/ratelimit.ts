// Outbound-mint-call rate limiting, against unauthenticated outbound swaps at
// our one uncontrollable dependency. The endpoint has no
// auth and no inbound rate limit, so a structurally valid token with fabricated
// secrets reaches the mint - an anonymous caller could otherwise drive
// unlimited requests to Minibits from our egress IP and get us banned by the
// single dependency the demo cannot lose.
//
// Three gates, all checked BEFORE any network call:
//   * a concurrency cap (simultaneous mint conversations),
//   * a global per-minute cap,
//   * a per-source-IP per-minute cap (the IP is forwarded by the MCP server,
//     which reads it at the transport; the sidecar itself only ever sees the
//     MCP container's address).
//
// Fixed one-minute windows, not sliding: at these limits (tens per minute) the
// burst-at-the-boundary imprecision is irrelevant and the implementation stays
// small enough to be obviously correct. RECOVERY calls (NUT-19 replay, NUT-09
// restore) run under a slot already acquired for the redemption - a retry must
// never be starved by the limiter mid-flight, or the limiter itself would
// create the unresolved states it exists to prevent.

export class RateLimiter {
  private inFlight = 0;
  private windowStart = 0;
  private globalCount = 0;
  private perIp = new Map<string, number>();

  private readonly maxConcurrent: number;
  private readonly maxPerMinute: number;
  private readonly maxPerMinutePerIp: number;

  constructor(maxConcurrent: number, maxPerMinute: number, maxPerMinutePerIp: number) {
    this.maxConcurrent = maxConcurrent;
    this.maxPerMinute = maxPerMinute;
    this.maxPerMinutePerIp = maxPerMinutePerIp;
  }

  /** Try to take a slot for one redemption's mint conversation. Returns a
   * release function, or null if refused. */
  acquire(ip: string | null, now = Date.now()): (() => void) | null {
    const minute = Math.floor(now / 60_000);
    if (minute !== this.windowStart) {
      this.windowStart = minute;
      this.globalCount = 0;
      this.perIp.clear();
    }
    if (this.inFlight >= this.maxConcurrent) return null;
    if (this.globalCount >= this.maxPerMinute) return null;
    // All transport-less callers (stdio, a future ContextVM path) share the
    // one 'unknown' bucket, so a single abuser there starves them
    // collectively - accepted at v0 (the public HTTP path always has an IP);
    // revisit if a second transport goes live.
    const ipKey = ip ?? 'unknown';
    const ipCount = this.perIp.get(ipKey) ?? 0;
    if (ipCount >= this.maxPerMinutePerIp) return null;

    this.inFlight += 1;
    this.globalCount += 1;
    this.perIp.set(ipKey, ipCount + 1);
    let released = false;
    return () => {
      if (!released) {
        released = true;
        this.inFlight -= 1;
      }
    };
  }
}
