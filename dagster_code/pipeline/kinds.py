"""Kind -> serving-table + anti-shrink MIN_ROWS maps, and the per-surface materialized-view lists.

Pure port of run.sh's kind_to_table / min_rows_for (run.sh:342-375). The MIN_ROWS floor is passed
to the loader (which gates on it before TRUNCATE); the table name is used for the post-load
row-count stat. No AWS, no DB — unit-testable.
"""

# kind -> serving table (mirrors the loader's table map). The (9735, "zapstore") subset has its
# own table so it doesn't collide with the full 9735 load.
_KIND_TABLE = {
    0: "events_kind_0_profiles",
    1: "events_kind_1_posts",
    9321: "events_kind_9321_nutzaps",
    9735: "events_kind_9735_zaps",
    10002: "events_kind_10002_relay_lists",
    10019: "events_kind_10019_nutzap_configs",
    30063: "events_kind_30063_app_releases",
    30267: "events_kind_30267_app_stacks",
    32267: "events_kind_32267_app_catalog",
}

# Per-kind anti-shrink FLOOR — 80% of a known-good load (run.sh:342-355). NOT an integrity gate;
# the loader's own 0-row refusal is the real never-wipe guard. The 9735->zapstore subset is NOT
# gated by 9735's floor — forced to 1 (run.sh:732).
_MIN_ROWS = {
    0: 1806407,
    9735: 2793747,
    10002: 757084,
    32267: 2704,
    30063: 11191,
    30267: 588,
    9321: 3855,
    10019: 5066,
}

# The /relays materialized views, refreshed CONCURRENTLY after the k=10002 load — lifted OUT of the
# loader (loader:2339-2350) into the relays_mv asset. CONCURRENTLY needs each MV's unique index
# (schema/init.sql) and a prior population (CREATE … WITH DATA).
RELAYS_MVS = ("relay_stats_mv", "relay_per_user_distribution_mv", "relay_tier_user_counts_mv")

# The /lightning materialized views, refreshed CONCURRENTLY by the lightning_mv asset after the
# kind-0 + kind-9735 + signer loads (the daily-grain MV reads BOTH the zaps table and the signer
# map, so lightning_mv is downstream of all three). lightning_provider_stats_mv backs the provider
# leaderboard; lightning_daily_provider_mv backs the four trend queries. Same CONCURRENTLY
# requirements as the relay MVs (unique index + prior population). See schema/init.sql.
LIGHTNING_MVS = ("lightning_provider_stats_mv", "lightning_daily_provider_mv")


def kind_to_class(kind):
    """Map a Nostr kind to its NIP-01 archive class — the `c=<class>` S3 path segment.

    Pure port of run.sh's kind_to_class (run.sh:401-408), which mirrors the producer's AUTHORITATIVE
    range law (nozzl-types/src/event_class.rs EventClass::classify) rather than a hand-maintained
    per-kind list (a list that lagged an onboarding would map a new kind to a wrong/empty prefix and
    silently short its extract). Returns "replaceable" | "addressable" | "regular", or None for an
    ephemeral [20000,30000) / non-integer kind that has NO archived chunks — callers MUST treat None
    as "no scoped prefix" (fall back to a full-bucket scan; no SQL template)."""
    try:
        k = int(kind)
    except (TypeError, ValueError):
        return None
    if k == 0 or k == 3 or (10000 <= k < 20000):
        return "replaceable"
    if 30000 <= k < 40000:
        return "addressable"
    if 20000 <= k < 30000:
        return None  # ephemeral — relays don't persist it; no chunks
    return "regular"


def kind_to_table(kind, into: str = "") -> str:
    """Serving table for a kind (+ optional INTO subset). Unknown kind -> "" (caller skips)."""
    k = int(kind)
    if into == "zapstore" and k == 9735:
        return "events_kind_9735_zaps_zapstore"
    return _KIND_TABLE.get(k, "")


def min_rows_for(kind, into: str = "") -> int:
    """Anti-shrink floor for a kind (+ optional INTO). The zapstore 9735 subset is forced to 1."""
    if into == "zapstore" and int(kind) == 9735:
        return 1
    return _MIN_ROWS.get(int(kind), 1)
