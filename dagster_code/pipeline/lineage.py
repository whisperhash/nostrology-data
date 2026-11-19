"""The data lineage the Discord alerts read: which serving tables each Dagster asset writes, and
which consumers (the public pages, the landing board's panels and the MCP server's domains) read
which tables.

Pure (no dagster import), so it unit-tests on a workstation. definitions.py turns CONSUMERS into
non-materializable Dagster assets (AssetSpec, group "consumers") whose deps are the assets that
write the tables they read, so the lineage shows in the Dagster asset graph, and the Discord
sensors walk that same graph to say who a failed run leaves stale (sensors.py, pipeline/alerts.py).

Why a graph and not a job -> page table: a job does not map 1:1 to a surface. lightning_refresh's
kind-0 (profile-metadata) load feeds /zapstore's publisher names and the MCP zapstore domain as
well as /lightning, and the landing board reads five jobs. A new consumer is one entry here; the
edges follow from the tables it reads.

Keeping it true: the reads below were resolved from each route's data functions and their SQL
constants (2026-10-17). scripts/check_lineage.py re-resolves them from the nostrology-web data
layer and the MCP server and fails on any difference.
"""

# Asset -> the serving tables it (re)writes. Only assets that write something a consumer reads:
# the *_extract assets write S3 part sets, and their downstream assets turn those into tables.
PRODUCES = {
    "relays_table": ("events_kind_10002_relay_lists",),
    "relays_mv": ("relay_stats_mv", "relay_tier_user_counts_mv", "relay_per_user_distribution_mv"),
    "nutzaps_payments_table": ("events_kind_9321_nutzaps",),
    "nutzaps_configs_table": ("events_kind_10019_nutzap_configs",),
    "nutzaps_mint_classes": ("nutzap_mint_classes",),
    "nutzaps_active_users": ("active_user_counts_kind_9321_nutzaps",),
    "lightning_profiles_table": ("events_kind_0_profiles",),
    "lightning_zaps_table": ("events_kind_9735_zaps",),
    "lightning_signers": ("lightning_provider_signers",),
    "lightning_mv": ("lightning_daily_provider_mv", "lightning_provider_stats_mv"),
    # The catalog load writes BOTH: the listings that are Zapstore apps, and the ones that
    # are not with the reason in `excl`. The second is not read by a page or the MCP, but the
    # three loads below read it to decide what to drop, so a failed catalog load leaves them
    # following the previous refresh's exclusions - which is why it is declared here.
    "zapstore_catalog_table": ("events_kind_32267_app_catalog",
                               "events_kind_32267_app_catalog_excluded"),
    "zapstore_releases_table": ("events_kind_30063_app_releases",),
    "zapstore_stacks_table": ("events_kind_30267_app_stacks",),
    "zapstore_zaps_table": ("events_kind_9735_zaps_zapstore",),
    "marketplaces_snapshot": ("marketplaces_snapshot", "marketplaces_listings"),
    "routstr_snapshot": ("routstr_snapshot", "routstr_providers", "routstr_provider_days",
                         "routstr_model_days", "routstr_catalog_versions", "routstr_reviews"),
    "ngit_snapshot": ("ngit_snapshot", "ngit_repos", "ngit_pushes", "ngit_repo_stats"),
}

SITE = "https://nostrolo.gy"

# Each consumer: key (kind, name), the label an alert shows, an optional public URL (pages only;
# the MCP domains are never linked, so a click from Discord cannot become an unmarked MCP
# request), and the tables it reads. `sources` says where scripts/check_lineage.py finds its
# reads: route directories under nostrology-web src/routes/(app)/, landing data functions, or MCP
# modules.
CONSUMERS = (
    # The public pages, served by nostrology-web from Aurora.
    {"key": ("web", "zapstore"), "label": "/zapstore", "url": SITE + "/zapstore",
     "reads": ("events_kind_30063_app_releases", "events_kind_9735_zaps_zapstore",
               "events_kind_0_profiles", "events_kind_30267_app_stacks",
               "events_kind_32267_app_catalog"),
     "sources": {"routes": ("zapstore",)}},
    {"key": ("web", "lightning"), "label": "/lightning", "url": SITE + "/lightning",
     "reads": ("lightning_provider_stats_mv", "lightning_daily_provider_mv"),
     "sources": {"routes": ("lightning",)}},
    {"key": ("web", "nutzaps"), "label": "/nutzaps", "url": SITE + "/nutzaps",
     "reads": ("events_kind_9321_nutzaps", "events_kind_10019_nutzap_configs",
               "active_user_counts_kind_9321_nutzaps", "nutzap_mint_classes"),
     "sources": {"routes": ("nutzaps", "../api/nutzaps")}},
    {"key": ("web", "relays"), "label": "/relays", "url": SITE + "/relays",
     "reads": ("relay_stats_mv", "relay_tier_user_counts_mv", "relay_per_user_distribution_mv"),
     "sources": {"routes": ("relays",)}},
    {"key": ("web", "marketplaces"), "label": "/marketplaces", "url": SITE + "/marketplaces",
     # marketplaces_listings since 2026-10-24: the Recent listings section reads the newest
     # live rows of it straight, beside the snapshot document, the way /git reads
     # ngit_repo_stats rather than growing the document.
     "reads": ("marketplaces_snapshot", "marketplaces_listings"),
     "sources": {"routes": ("marketplaces",)}},
    {"key": ("web", "routstr"), "label": "/routstr", "url": SITE + "/routstr",
     "reads": ("routstr_snapshot",),
     "sources": {"routes": ("routstr",)}},
    {"key": ("web", "git"), "label": "/git", "url": SITE + "/git",
     "reads": ("ngit_snapshot", "ngit_repo_stats"),     # its Stars and Zaps lists since 2026-10-18
     "sources": {"routes": ("git",)}},
    # The landing board's panels (nostrology-web src/lib/server/landing/sources.ts), each read
    # hourly by the landing cache from the data function named in `sources`.
    {"key": ("landing", "marketplaces"), "label": "marketplaces", "url": SITE + "/",
     "reads": ("marketplaces_snapshot",),
     "sources": {"landing": ("getMarketplacesSnapshot",)}},
    {"key": ("landing", "git"), "label": "git", "url": SITE + "/",
     "reads": ("ngit_snapshot",),
     "sources": {"landing": ("getNgitSnapshot",)}},
    {"key": ("landing", "zapstore"), "label": "zapstore", "url": SITE + "/",
     "reads": ("events_kind_30063_app_releases",),
     "sources": {"landing": ("getZapstoreAppsCumulativeDaily",)}},
    {"key": ("landing", "nutzapsDaily"), "label": "nutzapsDaily", "url": SITE + "/",
     "reads": ("events_kind_9321_nutzaps", "nutzap_mint_classes"),
     "sources": {"landing": ("getNutzapsDailySeries",)}},
    {"key": ("landing", "nutzapMints"), "label": "nutzapMints", "url": SITE + "/",
     "reads": ("events_kind_9321_nutzaps", "events_kind_10019_nutzap_configs",
               "nutzap_mint_classes"),
     "sources": {"landing": ("getNutzapMintStats",)}},
    {"key": ("landing", "lightningProviders"), "label": "lightningProviders", "url": SITE + "/",
     "reads": ("lightning_provider_stats_mv",),
     "sources": {"landing": ("getLightningProviderStats",)}},
    {"key": ("landing", "lightningShare"), "label": "lightningShare", "url": SITE + "/",
     "reads": ("lightning_daily_provider_mv",),
     "sources": {"landing": ("getLightningTrendsSatsShare",)}},
    # The public MCP server's domains (nostrology_mcp/metric.py and reference.py).
    {"key": ("mcp", "zapstore"), "label": "zapstore",
     "reads": ("events_kind_30063_app_releases", "events_kind_30267_app_stacks",
               "events_kind_32267_app_catalog", "events_kind_9735_zaps_zapstore",
               "events_kind_0_profiles"),
     "sources": {"mcp": ("metric.py", "reference.py")}},
    {"key": ("mcp", "nutzaps"), "label": "nutzaps",
     "reads": ("events_kind_9321_nutzaps", "events_kind_10019_nutzap_configs",
               "active_user_counts_kind_9321_nutzaps", "nutzap_mint_classes"),
     "sources": {"mcp": ("metric.py",)}},
    {"key": ("mcp", "routstr"), "label": "routstr",
     "reads": ("routstr_snapshot", "routstr_providers", "routstr_provider_days",
               "routstr_model_days", "routstr_catalog_versions", "routstr_reviews"),
     "sources": {"mcp": ("metric.py", "reference.py")}},
)

# Routes that read serving tables but are deliberately NOT consumers, with the reason; the drift
# check accepts them instead of reporting an undeclared read.
NOT_CONSUMERS = {
    "sitemap.xml": "lists app and mint URLs for crawlers; a stale copy only delays new URLs",
}

# The route that renders the landing board (under src/routes/): its reads are the landing panels',
# so the drift check holds it to their declared reads instead of naming it a consumer of its own.
LANDING_ROUTES = ("+page.server.ts",)

# Display order for the alert's "Affected" lines.
KIND_ORDER = ("web", "landing", "mcp")


def consumer_id(key) -> str:
    """A consumer's node id in the lineage graph: its Dagster asset key as a user string."""
    return "/".join(("consumer",) + tuple(key))


def table_producers() -> dict:
    """table -> the one asset that writes it. Raises on a table claimed by two assets."""
    out = {}
    for asset, tables in PRODUCES.items():
        for t in tables:
            if t in out:
                raise ValueError(f"{t} is written by both {out[t]} and {asset}")
            out[t] = asset
    return out


def consumer_deps(consumer, producers=None) -> list:
    """The node ids a consumer depends on: the producing asset of every table it reads. Raises
    KeyError on a table nothing produces."""
    producers = producers if producers is not None else table_producers()
    deps = []
    for t in consumer["reads"]:
        a = producers[t]
        if a not in deps:
            deps.append(a)
    return deps


def consumer_upstream() -> dict:
    """consumer node id -> its direct upstream node ids (the assets that write what it reads)."""
    producers = table_producers()
    return {consumer_id(c["key"]): set(consumer_deps(c, producers)) for c in CONSUMERS}


def consumer_labels() -> dict:
    """consumer node id -> (kind, label, url or None), for the alert text."""
    return {consumer_id(c["key"]): (c["key"][0], c["label"], c.get("url")) for c in CONSUMERS}


def problems() -> list:
    """Internal consistency, as strings (empty = consistent): every read has a producer, keys are
    unique, and kinds are known."""
    out = []
    try:
        producers = table_producers()
    except ValueError as exc:
        return [str(exc)]
    ids = [consumer_id(c["key"]) for c in CONSUMERS]
    if len(ids) != len(set(ids)):
        out.append("duplicate consumer keys")
    for c in CONSUMERS:
        cid = consumer_id(c["key"])
        if c["key"][0] not in KIND_ORDER:
            out.append(f"{cid}: unknown kind {c['key'][0]!r}")
        for t in c["reads"]:
            if t not in producers:
                out.append(f"{cid} reads {t}, which no asset in PRODUCES writes")
        if not c["reads"]:
            out.append(f"{cid} reads nothing")
    return out
