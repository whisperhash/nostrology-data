"""The MCP calls the Routstr battery is built on.

Ids are `RS..` so a run's raw fetches never collide with the Zapstore plan's in
the shared `raw/<date>/` cache. Every subject (top provider, an excluded provider,
the top model, a model folded out of the leaderboard, a reviewed node) and every
window is derived at run time from the data - see `routstr_run.build_context` -
so nothing here goes stale when the nightly refresh moves.

The calls are shaped after how a model client uses the tools: "last 7 days"
and "last 30 days" windows computed from TODAY, the refresh-anchored 30-day
window that reproduces the page, "this month" from the month's first day, one
well-scoped call with an entity filter rather than a dump.
"""
METRIC = "nostrology_query_metric"
REFERENCE = "nostrology_query_reference"
D = {"domain": "routstr"}


def build(ctx):
    m, r = METRIC, REFERENCE
    top, exc, tmodel, fmodel, rev = (ctx["top_provider"], ctx["excluded_provider"],
                                     ctx["top_model"], ctx["folded_model"], ctx["reviewed_node"])
    r30f, r30t = ctx["refresh_30d_from"], ctx["refresh_30d_to"]
    l7f, l7t = ctx["last7_from"], ctx["last7_to"]
    l30f, l30t = ctx["last30_from"], ctx["last30_to"]
    mf = ctx["month_from"]
    return [
        # -- network ----------------------------------------------------------
        {"id": "RS01", "tool": m, "args": {**D, "metric": "network_summary"}},
        {"id": "RS02", "tool": m, "args": {**D, "metric": "network_activity"}},
        {"id": "RS03", "tool": m, "args": {**D, "metric": "network_activity", "from": l7f, "to": l7t}},
        {"id": "RS04", "tool": m, "args": {**D, "metric": "network_activity", "from": l30f, "to": l30t}},
        {"id": "RS05", "tool": m, "args": {**D, "metric": "network_activity", "from": mf}},
        {"id": "RS06", "tool": m, "args": {**D, "metric": "cumulative_revenue"}},
        {"id": "RS07", "tool": m, "args": {**D, "metric": "cumulative_revenue", "from": r30f, "to": r30t}},
        {"id": "RS08", "tool": m, "args": {**D, "metric": "success_rate"}},
        # -- providers --------------------------------------------------------
        {"id": "RS10", "tool": m, "args": {**D, "metric": "provider_leaderboard"}},
        {"id": "RS11", "tool": m, "args": {**D, "metric": "provider_leaderboard", "provider_pubkey": top}},
        {"id": "RS12", "tool": m, "args": {**D, "metric": "provider_leaderboard", "provider_pubkey": exc}},
        {"id": "RS13", "tool": m, "args": {**D, "metric": "provider_activity", "grain": "total"}},
        {"id": "RS14", "tool": m, "args": {**D, "metric": "provider_activity", "grain": "total",
                                           "from": r30f, "to": r30t}},
        {"id": "RS15", "tool": m, "args": {**D, "metric": "provider_activity", "grain": "total",
                                           "from": l7f, "to": l7t}},
        {"id": "RS16", "tool": m, "args": {**D, "metric": "provider_activity", "grain": "total",
                                           "from": l30f, "to": l30t}},
        {"id": "RS17", "tool": m, "args": {**D, "metric": "provider_activity", "grain": "day",
                                           "provider_pubkey": top}},
        {"id": "RS18", "tool": m, "args": {**D, "metric": "provider_activity", "grain": "day"}},
        {"id": "RS19", "tool": m, "args": {**D, "metric": "provider_activity", "grain": "total",
                                           "provider_pubkey": exc}},
        {"id": "RS20", "tool": m, "args": {**D, "metric": "provider_activity", "grain": "day",
                                           "provider_pubkey": top, "from": l7f, "to": l7t}},
        # -- models -----------------------------------------------------------
        {"id": "RS21", "tool": m, "args": {**D, "metric": "model_leaderboard"}},
        {"id": "RS22", "tool": m, "args": {**D, "metric": "model_leaderboard", "model": tmodel}},
        {"id": "RS23", "tool": m, "args": {**D, "metric": "model_activity", "grain": "total"}},
        {"id": "RS24", "tool": m, "args": {**D, "metric": "model_activity", "grain": "total",
                                           "from": r30f, "to": r30t}},
        {"id": "RS25", "tool": m, "args": {**D, "metric": "model_activity", "grain": "total",
                                           "from": l7f, "to": l7t}},
        {"id": "RS26", "tool": m, "args": {**D, "metric": "model_activity", "grain": "day", "model": tmodel}},
        {"id": "RS27", "tool": m, "args": {**D, "metric": "model_activity", "grain": "day"}},
        {"id": "RS28", "tool": m, "args": {**D, "metric": "model_activity", "grain": "total", "model": fmodel}},
        {"id": "RS29", "tool": m, "args": {**D, "metric": "model_leaderboard", "model": "(other)"}},
        {"id": "RS30", "tool": m, "args": {**D, "metric": "catalog_size"}},
        {"id": "RS31", "tool": m, "args": {**D, "metric": "provider_facets"}},
        # -- reference records ------------------------------------------------
        {"id": "RS40", "tool": r, "args": {**D, "record": "provider_profile"}},
        {"id": "RS41", "tool": r, "args": {**D, "record": "provider_profile", "provider_pubkey": top}},
        {"id": "RS42", "tool": r, "args": {**D, "record": "provider_profile", "provider_pubkey": exc}},
        {"id": "RS43", "tool": r, "args": {**D, "record": "provider_reviews"}},
        {"id": "RS44", "tool": r, "args": {**D, "record": "provider_reviews", "provider_pubkey": rev}},
        {"id": "RS45", "tool": r, "args": {**D, "record": "provider_reviews", "from": mf}},
        {"id": "RS46", "tool": r, "args": {**D, "record": "catalog_versions"}},
        {"id": "RS47", "tool": r, "args": {**D, "record": "catalog_versions", "from": mf}},
        {"id": "RS48", "tool": r, "args": {**D, "record": "model_catalog"}},
        {"id": "RS49", "tool": r, "args": {**D, "record": "exclusions"}},
        # -- contract: domain routing, rejections, caps, formats ---------------
        {"id": "RS50", "tool": m, "args": {"metric": "network_summary"}},
        {"id": "RS51", "tool": m, "args": {"metric": "provider_leaderboard"}},
        {"id": "RS52", "tool": r, "args": {"record": "provider_profile"}},
        {"id": "RS53", "tool": m, "args": {"metric": "zap_count", "provider_pubkey": top}},
        {"id": "RS54", "tool": m, "args": {**D, "metric": "provider_leaderboard",
                                           "publisher_pubkey": top}},
        {"id": "RS55", "tool": m, "args": {**D, "metric": "provider_leaderboard", "from": l7f, "to": l7t}},
        {"id": "RS56", "tool": m, "args": {**D, "metric": "network_activity", "grain": "total"}},
        {"id": "RS57", "tool": m, "args": {**D, "metric": "provider_activity", "grain": "current"}},
        {"id": "RS58", "tool": m, "args": {**D, "metric": "network_activity", "from": l7t, "to": l7f}},
        {"id": "RS59", "tool": m, "args": {**D, "metric": "provider_activity", "grain": "total",
                                           "provider_pubkey": "0" * 64}},
        {"id": "RS60", "tool": m, "args": {**D, "metric": "provider_leaderboard",
                                           "provider_pubkey": "npub1" + "q" * 58}},
        {"id": "RS61", "tool": m, "args": {**D, "metric": "model_activity", "grain": "total",
                                           "model": "x" * 61}},
        {"id": "RS62", "tool": r, "args": {**D, "record": "model_catalog", "from": mf}},
        {"id": "RS63", "tool": r, "args": {**D, "record": "provider_profile", "publisher_pubkey": top}},
        {"id": "RS64", "tool": m, "args": {**D, "metric": "network_activity", "from": l7f, "to": l7t,
                                           "format": "markdown"}},
        {"id": "RS65", "tool": m, "args": {"domain": "zapstore", "metric": "network_summary"}},
        {"id": "RS66", "tool": m, "args": {**D, "metric": "total_downloads"}},
        {"id": "RS67", "tool": m, "args": {**D, "metric": "network_activity",
                                           "from": "2099-01-01", "to": "2099-01-31"}},
        {"id": "RS68", "tool": r, "args": {**D, "record": "provider_profile",
                                           "provider_pubkey": "0" * 64}},
        {"id": "RS69", "tool": m, "args": {**D, "metric": "model_activity", "grain": "total",
                                           "model": "no-such-model-" + "z" * 20}},
    ]

