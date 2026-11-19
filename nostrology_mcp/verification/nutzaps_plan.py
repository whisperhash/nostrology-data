"""The MCP calls the nutzaps battery is built on.

Ids are `NZ..` so a run's raw fetches never collide with the Zapstore (`M..`) or
Routstr (`RS..`) plans in the shared `raw/<date>/` cache. Every subject (the top
mint by all-time sats, a mint with configs but few payments, a config-only mint,
an excluded mint) and every window is derived at run time from the SQL truth in
`nutzaps_run.build_context` - nothing here goes stale when the nightly refresh
moves.

Windows, the way a caller asks them: "last 7 days" / "last 30 days" as the
complete days before today, "this month" from its 1st with an open-ended `to`, a
past full calendar month, and the web's trailing 365 days twice - from its partial
oldest day D0 (the OUTER bracket of the page's rolling cutoff) and from D0 + 1
(the INNER one). The page's rolling 7d / 30d periods get the same inner/outer pair.
"""
METRIC = "nostrology_query_metric"
PAID = "nostrology_query_nutzaps"
D = {"domain": "nutzaps"}
# Well-formed, normalizes to itself, and can never be a stored mint (.invalid is
# reserved, RFC 2606) - the "nothing matches the filter" probe.
NO_SUCH_MINT = "https://no-such-mint.invalid"


def split_url(url):
    """(scheme, userinfo@, host[:port], tail) of an http(s) URL - no normalization."""
    scheme, _, rest = url.partition("://")
    cut = min([i for i in (rest.find("/"), rest.find("?"), rest.find("#")) if i >= 0] or [len(rest)])
    netloc, tail = rest[:cut], rest[cut:]
    user, at, hostport = netloc.rpartition("@")
    return scheme, user + at, hostport, tail


def variants(url):
    """Spellings of a stored mint URL that the documented normalization folds back
    to it: host case, scheme case, a trailing slash, a missing scheme (read as
    https) and an explicit default port. Only the ones that apply to this URL."""
    scheme, user, hostport, tail = split_url(url)
    out = {
        "host_case": f"{scheme}://{user}{hostport.upper()}{tail}",
        "scheme_case": f"{scheme.upper()}://{user}{hostport}{tail}",
        "trailing_slash": url + "/",
    }
    if scheme == "https":
        out["no_scheme"] = f"{user}{hostport}{tail}"
        if ":" not in hostport.rsplit("]", 1)[-1]:
            out["default_port"] = f"https://{user}{hostport}:443{tail}"
    elif scheme == "http" and ":" not in hostport.rsplit("]", 1)[-1]:
        out["default_port"] = f"http://{user}{hostport}:80{tail}"
    return {k: v for k, v in out.items() if v != url}


def path_case_variant(url):
    """The same URL with its PATH lowercased - a DIFFERENT mint by the documented
    rule (path case kept), or None when the path has no upper-case letter."""
    scheme, user, hostport, tail = split_url(url)
    return f"{scheme}://{user}{hostport}{tail.lower()}" if tail.lower() != tail else None


def build(ctx):
    m, p = METRIC, PAID
    top, few, cfg_only, exc = ctx["top_mint"], ctx["few_mint"], ctx["config_only_mint"], ctx["excluded_mint"]
    l7f, l7t, l30f, l30t = ctx["last7_from"], ctx["last7_to"], ctx["last30_from"], ctx["last30_to"]
    mf, pmf, pmt = ctx["month_from"], ctx["past_month_from"], ctx["past_month_to"]
    d0, d1 = ctx["web_d0"], ctx["web_d1"]
    r7o, r7i, r30o, r30i = ctx["roll7_outer"], ctx["roll7_inner"], ctx["roll30_outer"], ctx["roll30_inner"]
    ef, et = ctx["empty_from"], ctx["empty_to"]
    steps = [
        # -- payment_activity ------------------------------------------------
        {"id": "NZ01", "tool": m, "args": {**D, "metric": "payment_activity"}},  # grain omitted -> day
        {"id": "NZ02", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "day", "from": l7f, "to": l7t}},
        {"id": "NZ03", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "day", "from": l30f, "to": l30t}},
        {"id": "NZ04", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "day", "from": mf}},
        {"id": "NZ05", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "day", "from": pmf, "to": pmt}},
        {"id": "NZ06", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "day", "from": d0}},
        {"id": "NZ07", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "total"}},
        {"id": "NZ08", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "total", "from": l7f, "to": l7t}},
        {"id": "NZ09", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "total", "from": l30f, "to": l30t}},
        {"id": "NZ10", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "total", "from": mf}},
        {"id": "NZ11", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "total", "from": pmf, "to": pmt}},
        {"id": "NZ12", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "total", "from": d0}},
        {"id": "NZ13", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "total", "from": d1}},
        {"id": "NZ14", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "total", "from": ef, "to": et}},
        {"id": "NZ15", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "day", "mint_url": top}},
        {"id": "NZ16", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "total", "mint_url": top}},
        {"id": "NZ17", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "total", "mint_url": top, "from": d0}},
        {"id": "NZ18", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "total", "mint_url": top, "from": d1}},
        {"id": "NZ19", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "day", "mint_url": few}},
        {"id": "NZ20", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "total", "mint_url": few}},
        {"id": "NZ21", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "day", "mint_url": top,
                                           "from": l30f, "to": l30t}},
        # -- mint_leaderboard --------------------------------------------------
        {"id": "NZ30", "tool": m, "args": {**D, "metric": "mint_leaderboard"}},  # grain omitted -> total
        {"id": "NZ31", "tool": m, "args": {**D, "metric": "mint_leaderboard", "from": l7f, "to": l7t}},
        {"id": "NZ32", "tool": m, "args": {**D, "metric": "mint_leaderboard", "from": l30f, "to": l30t}},
        {"id": "NZ33", "tool": m, "args": {**D, "metric": "mint_leaderboard", "from": mf}},
        {"id": "NZ34", "tool": m, "args": {**D, "metric": "mint_leaderboard", "from": pmf, "to": pmt}},
        {"id": "NZ35", "tool": m, "args": {**D, "metric": "mint_leaderboard", "grain": "total", "from": d0}},
        {"id": "NZ36", "tool": m, "args": {**D, "metric": "mint_leaderboard", "grain": "total", "from": d1}},
        {"id": "NZ37", "tool": m, "args": {**D, "metric": "mint_leaderboard", "from": r7o}},
        {"id": "NZ38", "tool": m, "args": {**D, "metric": "mint_leaderboard", "from": r7i}},
        {"id": "NZ39", "tool": m, "args": {**D, "metric": "mint_leaderboard", "from": r30o}},
        {"id": "NZ40", "tool": m, "args": {**D, "metric": "mint_leaderboard", "from": r30i}},
        {"id": "NZ41", "tool": m, "args": {**D, "metric": "mint_leaderboard", "mint_url": top}},
        {"id": "NZ42", "tool": m, "args": {**D, "metric": "mint_leaderboard", "mint_url": few}},
        {"id": "NZ43", "tool": m, "args": {**D, "metric": "mint_leaderboard", "mint_url": cfg_only or few}},
        {"id": "NZ44", "tool": m, "args": {**D, "metric": "mint_leaderboard", "mint_url": top, "from": d1}},
        # -- mint_activity -----------------------------------------------------
        {"id": "NZ50", "tool": m, "args": {**D, "metric": "mint_activity"}},  # grain omitted -> day
        {"id": "NZ51", "tool": m, "args": {**D, "metric": "mint_activity", "from": l30f, "to": l30t}},
        {"id": "NZ52", "tool": m, "args": {**D, "metric": "mint_activity", "mint_url": top}},
        {"id": "NZ53", "tool": m, "args": {**D, "metric": "mint_activity", "grain": "day", "from": d0}},
        # -- mint_active_users -------------------------------------------------
        {"id": "NZ60", "tool": m, "args": {**D, "metric": "mint_active_users"}},  # grain omitted -> month
        {"id": "NZ61", "tool": m, "args": {**D, "metric": "mint_active_users", "grain": "month", "mint_url": top}},
        {"id": "NZ62", "tool": m, "args": {**D, "metric": "mint_active_users", "grain": "day"}},
        {"id": "NZ63", "tool": m, "args": {**D, "metric": "mint_active_users", "grain": "week"}},
        {"id": "NZ64", "tool": m, "args": {**D, "metric": "mint_active_users", "grain": "month", "mint_url": few}},
        {"id": "NZ65", "tool": m, "args": {**D, "metric": "mint_active_users", "grain": "month", "from": pmf, "to": pmt}},
        {"id": "NZ66", "tool": m, "args": {**D, "metric": "mint_active_users", "grain": "day", "mint_url": top,
                                           "from": l30f, "to": l30t}},
        {"id": "NZ67", "tool": m, "args": {**D, "metric": "mint_active_users", "grain": "week", "mint_url": top}},
        # a window that starts on the 2nd: no month bucket STARTS inside it
        {"id": "NZ68", "tool": m, "args": {**D, "metric": "mint_active_users", "grain": "month",
                                           "from": ctx["past_month_second"], "to": pmt}},
        # a window that starts on a Wednesday: the week that began that Monday is left out whole
        {"id": "NZ69", "tool": m, "args": {**D, "metric": "mint_active_users", "grain": "week",
                                           "from": ctx["midweek_from"], "to": l7t}},
        # -- routing, rejections -----------------------------------------------
        {"id": "NZ70", "tool": m, "args": {"metric": "payment_activity"}},
        {"id": "NZ71", "tool": m, "args": {"metric": "mint_leaderboard"}},
        {"id": "NZ72", "tool": m, "args": {**D, "metric": "active_users"}},
        {"id": "NZ73", "tool": m, "args": {"metric": "active_users"}},
        {"id": "NZ74", "tool": m, "args": {**D, "metric": "payment_activity", "mint_url": "ftp://mint.example.com"}},
        {"id": "NZ75", "tool": m, "args": {**D, "metric": "payment_activity", "mint_url": "https://mint.example.com:99999"}},
        {"id": "NZ76", "tool": m, "args": {**D, "metric": "payment_activity",
                                           "mint_url": "https://mint.example.com/" + "a" * 1100}},
        {"id": "NZ77", "tool": m, "args": {"metric": "zap_count", "mint_url": top}},
        {"id": "NZ78", "tool": m, "args": {**D, "metric": "payment_activity", "provider_pubkey": "0" * 64}},
        {"id": "NZ79", "tool": m, "args": {**D, "metric": "mint_leaderboard", "publisher_pubkey": "0" * 64}},
        {"id": "NZ80", "tool": m, "args": {**D, "metric": "payment_activity", "from": l7t, "to": l7f}},
        {"id": "NZ81", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "week"}},
        {"id": "NZ82", "tool": m, "args": {**D, "metric": "mint_leaderboard", "grain": "day"}},
        {"id": "NZ83", "tool": m, "args": {**D, "metric": "mint_activity", "grain": "total"}},
        {"id": "NZ84", "tool": m, "args": {**D, "metric": "payment_activity", "from": "2099-01-01", "to": "2099-01-31"}},
        {"id": "NZ85", "tool": m, "args": {"domain": "routstr", "metric": "network_activity", "mint_url": top}},
        {"id": "NZ86", "tool": m, "args": {**D, "metric": "payment_activity", "from": l7f, "to": l7t,
                                           "format": "markdown"}},
        # mint_active_users serves day | week | month; total is not a bucket. (NZ87 / NZ88 asked
        # for day / week while they were withdrawn; retired with the 2026-09-30 decision.)
        {"id": "NZ89", "tool": m, "args": {**D, "metric": "mint_active_users", "grain": "total"}},
        # -- the excluded mint, by filter (raw rows exist; none may come back) ----
        {"id": "NZ100", "tool": m, "args": {**D, "metric": "payment_activity", "mint_url": exc}},
        {"id": "NZ101", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "total", "mint_url": exc}},
        {"id": "NZ102", "tool": m, "args": {**D, "metric": "mint_leaderboard", "mint_url": exc}},
        {"id": "NZ103", "tool": m, "args": {**D, "metric": "mint_activity", "mint_url": exc}},
        {"id": "NZ104", "tool": m, "args": {**D, "metric": "mint_active_users", "mint_url": exc}},
        # -- a well-formed mint URL the archive has never seen ---------------------
        {"id": "NZ105", "tool": m, "args": {**D, "metric": "payment_activity", "mint_url": NO_SUCH_MINT}},
        {"id": "NZ106", "tool": m, "args": {**D, "metric": "payment_activity", "grain": "total", "mint_url": NO_SUCH_MINT}},
        {"id": "NZ107", "tool": m, "args": {**D, "metric": "mint_leaderboard", "mint_url": NO_SUCH_MINT}},
        {"id": "NZ108", "tool": m, "args": {**D, "metric": "mint_activity", "mint_url": NO_SUCH_MINT}},
        {"id": "NZ109", "tool": m, "args": {**D, "metric": "mint_active_users", "mint_url": NO_SUCH_MINT}},
        # -- the longest live mint URL (> 256 chars) must round-trip whole ------------
        {"id": "NZ45", "tool": m, "args": {**D, "metric": "mint_leaderboard", "mint_url": ctx.get("long_mint"),
                                           "from": ctx.get("long_mint_from")}},
        {"id": "NZ46", "tool": m, "args": {**D, "metric": "payment_activity", "mint_url": ctx.get("long_mint")}},
        # -- the PAID tool, without a token (costs nothing) --------------------------
        {"id": "NZ110", "tool": p, "args": {"metric": "active_users"}},
        {"id": "NZ111", "tool": p, "args": {"metric": "active_users", "grain": "month", "from": pmf, "to": pmt}},
    ]
    # -- normalization: every spelling of the normalization subject folds to it ----
    for n, (kind, url) in enumerate(sorted(variants(ctx["norm_mint"]).items())):
        steps.append({"id": f"NZ9{n}", "tool": m, "variant": kind,
                      "args": {**D, "metric": "payment_activity", "mint_url": url}})
    steps.append({"id": "NZ98", "tool": m, "args": {**D, "metric": "payment_activity", "mint_url": ctx["norm_mint"]}})
    if ctx.get("path_case_mint"):
        steps.append({"id": "NZ99", "tool": m, "variant": "path_case",
                      "args": {**D, "metric": "payment_activity", "mint_url": path_case_variant(ctx["path_case_mint"])}})
    # A subject the data does not have today (no excluded mint, no config-only mint)
    # drops its calls rather than sending an unfiltered call under a filtered id.
    return [s for s in steps if None not in s["args"].values()]

