"""The battery.

Add questions here. Two rules keep a run trustworthy:

1. **Never hard-code an expected value.** Both sides must be recomputed from the
   fetched data on every run - `want` from `T` (SQL) or `W` (the live site), `got`
   from the MCP responses. A literal in `want` is a value that will silently rot
   the next time Dagster refreshes the serving tables.
2. **Say how it was verified.** The `how` string is what makes the report readable
   as evidence rather than as a green tick, and it is what a future reader uses to
   judge whether the check is actually independent.

`ask()` compares for equality. `ask_bounded()` is for the one case where MCP
*cannot* be exact by construction - see the chart-3 boundary-day note.
"""
from collections import defaultdict

from . import subjects as S


# ---------------------------------------------------------------------------
# Ground-truth views over the SQL sections
# ---------------------------------------------------------------------------
class Truth:
    def __init__(self, sql):
        self.summary = sql["SUMMARY"][0]
        self.rel = {r["d"]: (r["native"], r["republished"]) for r in sql["DAILY_RELEASES"]}
        self.new = {r["d"]: (r["native"], r["republished"]) for r in sql["DAILY_NEWAPPS"]}
        self.zap_rows = sql["APP_ZAPS_DAILY"]
        self.stacks = {(r["pubkey"], r["d_tag"]): r["stacks"] for r in sql["APP_STACKS"]}
        self.pub_apps = {r["pubkey"]: r["apps"] for r in sql["PUBLISHER_APPS"]}
        self.catalog = sql["CATALOG"]
        self.rel_by_app = {(r["pubkey"], r["d_tag"]): r for r in sql["RELEASES_BY_APP"]}
        self.subject_releases = sql["SUBJECT_RELEASES"]
        self.profiles = sql["PROFILES"]

    # -- daily series (sparse, matching the metrics' own contract) ----------
    @property
    def nat_new(self): return {d: v[0] for d, v in self.new.items() if v[0]}
    @property
    def rep_new(self): return {d: v[1] for d, v in self.new.items() if v[1]}
    @property
    def nat_rel(self): return {d: v[0] for d, v in self.rel.items() if v[0]}
    @property
    def rep_rel(self): return {d: v[1] for d, v in self.rel.items() if v[1]}

    def zap_count(self, frm=None, to=None):
        out = defaultdict(int)
        for r in self._zaps(frm, to):
            out[r["d"]] += r["zaps"]
        return dict(out)

    def zap_msats(self, frm=None, to=None):
        out = defaultdict(int)
        for r in self._zaps(frm, to):
            out[r["d"]] += r["msats"]
        return dict(out)

    def _zaps(self, frm=None, to=None):
        return [r for r in self.zap_rows
                if (frm is None or r["d"] >= frm) and (to is None or r["d"] <= to)]

    def app_zaps(self, frm=None, to=None):
        """(pubkey, d_tag) -> {zaps, msats, is_native} over the window."""
        out = {}
        for r in self._zaps(frm, to):
            k = (r["pubkey"], r["d_tag"])
            cur = out.setdefault(k, {"zaps": 0, "msats": 0, "is_native": r["is_native"]})
            cur["zaps"] += r["zaps"]
            cur["msats"] += r["msats"]
        return out

    def catalog_of(self, pubkey, d_tag=None):
        return [r for r in self.catalog
                if r["pubkey"] == pubkey and (d_tag is None or r["d_tag"] == d_tag)]

    def releases_of(self, pubkey, d_tag=None, frm=None, to=None):
        return [r for r in self.subject_releases
                if r["pubkey"] == pubkey and (d_tag is None or r["d_tag"] == d_tag)
                and (frm is None or r["d"] >= frm) and (to is None or r["d"] <= to)]

    def profile_of(self, pubkey):
        return next((r for r in self.profiles if r["pubkey"] == pubkey), None)


def wsum(series, frm=None, to=None):
    return sum(v for d, v in series.items()
               if (frm is None or d >= frm) and (to is None or d <= to))


def argmax(series):
    """(day, value) of the largest value; ties break on the later day, both sides."""
    return max(series.items(), key=lambda kv: (kv[1], kv[0]))


# ---------------------------------------------------------------------------
# Battery
# ---------------------------------------------------------------------------
def build(M, sql, W, ctx):
    """M: id -> Resp. sql: SQL sections. W: live-site values. ctx: run context."""
    T = Truth(sql)
    Q = []

    def ask(cat, question, calls, got, want, how):
        Q.append(dict(n=len(Q) + 1, cat=cat, q=question, calls=calls,
                      got=got, want=want, how=how, bounded=False,
                      ok=(bool(got) == want) if isinstance(want, bool) else (got == want)))

    def ask_bounded(cat, question, calls, low, high, want, how):
        Q.append(dict(n=len(Q) + 1, cat=cat, q=question, calls=calls,
                      got=f"{low} (day excluded) .. {high} (day included)", want=want,
                      how=how, bounded=True, ok=(low <= want <= high)))

    m01, m02 = M["M01"].series(), M["M02"].series()
    m03, m04 = M["M03"].series(), M["M04"].series()
    m05, m06 = M["M05"].series(), M["M06"].series()
    m07 = M["M07"].dicts()
    m09 = M["M09"].dicts()
    m10 = M["M10"].dicts()
    LAST = ctx["last_data_day"]

    # ---- Chart 1: cumulative apps ----------------------------------------
    C = "Chart 1 - Cumulative apps"
    ask(C, "How many Nostr-native apps has Zapstore ever had (cumulative, all history)?",
        ["M01"], sum(m01.values()), sum(T.nat_new.values()),
        "SUM(new_native_app_count) vs SQL COUNT(DISTINCT pubkey,app_d_tag) over the native "
        "partition of kind-30063")
    ask(C, "How many republished (GitHub / F-Droid) apps, cumulative all history?",
        ["M02"], sum(m02.values()), sum(T.rep_new.values()),
        "SUM(new_republished_app_count) vs the SQL distinct-app count in the curator partition")
    ask(C, "What is the total Zapstore catalog size (the chart's 'Total' line endpoint)?",
        ["M01", "M02"], sum(m01.values()) + sum(m02.values()),
        sum(T.nat_new.values()) + sum(T.rep_new.values()),
        "native + republished cumulative vs SQL total distinct apps with a release")
    ask(C, "Are the two partitions exact complements (no overlap, no gap)?",
        ["M01", "M02"], sum(m01.values()) + sum(m02.values()),
        T.summary["apps_with_release"],
        "MCP pair sum vs SQL COUNT(DISTINCT pubkey,app_d_tag) over ALL of kind-30063")
    ask(C, "How many native apps existed cumulatively as of the end of last year?",
        ["M01"], wsum(m01, to=ctx["last_year_end"]), wsum(T.nat_new, to=ctx["last_year_end"]),
        "running sum truncated at the year boundary, both sides")
    ask(C, "How many republished apps existed cumulatively as of the end of last year?",
        ["M02"], wsum(m02, to=ctx["last_year_end"]), wsum(T.rep_new, to=ctx["last_year_end"]),
        "running sum truncated at the year boundary, both sides")
    ask(C, "On what day did the first Nostr-native app appear?",
        ["M01"], min(m01), min(T.nat_new), "MIN(utc_day) vs SQL MIN(first-release day), native partition")
    ask(C, "On what day does the republished series start (the pre-Nostr upstream tail)?",
        ["M02"], min(m02), min(T.rep_new),
        "MIN(utc_day) vs SQL; expected to predate Nostr, since curator republishes carry "
        "upstream release dates")
    ask(C, "Which single day added the most new native apps, and how many?",
        ["M01"], argmax(m01), argmax(T.nat_new), "argmax over the series vs SQL")
    ask(C, "Which single day added the most new republished apps, and how many?",
        ["M02"], argmax(m02), argmax(T.rep_new), "argmax over the series vs SQL")
    ask(C, "How many distinct days saw at least one new native app?",
        ["M01"], len(m01), len(T.nat_new), "sparse row count vs SQL distinct first-release days")
    ask(C, "Is the series sparse (zero-days omitted) as documented?",
        ["M01"], all(v > 0 for v in m01.values()), True, "no zero rows in the returned CSV")

    # ---- Chart 2: native share -------------------------------------------
    C = "Chart 2 - Native share"

    def share(nat, rep, upto):
        n, r = wsum(nat, to=upto), wsum(rep, to=upto)
        return round(100 * n / (n + r), 2) if n + r else None

    ask(C, "What share of the catalog is Nostr-native as of the latest data day (%)?",
        ["M01", "M02"], share(m01, m02, LAST), share(T.nat_new, T.rep_new, LAST),
        "ratio of the two running sums vs the same ratio computed from SQL")
    ask(C, "What was the Nostr-native share a year before the latest data day (%)?",
        ["M01", "M02"], share(m01, m02, ctx["year_ago"]), share(T.nat_new, T.rep_new, ctx["year_ago"]),
        "running sums truncated a year back, ratio, both sides")

    def first_over(nat, rep, pct):
        days = sorted(set(nat) | set(rep))
        cn = cr = 0
        for d in days:
            cn += nat.get(d, 0)
            cr += rep.get(d, 0)
            if cn + cr and 100 * cn / (cn + cr) >= pct:
                return d
        return None

    ask(C, "On what day did the native share first reach 10%?",
        ["M01", "M02"], first_over(m01, m02, 10), first_over(T.nat_new, T.rep_new, 10),
        "walk both cumulative series to the crossing day, vs the SQL equivalent")
    ask(C, "Is the native share higher now than a year ago?",
        ["M01", "M02"], share(m01, m02, LAST) > share(m01, m02, ctx["year_ago"]),
        share(T.nat_new, T.rep_new, LAST) > share(T.nat_new, T.rep_new, ctx["year_ago"]),
        "direction of travel computed on both sides")

    # ---- Chart 3: daily releases -----------------------------------------
    C = "Chart 3 - App releases"
    MF, MT = S.MONTH_FROM, S.MONTH_TO
    ask(C, "How many Nostr-native release events all time?",
        ["M03"], sum(m03.values()), sum(T.nat_rel.values()),
        "SUM(native_release_count) vs SQL COUNT(*) over the native partition")
    ask(C, "How many republished release events all time?",
        ["M04"], sum(m04.values()), sum(T.rep_rel.values()),
        "SUM(republished_release_count) vs SQL COUNT(*) over the curator partition")
    ask(C, "How many release events in total (both partitions)?",
        ["M03", "M04"], sum(m03.values()) + sum(m04.values()), T.summary["release_rows"],
        "MCP pair sum vs SQL COUNT(*) on events_kind_30063_app_releases")
    ask(C, f"How many native releases were published in the sample month ({MF[:7]})?",
        ["M20"], sum(M["M20"].series().values()), wsum(T.nat_rel, MF, MT),
        "windowed native_release_count vs the SQL day-bucketed count")
    ask(C, f"How many republished releases in {MF[:7]}?",
        ["M21"], sum(M["M21"].series().values()), wsum(T.rep_rel, MF, MT),
        "windowed republished_release_count vs SQL")
    ask(C, f"How many NEW native apps in {MF[:7]}?",
        ["M22"], sum(M["M22"].series().values()), wsum(T.nat_new, MF, MT),
        "windowed new_native_app_count vs SQL")
    ask(C, "Does a server-side window agree with slicing the full series client-side?",
        ["M03", "M20"], sum(M["M20"].series().values()), wsum(m03, MF, MT),
        "server-side window vs client-side slice of the same metric")
    ask(C, "Is new_native_app_count <= native_release_count every day (the yellow-overlay invariant)?",
        ["M01", "M03"], all(m01.get(d, 0) <= m03.get(d, 0) for d in m01), True,
        "per-day comparison; the chart's yellow bar must never exceed the purple one")
    ask(C, "Which day had the most native releases?",
        ["M03"], argmax(m03), argmax(T.nat_rel), "argmax vs SQL")
    ask(C, "Which day had the most republished releases?",
        ["M04"], argmax(m04), argmax(T.rep_rel), "argmax vs SQL")
    ask(C, "What is the trailing-30-day native release total?",
        ["M03"], wsum(m03, ctx["d30"], LAST), wsum(T.nat_rel, ctx["d30"], LAST),
        "the chart's rolling-average input vs SQL")
    ask(C, "What is the trailing-30-day TOTAL release rate (both partitions)?",
        ["M03", "M04"], wsum(m03, ctx["d30"], LAST) + wsum(m04, ctx["d30"], LAST),
        wsum(T.nat_rel, ctx["d30"], LAST) + wsum(T.rep_rel, ctx["d30"], LAST),
        "the chart's teal grand-total line input vs SQL")
    ask(C, "On how many days this year was at least one native release published?",
        ["M03"], len([d for d in m03 if d >= ctx["year_start"]]),
        len([d for d in T.nat_rel if d >= ctx["year_start"]]),
        "count of sparse rows in the current year vs SQL")
    ask(C, "What is the earliest republished release day (upstream backdating)?",
        ["M04"], min(m04), min(T.rep_rel), "MIN(utc_day) vs SQL")
    ask(C, "Native releases per native app (mean, all time)?",
        ["M01", "M03"], round(sum(m03.values()) / sum(m01.values()), 2),
        round(sum(T.nat_rel.values()) / sum(T.nat_new.values()), 2),
        "MCP-derived ratio vs SQL-derived ratio")

    # ---- Chart 4: zaps ----------------------------------------------------
    C = "Chart 4 - Zaps"
    zc, zm = T.zap_count(), T.zap_msats()
    ask(C, "How many Zapstore-attributed zap receipts all time?",
        ["M05"], sum(m05.values()), T.summary["zap_rows"],
        "SUM(zap_count) vs SQL COUNT(*) on events_kind_9735_zaps_zapstore")
    ask(C, "How many msats were zapped all time?",
        ["M06"], sum(m06.values()), T.summary["zap_msats"],
        "SUM(zapped_msats) vs SQL SUM(amount_msats)")
    ask(C, "How many SATS is that (the chart's cumulative-line endpoint)?",
        ["M06"], sum(m06.values()) // 1000, T.summary["zap_msats"] // 1000,
        "msats/1000, converted once at the end as the metric instructs")
    ask(C, "On what day does the zap series open?",
        ["M05"], min(m05), min(zc), "MIN(utc_day) vs SQL; the documented fabricated demo receipt")
    ask(C, "What amount does that opening day carry?",
        ["M06"], m06[min(m06)], zm[min(zm)],
        "first-day amount vs SQL; the flagged 100,000-sat demo receipt")
    ask(C, "Excluding the demo receipt's day, how many real zaps are there?",
        ["M05"], sum(m05.values()) - m05[min(m05)], wsum(zc, ctx["zap_era_from"]),
        "MCP all-time minus the opening day vs SQL over the real-activity era")
    ask(C, "Which day had the most zap receipts?",
        ["M05"], argmax(m05), argmax(zc), "argmax vs SQL")
    ask(C, "Which day carried the most msats?",
        ["M06"], argmax(m06), argmax(zm), "argmax vs SQL")
    ask(C, f"How many zaps in {MF[:7]}?",
        ["M18"], sum(M["M18"].series().values()), wsum(zc, MF, MT),
        "windowed zap_count vs the SQL month count")
    ask(C, f"How many msats in {MF[:7]}?",
        ["M19"], sum(M["M19"].series().values()), wsum(zm, MF, MT),
        "windowed zapped_msats vs the SQL month sum")
    ask(C, "What is the mean zap size in sats (all time)?",
        ["M05", "M06"], round(sum(m06.values()) / sum(m05.values()) / 1000, 1),
        round(T.summary["zap_msats"] / T.summary["zap_rows"] / 1000, 1),
        "msats/count/1000 on both sides")
    ask(C, "On how many distinct days did any Zapstore zap land?",
        ["M05"], len(m05), len(zc), "sparse row count vs SQL distinct days")
    ask(C, "Do zap_count and zapped_msats cover exactly the same days?",
        ["M05", "M06"], sorted(m05) == sorted(m06), True,
        "one population counted two ways, so the sparse day sets must match")
    ask(C, "Does an open-ended 'from' window equal the demo-excluded total?",
        ["M52"], sum(M["M52"].series().values()), wsum(zc, ctx["zap_era_from"]),
        "from-only window (to defaults to today) vs SQL")

    # ---- Leaderboard: apps by zaps ---------------------------------------
    C = "Card - App zaps"
    gt_az = T.app_zaps()
    top_c = max(gt_az.items(), key=lambda kv: kv[1]["zaps"])
    top_m = max(gt_az.items(), key=lambda kv: kv[1]["msats"])
    ask(C, "Which app has received the most zap receipts all time?",
        ["M07"], max(m07, key=lambda r: int(r["zap_count"]))["app_id"], top_c[0][1],
        "top row by zap_count vs SQL GROUP BY app")
    ask(C, "How many zaps does the #1 app have?",
        ["M07"], max(int(r["zap_count"]) for r in m07), top_c[1]["zaps"], "max vs SQL")
    ask(C, "Which app has received the most msats all time?",
        ["M07"], max(m07, key=lambda r: int(r["zapped_msats"]))["app_id"], top_m[0][1],
        "top row by zapped_msats vs SQL")
    ask(C, "Is the most-zapped app by COUNT a different app from the top app by SATS?",
        ["M07"], max(m07, key=lambda r: int(r["zap_count"]))["app_id"]
        != max(m07, key=lambda r: int(r["zapped_msats"]))["app_id"],
        top_c[0] != top_m[0], "compare the two orderings on both sides")
    ask(C, "How many distinct apps have ever been zapped?",
        ["M07"], len(m07), len(gt_az), "row count vs SQL COUNT(DISTINCT app)")
    ask(C, "Do the per-app zap counts sum to the global all-time zap_count?",
        ["M07", "M05"], sum(int(r["zap_count"]) for r in m07), sum(m05.values()),
        "cross-family reconciliation within the zap population")
    ask(C, "Do the per-app msats sum to the global all-time zapped_msats?",
        ["M07", "M06"], sum(int(r["zapped_msats"]) for r in m07), sum(m06.values()),
        "same reconciliation, for amounts")
    ask(C, "What are the top 3 apps by zap count, in order?",
        ["M07"], [r["app_id"] for r in sorted(m07, key=lambda r: -int(r["zap_count"]))[:3]],
        [k[1] for k, _ in sorted(gt_az.items(), key=lambda kv: -kv[1]["zaps"])[:3]],
        "top-3 ordering vs SQL")
    ask(C, "Is the top-zapped app flagged as Nostr-native?",
        ["M07"], max(m07, key=lambda r: int(r["zap_count"]))["is_nostr_native"] == "true",
        top_c[1]["is_native"], "is_nostr_native flag vs the SQL partition predicate")
    ask(C, "How many zapped apps are republished rather than native?",
        ["M07"], sum(1 for r in m07 if r["is_nostr_native"] == "false"),
        sum(1 for v in gt_az.values() if not v["is_native"]), "flag counts vs SQL")
    ask(C, "What share of all-time zapped sats went to the Zapstore curator account (%)?",
        ["M07"],
        round(100 * sum(int(r["zapped_msats"]) for r in m07
                        if r["publisher_pubkey"] == S.CURATOR_PUBKEY)
              / sum(int(r["zapped_msats"]) for r in m07), 1),
        round(100 * sum(v["msats"] for k, v in gt_az.items() if k[0] == S.CURATOR_PUBKEY)
              / T.summary["zap_msats"], 1),
        "MCP curator rows over total vs the same ratio from SQL")
    win_az = T.app_zaps(MF, MT)
    ask(C, f"Which app was the top-zapped in {MF[:7]} alone?",
        ["M17"], max(M["M17"].dicts(), key=lambda r: int(r["zap_count"]))["app_id"],
        max(win_az.items(), key=lambda kv: kv[1]["zaps"])[0][1],
        "windowed leaderboard vs SQL GROUP BY app over the same month")
    ask(C, "How many zaps did that month's leader receive?",
        ["M17"], max(int(r["zap_count"]) for r in M["M17"].dicts()),
        max(v["zaps"] for v in win_az.values()), "windowed max vs SQL")
    ask(C, f"How many apps received any zap in {MF[:7]}?",
        ["M17"], len(M["M17"].rows), len(win_az), "windowed row count vs SQL")
    ask(C, "Do the windowed per-app counts sum to the windowed global zap_count?",
        ["M17", "M18"], sum(int(r["zap_count"]) for r in M["M17"].dicts()),
        sum(M["M18"].series().values()), "windowed leaderboard vs windowed daily series")
    ask(C, "How many (day, app) pairs saw at least one zap?",
        ["M08"], len(M["M08"].rows), len(T.zap_rows),
        "grain=day leaderboard row count vs SQL GROUP BY (app, day)")
    ask(C, "Do the per-(day, app) rows sum to the all-time zap total?",
        ["M08", "M05"], sum(int(r["zap_count"]) for r in M["M08"].dicts()), sum(m05.values()),
        "the finest grain reconciling to the coarsest, within one population")
    ask(C, f"How many new native apps appeared in {S.YEAR_FROM[:4]}?",
        ["M45"], sum(M["M45"].series().values()), wsum(T.nat_new, S.YEAR_FROM, S.YEAR_TO),
        "a calendar-year window on new_native_app_count vs SQL")

    # ---- Leaderboard: publishers by zaps ---------------------------------
    C = "Card - Publisher zaps"
    pub_z = defaultdict(lambda: [0, 0])
    for r in m07:
        if r["is_nostr_native"] == "true":
            pub_z[r["publisher_pubkey"]][0] += int(r["zap_count"])
            pub_z[r["publisher_pubkey"]][1] += int(r["zapped_msats"])
    gt_pz = defaultdict(lambda: [0, 0])
    for (pk, _), v in gt_az.items():
        if v["is_native"]:
            gt_pz[pk][0] += v["zaps"]
            gt_pz[pk][1] += v["msats"]
    ask(C, "Which publisher has received the most zap receipts (native apps only)?",
        ["M07"], max(pub_z.items(), key=lambda kv: kv[1][0])[0],
        max(gt_pz.items(), key=lambda kv: kv[1][0])[0],
        "native rows grouped by publisher_pubkey vs the SQL grouped query")
    ask(C, "How many zaps does that top publisher have?",
        ["M07"], max(v[0] for v in pub_z.values()), max(v[0] for v in gt_pz.values()),
        "grouped max vs SQL")
    ask(C, "Which publisher has received the most msats (native apps only)?",
        ["M07"], max(pub_z.items(), key=lambda kv: kv[1][1])[0],
        max(gt_pz.items(), key=lambda kv: kv[1][1])[0], "grouped by amount vs SQL")
    ask(C, "How many distinct publishers have received at least one zap?",
        ["M07"], len(pub_z), len(gt_pz), "grouped row count vs SQL")
    dev_apps = {k[1]: v for k, v in gt_az.items() if k[0] == S.DEV_PUBKEY}
    ask(C, "How many zaps has the sample developer received across all their apps?",
        ["M11"], sum(int(r["zap_count"]) for r in M["M11"].dicts()),
        sum(v["zaps"] for v in dev_apps.values()),
        "publisher_pubkey entity filter vs the SQL per-publisher sum")
    ask(C, "How many msats has the sample developer received?",
        ["M11"], sum(int(r["zapped_msats"]) for r in M["M11"].dicts()),
        sum(v["msats"] for v in dev_apps.values()), "entity filter vs SQL")
    ask(C, "Does the publisher-filtered call match grouping the unfiltered leaderboard?",
        ["M07", "M11"], sum(int(r["zap_count"]) for r in M["M11"].dicts()),
        sum(int(r["zap_count"]) for r in m07 if r["publisher_pubkey"] == S.DEV_PUBKEY),
        "server-side entity filter vs client-side filter of the full table")
    ask(C, "How many of the sample developer's apps have ever been zapped?",
        ["M11"], len(M["M11"].rows), len(dev_apps), "filtered row count vs SQL")
    cur_apps = {k[1]: v for k, v in gt_az.items() if k[0] == S.CURATOR_PUBKEY}
    ask(C, "How many zaps has the curator account received across all its listings?",
        ["M43"], sum(int(r["zap_count"]) for r in M["M43"].dicts()),
        sum(v["zaps"] for v in cur_apps.values()),
        "curator entity filter vs SQL (its republished listings included)")
    ask(C, "How many curator-listed apps have been zapped?",
        ["M43"], len(M["M43"].rows), len(cur_apps), "curator filter row count vs SQL")

    # ---- Leaderboard: app stacks -----------------------------------------
    C = "Card - App stacks"
    top_s = max(T.stacks.items(), key=lambda kv: kv[1])
    ask(C, "Which app appears in the most public app stacks?",
        ["M09"], max(m09, key=lambda r: int(r["stack_count"]))["app_id"], top_s[0][1],
        "top stack_count vs the SQL unnest of kind-30267 a-tags")
    ask(C, "How many stacks does the #1 app appear in?",
        ["M09"], max(int(r["stack_count"]) for r in m09), top_s[1], "max vs SQL")
    ask(C, "How many apps appear in at least one public stack?",
        ["M09"], len(m09), len(T.stacks), "row count vs SQL")
    ask(C, "What are the top 3 apps by stack appearances?",
        ["M09"], [r["app_id"] for r in sorted(m09, key=lambda r: (-int(r["stack_count"]), r["app_id"]))[:3]],
        [k[1] for k, _ in sorted(T.stacks.items(), key=lambda kv: (-kv[1], kv[0][1]))[:3]],
        "top-3 vs SQL")
    ask(C, "How many stack appearances does the sample app have?",
        ["M09"], int(next((r["stack_count"] for r in m09
                           if r["app_id"] == S.DEV_APP_ID and r["publisher_pubkey"] == S.DEV_PUBKEY), 0)),
        T.stacks.get((S.DEV_PUBKEY, S.DEV_APP_ID), 0),
        "MCP row vs SQL COUNT(DISTINCT stack) for that a-tag")
    ask(C, "How many stack-referenced apps are curator republishes?",
        ["M09"], sum(1 for r in m09 if r["publisher_pubkey"] == S.CURATOR_PUBKEY),
        sum(1 for k in T.stacks if k[0] == S.CURATOR_PUBKEY), "MCP curator rows vs SQL")
    ask(C, "Does the publisher-filtered stack call match SQL for the sample developer?",
        ["M14"], {r["app_id"]: int(r["stack_count"]) for r in M["M14"].dicts()},
        {k[1]: v for k, v in T.stacks.items() if k[0] == S.DEV_PUBKEY},
        "entity-filtered stack counts vs SQL per-app stack counts")
    ask(C, "Does a curator-filtered stack call match filtering the full table client-side?",
        ["M09", "M44"], len(M["M44"].rows),
        sum(1 for r in m09 if r["publisher_pubkey"] == S.CURATOR_PUBKEY),
        "server-side vs client-side entity filter")
    ask(C, "Is the stack leaderboard a snapshot with no time axis?",
        ["M09"], "utc_day" not in M["M09"].hdr and "current snapshot" in M["M09"].body, True,
        "the column contract carries no day column and the envelope declares a snapshot")

    # ---- Leaderboard: publisher app counts --------------------------------
    C = "Card - Publisher apps"
    top_p = max(T.pub_apps.items(), key=lambda kv: kv[1])
    ask(C, "Which publisher has the most Nostr-native apps?",
        ["M10"], max(m10, key=lambda r: int(r["app_count"]))["publisher_pubkey"], top_p[0],
        "top app_count vs SQL COUNT(DISTINCT d_tag) on kind-32267")
    ask(C, "How many apps does the top publisher have?",
        ["M10"], max(int(r["app_count"]) for r in m10), top_p[1], "max vs SQL")
    ask(C, "How many publishers have at least one native app?",
        ["M10"], len(m10), len(T.pub_apps), "row count vs SQL")
    ask(C, "Do the per-publisher app counts sum to the native catalog size?",
        ["M10"], sum(int(r["app_count"]) for r in m10), T.summary["catalog_native"],
        "MCP sum vs SQL COUNT over the native partition of kind-32267")
    ask(C, "How many native apps does the sample developer publish?",
        ["M15"], int(M["M15"].dicts()[0]["app_count"]), T.pub_apps[S.DEV_PUBKEY],
        "entity filter vs SQL")
    ask(C, "How many native apps does the no-profile publisher publish (entity-filtered)?",
        ["M49"], int(M["M49"].dicts()[0]["app_count"]), T.pub_apps[S.NO_PROFILE_PUBKEY],
        "entity filter vs SQL")
    ask(C, "Is the no-profile publisher's name NULL in the leaderboard?",
        ["M10"], next(r["publisher_name"] for r in m10
                      if r["publisher_pubkey"] == S.NO_PROFILE_PUBKEY) in ("\\N", ""),
        not bool(T.profile_of(S.NO_PROFILE_PUBKEY)["has_k0"]),
        "MCP name column vs SQL: no kind-0 row exists for that pubkey")
    ask(C, "Is the top publisher by app count also the top publisher by zaps?",
        ["M07", "M10"],
        max(m10, key=lambda r: int(r["app_count"]))["publisher_pubkey"]
        == max(pub_z.items(), key=lambda kv: kv[1][0])[0],
        top_p[0] == max(gt_pz.items(), key=lambda kv: kv[1][0])[0],
        "cross-card comparison, computed on both sides")

    # ---- App detail page --------------------------------------------------
    C = "App page"
    dev_rel = T.releases_of(S.DEV_PUBKEY, S.DEV_APP_ID)
    m30 = M["M30"].dicts()
    ask(C, "How many releases does the sample app have?",
        ["M30"], len(M["M30"].rows), len(dev_rel), "app_releases row count vs SQL COUNT(*)")
    ask(C, "What is the sample app's first release date?",
        ["M30"], min(r["released_at"][:10] for r in m30), min(r["d"] for r in dev_rel),
        "MIN(released_at) vs SQL MIN(created_at) day")
    ask(C, "What is the sample app's most recent release date?",
        ["M30"], max(r["released_at"][:10] for r in m30), max(r["d"] for r in dev_rel),
        "MAX(released_at) vs SQL")
    ask(C, "What is the sample app's latest version string?",
        ["M30"], m30[0]["version"],
        max(dev_rel, key=lambda r: r["created_at"])["version"],
        "first row of the newest-first listing vs SQL ORDER BY created_at DESC")
    ask(C, "Is the release listing actually returned newest-first as documented?",
        ["M30"], [r["released_at"] for r in m30] == sorted([r["released_at"] for r in m30], reverse=True),
        True, "ordering check on the returned rows")
    ask(C, "What release channels does the sample app ship on?",
        ["M30"], sorted(set(r["channel"] for r in m30)),
        sorted(set(r["channel"] for r in dev_rel)),
        "distinct channel values vs SQL (a NULL channel tag renders as empty)")
    ask(C, "Do release notes ship at single-app scope but not at publisher scope?",
        ["M29", "M30"], ("release_notes" in M["M30"].hdr, "release_notes" in M["M29"].hdr),
        (True, False), "column contract at the two scopes - the documented long-text rule")
    ask(C, "How many of the sample app's releases fell in the sample window?",
        ["M31"], len(M["M31"].rows),
        len(T.releases_of(S.DEV_PUBKEY, S.DEV_APP_ID, S.APP_WINDOW_FROM, S.APP_WINDOW_TO)),
        "windowed app_releases vs the SQL count over the same days")
    cat_row = T.catalog_of(S.DEV_PUBKEY, S.DEV_APP_ID)[0]
    ask(C, "What is the sample app's catalog display name?",
        ["M24"], M["M24"].dicts()[0]["app_name"], cat_row["name"],
        "app_catalog vs the SQL name column")
    ask(C, "What is the sample app's repository URL?",
        ["M24"], M["M24"].dicts()[0]["repository_url"] or None, cat_row["repository"],
        "app_catalog vs the SQL repository column")
    ask(C, "Does the full description ship only at single-app catalog scope?",
        ["M23", "M24"], ("description" in M["M24"].hdr, "description" in M["M23"].hdr),
        (True, False), "column contract at the two scopes")
    dev_app_z = gt_az.get((S.DEV_PUBKEY, S.DEV_APP_ID), {"zaps": 0, "msats": 0})
    ask(C, "How many zaps has the sample app received (app page header)?",
        ["M12"], int(M["M12"].dicts()[0]["zap_count"]), dev_app_z["zaps"],
        "publisher+app entity filter vs SQL")
    ask(C, "How many sats has the sample app received?",
        ["M12"], int(M["M12"].dicts()[0]["zapped_msats"]) // 1000, dev_app_z["msats"] // 1000,
        "entity filter, msats/1000, vs SQL")
    ask(C, "On how many distinct days did the sample app receive a zap?",
        ["M16"], len(M["M16"].rows),
        len({r["d"] for r in T.zap_rows
             if r["pubkey"] == S.DEV_PUBKEY and r["d_tag"] == S.DEV_APP_ID}),
        "grain=day entity-filtered row count vs SQL COUNT(DISTINCT day)")
    ask(C, "Do the sample app's daily zap rows sum to its total-grain zap count?",
        ["M12", "M16"], sum(int(r["zap_count"]) for r in M["M16"].dicts()),
        int(M["M12"].dicts()[0]["zap_count"]), "grain=day vs grain=total for one entity")
    ask(C, "Do its daily msats sum to its total-grain msats?",
        ["M12", "M16"], sum(int(r["zapped_msats"]) for r in M["M16"].dicts()),
        int(M["M12"].dicts()[0]["zapped_msats"]), "grain=day vs grain=total")
    ask(C, "Is the curator's own client flagged Nostr-native (the own-d-tag exception)?",
        ["M13"], M["M13"].dicts()[0]["is_nostr_native"] == "true",
        S.is_native(S.CURATOR_PUBKEY, S.FIRST_PARTY_APP_ID),
        "curator key but first-party, so native; the SQL predicate agrees")
    ask(C, "How many zaps has the Zapstore Android client itself received?",
        ["M13"], int(M["M13"].dicts()[0]["zap_count"]),
        gt_az.get((S.CURATOR_PUBKEY, S.FIRST_PARTY_APP_ID), {"zaps": 0})["zaps"],
        "entity filter vs SQL")
    rep_rel_rows = T.releases_of(S.CURATOR_PUBKEY, S.REPUBLISHED_APP_ID)
    ask(C, "How many releases does the sample republished listing have?",
        ["M42"], len(M["M42"].rows), len(rep_rel_rows),
        "app_releases for a curator republish vs SQL")
    rep_cat = T.catalog_of(S.CURATOR_PUBKEY, S.REPUBLISHED_APP_ID)[0]
    ask(C, "What is the republished listing's catalog name?",
        ["M41"], M["M41"].dicts()[0]["app_name"], rep_cat["name"], "app_catalog vs SQL")
    ask(C, "Is the republished listing correctly flagged NOT Nostr-native?",
        ["M41"], M["M41"].dicts()[0]["is_nostr_native"] == "false",
        not S.is_native(S.CURATOR_PUBKEY, S.REPUBLISHED_APP_ID),
        "flag vs the SQL partition predicate")

    # ---- Publisher detail page --------------------------------------------
    C = "Publisher page"
    ask(C, "How many apps does the sample developer have in the catalog?",
        ["M23"], len(M["M23"].rows), len(T.catalog_of(S.DEV_PUBKEY)),
        "app_catalog row count vs SQL")
    ask(C, "How many release events has the sample developer published across all apps?",
        ["M29"], len(M["M29"].rows), len(T.releases_of(S.DEV_PUBKEY)),
        "app_releases publisher scope vs SQL COUNT(*)")
    ask(C, "How many distinct apps do those releases cover?",
        ["M29"], len(set(r["app_id"] for r in M["M29"].dicts())),
        len({r["d_tag"] for r in T.releases_of(S.DEV_PUBKEY)}), "distinct app_id vs SQL")
    prof = T.profile_of(S.DEV_PUBKEY)
    ask(C, "What is the sample developer's profile name?",
        ["M27"], M["M27"].dicts()[0]["publisher_name"],
        prof["display_name"] or prof["name"], "publisher_profile vs the SQL kind-0 row")
    ask(C, "Is the sample developer flagged as the Zapstore account?",
        ["M27"], M["M27"].dicts()[0]["is_zapstore_account"] == "true",
        S.DEV_PUBKEY == S.CURATOR_PUBKEY, "flag vs the pubkey comparison")
    cur_prof = T.profile_of(S.CURATOR_PUBKEY)
    ask(C, "What is the Zapstore curator's NIP-05?",
        ["M28"], M["M28"].dicts()[0]["nip05"], cur_prof["nip05"],
        "publisher_profile vs the SQL kind-0 nip05")
    ask(C, "Is the curator flagged as the Zapstore account?",
        ["M28"], M["M28"].dicts()[0]["is_zapstore_account"] == "true", True, "flag vs SQL")
    ask(C, "How many publishers are in the full directory?",
        ["M26"], len(M["M26"].rows), T.summary["catalog_pubkeys"],
        "directory row count vs SQL COUNT(DISTINCT catalog pubkey)")
    ask(C, "How many catalog publishers resolve to a printable profile name?",
        ["M26"], sum(1 for r in M["M26"].dicts() if r["publisher_name"] not in ("\\N", "")),
        T.summary["catalog_pubkeys_with_name"],
        "non-null names in the directory vs the SQL join count")
    ask(C, "How many catalog publishers have no printable name?",
        ["M26"], len(M["M26"].rows) - sum(1 for r in M["M26"].dicts()
                                          if r["publisher_name"] not in ("\\N", "")),
        T.summary["catalog_pubkeys"] - T.summary["catalog_pubkeys_with_name"],
        "NULL-name rows vs SQL")
    blank = [r for r in T.profiles
             if r["has_k0"] and not r["name"] and not r["display_name"]]
    if blank:
        ask(C, "Does the directory distinguish 'no kind-0 event' from 'kind-0 with a blank name'?",
            ["M26"], next((r for r in M["M26"].dicts()
                           if r["publisher_pubkey"] == blank[0]["pubkey"]), {}).get("nip05"),
            blank[0]["nip05"],
            "SQL finds a publisher whose kind-0 has empty name AND display_name; its other "
            "profile columns must still be served, proving a NULL name is not a missing profile")
    ask(C, "Does the no-profile publisher appear in the directory with a NULL name?",
        ["M26"], next((r["publisher_name"] for r in M["M26"].dicts()
                       if r["publisher_pubkey"] == S.NO_PROFILE_PUBKEY), "MISSING"),
        "\\N", "directory row vs SQL (no kind-0 row exists for that pubkey)")
    ask(C, "How many apps does the no-profile publisher list in the catalog?",
        ["M50"], len(M["M50"].rows), len(T.catalog_of(S.NO_PROFILE_PUBKEY)),
        "app_catalog publisher scope vs SQL")
    ask(C, "Are all of that publisher's listings flagged Nostr-native?",
        ["M50"], all(r["is_nostr_native"] == "true" for r in M["M50"].dicts()),
        all(r["is_native"] for r in T.catalog_of(S.NO_PROFILE_PUBKEY)),
        "flag column vs the SQL partition")

    # ---- Contract / semantics / errors ------------------------------------
    C = "Contract"
    ask(C, "Does the curator catalog query hit the 1,000-row cap and say so?",
        ["M25"], (len(M["M25"].rows), bool(M["M25"].truncated)),
        (min(1000, T.summary["curator_catalog_rows"]), T.summary["curator_catalog_rows"] > 1000),
        "SQL gives the curator's true catalog size; the cap must bind and be announced")
    ask(C, "Does the truncated line name a remedy rather than failing silently?",
        ["M25"], bool(M["M25"].truncated and len(M["M25"].truncated[0]) > 30), True,
        "inspect the truncated: line text")
    ask(C, "Is app_id rejected without publisher_pubkey?",
        ["M33"], M["M33"].error and "requires publisher_pubkey" in M["M33"].body, True,
        "app ids are unique only per publisher - SQL confirms the sample app id exists "
        "under more than one pubkey")
    ask(C, "Does an unknown publisher_pubkey return an empty table rather than an error (reference)?",
        ["M32"], (not M["M32"].error, len(M["M32"].rows)), (True, 0),
        "all-zeros pubkey vs the documented empty-not-error rule")
    ask(C, "Does an unknown publisher_pubkey return an empty table on a metric too?",
        ["M51"], (not M["M51"].error, len(M["M51"].rows)), (True, 0), "same rule on the metric tool")
    ask(C, "Is an unknown metric name rejected with a useful error?",
        ["M34"], M["M34"].error and "unknown metric" in M["M34"].body, True,
        "a metric name that does not exist")
    ask(C, "Are from/to rejected on the app_stack_count snapshot metric?",
        ["M35"], M["M35"].error, True, "snapshot metrics have no time axis")
    ask(C, "Is grain=day rejected on the publisher_app_count snapshot metric?",
        ["M36"], M["M36"].error, True, "snapshot metrics serve grain=current only")
    ask(C, "Is an inverted window (from after to) rejected?",
        ["M37"], M["M37"].error and "after" in M["M37"].body, True, "from and to swapped")
    ask(C, "Is grain=total rejected on a daily-only metric, saying its day rows add up?",
        ["M48"], M["M48"].error and "its day rows add up - sum them over from/to" in M["M48"].body, True,
        "native_release_count serves day grain only; the rejection names the remedy (0.5.1)")
    ask(C, "Is grain=current rejected on the app_zaps event metric?",
        ["M39"], M["M39"].error, True, "app_zaps serves day/total, not current")
    ask(C, "Does an all-future window return an empty series rather than an error?",
        ["M38"], (not M["M38"].error, len(M["M38"].rows)), (True, 0), "a window far in the future")
    ask(C, "Does format=markdown render a markdown table instead of CSV?",
        ["M40"], M["M40"].body.count("|") > 10 and "---" in M["M40"].body, True,
        "format switch on the same window")
    import re as _re
    md_days = sorted(M["M18"].series())[:4]
    ask(C, "Do markdown and CSV carry the same values for the same window?",
        ["M18", "M40"], [M["M18"].series()[d] for d in md_days],
        [int(_re.search(r"\|\s*" + d + r"\s*\|\s*(\d+)", M["M40"].body).group(1)) for d in md_days],
        "the same days parsed out of both renderings")
    ask(C, "Does every response declare a data-derived freshness (as_of) line?",
        ["M01", "M05", "M07", "M09"],
        all("as_of" in M[k].body for k in ["M01", "M05", "M07", "M09"]), True,
        "envelope contract check across the metric families")
    ask(C, "Does the zap freshness match the newest zap event actually in the DB?",
        ["M05"], M["M05"].as_of_day(), T.summary["max_zap_at"][:10],
        "MCP as_of vs SQL MAX(created_at) on events_kind_9735_zaps_zapstore")
    ask(C, "Does the release freshness match the newest release event in the DB?",
        ["M03"], M["M03"].as_of_day(), T.summary["max_release_at"][:10],
        "MCP as_of vs SQL MAX(created_at) on events_kind_30063_app_releases")
    ask(C, "Does an open-ended 'to' default to today and flag the final day as in progress?",
        ["M52"], "in progress" in M["M52"].body, True,
        "the incomplete-day wrinkle must be declared, not silently served")
    ask(C, "Does the declared row count match the rows actually returned?",
        ["M07", "M09", "M10"],
        [M[k].declared_rows() == len(M[k].rows) for k in ["M07", "M09", "M10"]],
        [True, True, True], "envelope 'rows:' vs the parsed CSV length")
    ask(C, "Does the zap methodology disclose the fabricated demo receipt?",
        ["M05"], "demo-zap-request" in M["M05"].body, True,
        "methodology honesty check against the SQL-confirmed event")
    ask(C, "Does new_native_app_count explain why it uses kind-30063 not the kind-32267 catalog?",
        ["M01"], "32267 is replaceable" in M["M01"].body, True, "methodology completeness check")

    # ---- Live-page reproduction -------------------------------------------
    WS, WE = W["window_start"], W["window_end"]

    C = "Live page - Chart 1"
    ask(C, "Chart 1 'Total' endpoint: does MCP redraw the catalog-size line end?",
        ["M01", "M02"], wsum(m01, to=WE) + wsum(m02, to=WE), int(W["cum_total"][-1][1]),
        "live /zapstore chart series endpoint")
    ask(C, "Chart 1 'Nostr-native' endpoint?",
        ["M01"], wsum(m01, to=WE), int(W["cum_native"][-1][1]), "live chart series endpoint")
    ask(C, "Chart 1 'Republished' endpoint?",
        ["M02"], wsum(m02, to=WE), int(W["cum_github"][-1][1]), "live chart series endpoint")
    ask(C, "Chart 1 'Total' at the window start (365 days back)?",
        ["M01", "M02"], wsum(m01, to=WS) + wsum(m02, to=WS), int(W["cum_total"][0][1]),
        "live chart first point")
    ask(C, "Chart 1 'Nostr-native' at the window start?",
        ["M01"], wsum(m01, to=WS), int(W["cum_native"][0][1]), "live chart first point")
    ask(C, "Chart 1 'Republished' at the window start?",
        ["M02"], wsum(m02, to=WS), int(W["cum_github"][0][1]), "live chart first point")

    C = "Live page - Chart 2"
    ask(C, "Chart 2 native-share endpoint (%)?",
        ["M01", "M02"], share(m01, m02, WE), round(W["native_share"][-1][1], 2),
        "live nativeShare series endpoint")
    ask(C, "Chart 2 native-share at the window start (%)?",
        ["M01", "M02"], share(m01, m02, WS), round(W["native_share"][0][1], 2),
        "live nativeShare first point")

    C = "Live page - Chart 4"
    ask(C, "Chart 4 bars: zap receipts over the charted year?",
        ["M05"], wsum(m05, WS, WE), int(sum(v for _, v in W["zaps_daily"])),
        "live aggZaps.daily summed")
    ask(C, "Chart 4 cumulative-sats line endpoint?",
        ["M06"], wsum(m06, WS, WE) // 1000, int(W["zaps_cumulative"][-1][1]),
        "live aggZaps.cumulative endpoint")

    # Chart 3 is where the calendar-vs-timestamp boundary actually bites.
    C = "Live page - Chart 3"
    day_after = ctx["day_after"](WS)
    for label, mcp_series, web_key in (
            ("purple bars: native releases", m03, "rel_native"),
            ("yellow overlay: new native apps", m01, "rel_new_native"),
            ("blue bars: republished releases", m04, "rel_github")):
        web_total = int(sum(v for _, v in W[web_key]))
        excl = wsum(mcp_series, day_after, WE)
        incl = wsum(mcp_series, WS, WE)
        how = ("live releasesDaily series summed; the site cuts on a mid-day timestamp while MCP "
               "windows on whole UTC days, so the boundary day can only be taken or dropped whole")
        if excl == web_total or incl == web_total:
            ask(C, f"Chart 3 {label} over the charted year?",
                ["M03" if mcp_series is m03 else "M01" if mcp_series is m01 else "M04"],
                excl if excl == web_total else incl, web_total,
                how + " - here the boundary day falls entirely on one side, so it matches exactly")
        else:
            ask_bounded(C, f"Chart 3 {label} over the charted year?",
                        ["M04" if mcp_series is m04 else "M03"], excl, incl, web_total, how)

    C = "Live card - Stacks"
    ask(C, "Does MCP redraw the 'App Stack Appearances' card top 4?",
        ["M09"], [(r["app_name"], int(r["stack_count"]))
                  for r in sorted(m09, key=lambda r: (-int(r["stack_count"]), r["app_id"]))[:4]],
        [tuple(x) for x in W["card_stacks"]], "live card rows 1-4, scraped from the rendered page")

    C = "Live card - Publisher apps"
    ask(C, "Does MCP redraw the 'Total Apps' publisher card top 4?",
        ["M10"], [int(r["app_count"]) for r in sorted(m10, key=lambda r: -int(r["app_count"]))[:4]],
        list(W["card_pub_apps"]), "live card rows 1-4, scraped from the rendered page")
    ask(C, "Does the card's #1 row correspond to a publisher with no profile name?",
        ["M10"], max(m10, key=lambda r: int(r["app_count"]))["publisher_name"] in ("\\N", ""),
        True, "the live card renders a truncated pubkey rather than a name for that row")

    # The two rolling-30-day zap cards. The site cuts on a mid-day timestamp, so
    # the same boundary-day effect as chart 3 applies: the calendar window that
    # INCLUDES the boundary day is an upper bound, the one that EXCLUDES it a
    # lower bound, and the card must sit between them. When no zaps land in the
    # straddled hours the two coincide and the check is exact - do not mistake
    # that for the general case.
    def card_rows(resp, group_by_publisher):
        rows = resp.dicts()
        if not group_by_publisher:
            return {(r["app_name"]): (int(r["zap_count"]), int(r["zapped_msats"]) // 1000)
                    for r in rows}
        acc = defaultdict(lambda: [0, 0])
        for r in rows:
            if r["is_nostr_native"] != "true":
                continue
            key = (r["publisher_name"] if r["publisher_name"] not in ("\\N", "")
                   else r["publisher_pubkey"])
            acc[key][0] += int(r["zap_count"])
            acc[key][1] += int(r["zapped_msats"])
        return {k: (v[0], v[1] // 1000) for k, v in acc.items()}

    def rolling_card(cat, question, web_key, group):
        incl = card_rows(M["W30"], group)
        excl = card_rows(M["W30E"], group)
        live = [tuple(x) for x in W[web_key]]
        got_incl = [(name, ) + incl.get(name, (0, 0)) for name, _, _ in live]
        got_excl = [(name, ) + excl.get(name, (0, 0)) for name, _, _ in live]
        how = ("app_zaps grain=total over the calendar window equivalent to the card's rolling "
               "30 days, vs the live card rows 1-4; the site's cutoff is a mid-day timestamp, so "
               "the live value must fall between the boundary-day-excluded and -included windows")
        if got_incl == live:
            ask(cat, question, ["W30"], got_incl, live, how + " - here the two coincide exactly")
        else:
            bracketed = all(lo <= v <= hi for (_, *los), (_, *vs), (_, *his)
                            in zip(got_excl, live, got_incl)
                            for lo, v, hi in zip(los, vs, his))
            Q.append(dict(n=len(Q) + 1, cat=cat, q=question, calls=["W30", "W30E"],
                          got=f"{got_excl} (day excluded) .. {got_incl} (day included)",
                          want=live, how=how, bounded=True, ok=bracketed))

    rolling_card("Live card - App zaps",
                 "Does MCP reproduce the 'Zaps' card on its default 30-day period?",
                 "card_app_zaps", group=False)
    rolling_card("Live card - Publisher zaps",
                 "Does MCP reproduce the 'Total Zaps' publisher card on its 30-day period?",
                 "card_pub_zaps", group=True)

    C = "Live page - App page"
    ask(C, "App page header: the sample app's first-release date and total release count?",
        ["M30"], (min(r["released_at"][:10] for r in m30), len(M["M30"].rows)),
        (ctx["us_date"](W["app_first_release"]), W["app_releases_total"]),
        "live app page header line")
    ask(C, "App page header: most recent release date?",
        ["M30"], max(r["released_at"][:10] for r in m30),
        ctx["us_date"](W["app_last_release"]), "live app page header line")
    ask(C, "App page: the sample app's sats figure?",
        ["M12"], int(M["M12"].dicts()[0]["zapped_msats"]) // 1000, W["app_sats"],
        "live app page sats figure")

    C = "Live page - Publisher page"
    ask(C, "Publisher page header: app count?",
        ["M23"], len(M["M23"].rows), W["pub_app_count"], "live publisher page header")
    ask(C, "Publisher page header: zaps in the past year?",
        ["M11"], sum(int(r["zap_count"]) for r in M["M11"].dicts()), W["pub_zaps_1y"],
        "live publisher page header")
    ask(C, "Publisher page header: sats in the past year?",
        ["M11"], sum(int(r["zapped_msats"]) for r in M["M11"].dicts()) // 1000, W["pub_sats_1y"],
        "live publisher page header")

    routstr_questions(M, sql, W, ctx, ask)
    return Q


# ---------------------------------------------------------------------------
# Routstr (2026-08-29) - the second domain
# ---------------------------------------------------------------------------
def routstr_questions(M, sql, W, ctx, ask):
    """Three independent sides, as for Zapstore: the DOCUMENT the page serves
    (read raw via psql), an INDEPENDENT SQL re-sum over the substrate ledger
    (truth_sql's own 30d/all-time statement), and the live page's figures."""
    doc = sql["ROUTSTR_DOC"][0]["data"]
    s = doc["summary"]
    prov_sql = {r["pubkey"]: r for r in sql["ROUTSTR_PROVIDER_SUMS"]}
    model_sql = {r["model"]: r for r in sql["ROUTSTR_MODEL_SUMS"]}

    C = "Routstr - headline"
    r01 = M["R01"].dicts()[0]
    for col in ("providers_announced", "providers_reporting", "providers_active_30d",
                "revenue_30d_sats", "requests_30d", "tokens_30d", "revenue_alltime_sats",
                "requests_alltime", "tokens_alltime", "models_30d", "mints"):
        ask(C, f"network_summary.{col} equals the page document's summary?",
            ["R01"], int(r01[col]), s[col], "one-row snapshot vs the routstr_snapshot document")
    ask(C, "network_summary's refresh instant is the document's generated_utc?",
        ["R01"], r01["refresh_generated_at"], doc["generated_utc"].replace(" ", "T"),
        "timestamp column vs the document header")
    ask(C, "A bare network_summary call (no domain) resolves to routstr?",
        ["R19"], (M["R19"].env.get("metric") or "").split("  |  ")[0], "routstr.network_summary",
        "the envelope's identity line after domain resolution by metric name")

    C = "Routstr - network series"
    r02 = M["R02"].dicts()
    ask(C, "network_activity IS the document's daily series (every day, all four measures)?",
        ["R02"], [(r["utc_day"], int(r["msats"]), int(r["requests"]), int(r["tokens"]),
                   int(r["active_providers"])) for r in r02],
        [(d["d"], d["msats"], d["req"], d["tok"], d["prov"]) for d in doc["daily"]],
        "row-for-row against the document")
    in30 = [r for r in r02 if ctx["routstr_30d_from"] <= r["utc_day"] <= ctx["routstr_30d_to"]]
    ask(C, "Summing the last 30 complete days of network_activity reproduces the 30d headline?",
        ["R02", "R01"],
        (round(sum(int(r["msats"]) for r in in30) / 1000), sum(int(r["requests"]) for r in in30)),
        (s["revenue_30d_sats"], s["requests_30d"]),
        "SUM over today-30..today-1 of the daily series, msats rounded once to sats")
    r03 = M["R03"].dicts()
    ask(C, "cumulative_revenue ends at the all-time revenue headline?",
        ["R03", "R01"], round(int(r03[-1]["cumulative_msats"]) / 1000), s["revenue_alltime_sats"],
        "last row of the running total, msats -> sats")
    ask(C, "cumulative_revenue is monotone?",
        ["R03"], all(int(a["cumulative_msats"]) <= int(b["cumulative_msats"])
                     for a, b in zip(r03, r03[1:])), True, "pairwise check over the rows")

    C = "Routstr - providers"
    r04 = {r["provider_pubkey"]: r for r in M["R04"].dicts()}
    incl = {pk: r for pk, r in r04.items() if r["excluded"] == "false"}
    ask(C, "provider_leaderboard lists every provider the document counts (announced + reporting)?",
        ["R04"], len(r04), doc["providers_total"], "row count vs the document's providers_total")
    ask(C, "provider_leaderboard 30d and all-time figures equal an independent SQL re-sum of the ledger?",
        ["R04"],
        sorted((pk, int(r["msats_30d"]), int(r["tokens_30d"]), int(r["msats_alltime"]))
               for pk, r in incl.items() if pk in prov_sql),
        sorted((pk, int(x["msats_30d"]), int(x["tokens_30d"]), int(x["msats_alltime"]))
               for pk, x in prov_sql.items() if not x["excluded"]),
        "truth_sql's own SUM over routstr_provider_days at the document's 30-complete-day window")
    r05 = {r["provider_pubkey"]: r for r in M["R05"].dicts()}
    r06 = {r["provider_pubkey"]: r for r in M["R06"].dicts()}
    ask(C, "provider_activity grain=total with NO window equals the leaderboard's all-time columns?",
        ["R05", "R04"],
        sorted((pk, int(r["msats"]), int(r["requests"]), int(r["tokens"])) for pk, r in r05.items()),
        sorted((pk, int(r["msats_alltime"]), int(r["requests_alltime"]), int(r["tokens_alltime"]))
               for pk, r in incl.items()
               if int(r["msats_alltime"]) or int(r["requests_alltime"]) or int(r["tokens_alltime"])),
        "the SQL SUM (all grains) vs the document-derived columns, included providers only - a "
        "provider that never reported has no ledger row, so no substrate row either")
    ask(C, "provider_activity grain=total over refresh-30..refresh-1 equals the leaderboard's 30d columns?",
        ["R06", "R04"],
        sorted((pk, int(r["msats"]), int(r["requests"]), int(r["tokens"])) for pk, r in r06.items()),
        sorted((pk, int(r["msats_30d"]), int(r["requests_30d"]), int(r["tokens_30d"]))
               for pk, r in incl.items() if int(r["msats_30d"]) or int(r["requests_30d"]) or int(r["tokens_30d"])),
        "THE drift detector: the windowed SUM vs the page's fixed 30d figures")
    ask(C, "Unfiltered provider_activity never returns an excluded provider?",
        ["R05"], all(r["excluded"] == "false" for r in r05.values()), True, "excluded column scan")
    top = ctx["routstr_top_provider"]
    r18 = M["R18"].dicts()
    ask(C, "The top provider's day rows re-sum to its all-time total?",
        ["R18", "R05"], sum(int(r["msats"]) for r in r18), int(r05[top]["msats"]),
        "SUM of the filtered grain=day rows vs the grain=total row")
    ask(C, "The provider_pubkey filter is echoed in the envelope scope line?",
        ["R18"], M["R18"].env.get("scope"), f"provider_pubkey={top}", "envelope scope line")

    C = "Routstr - models"
    r07 = {r["model"]: r for r in M["R07"].dicts()}
    ask(C, "model_leaderboard is the document's model rows in order?",
        ["R07"], [(m, int(r["msats_30d"]), int(r["msats_alltime"]), int(r["providers_30d"]))
                  for m, r in r07.items()],
        [(m["model"], m["msats_30d"], m["msats_alltime"], m["providers_30d"]) for m in doc["models"]],
        "row-for-row against the document's models[]")
    named = [m for m in r07 if m != "(other)"]
    ask(C, "Named model rows equal an independent SQL re-sum of routstr_model_days?",
        ["R07"], sorted((m, int(r07[m]["msats_30d"]), int(r07[m]["msats_alltime"])) for m in named),
        sorted((m, int(model_sql[m]["msats_30d"]), int(model_sql[m]["msats_alltime"])) for m in named),
        "truth_sql's own SUM over routstr_model_days at the document's window")
    r08 = {r["model"]: r for r in M["R08"].dicts()}
    r09 = {r["model"]: r for r in M["R09"].dicts()}
    ask(C, "model_activity grain=total (no window) equals the leaderboard's all-time msats for named models?",
        ["R08", "R07"], sorted((m, int(r08[m]["msats"])) for m in named),
        sorted((m, int(r07[m]["msats_alltime"])) for m in named), "SQL SUM vs document columns")
    ask(C, "model_activity grain=total over the 30d window equals the leaderboard's 30d msats?",
        ["R09", "R07"], sorted((m, int(r09.get(m, {"msats": 0})["msats"])) for m in named),
        sorted((m, int(r07[m]["msats_30d"])) for m in named), "windowed SQL SUM vs document columns")
    ask(C, "The model filter accepts '(other)' and returns exactly that row?",
        ["R22"], [r["model"] for r in M["R22"].dicts()], ["(other)"] if "(other)" in r07 else [],
        "filtered call vs the leaderboard's residual row")

    C = "Routstr - catalog, reliability, network shape"
    ask(C, "catalog_size is the document's catalog history?",
        ["R10"], [(r["utc_day"], int(r["catalog_models"])) for r in M["R10"].dicts()],
        [(h["d"], h["n"]) for h in doc["catalog"]["history"]], "row-for-row")
    r11 = M["R11"].dicts()[0]
    ask(C, "success_rate carries the document's reliability block?",
        ["R11"], (r11["success_rate_30d"], int(r11["requests_reported"]), int(r11["providers_fresh"])),
        (str(doc["success"]["rate_30d"]) if doc["success"]["rate_30d"] is not None else "\\N",
         doc["success"]["requests_reported"], doc["success"]["providers_fresh"]),
        "one-row snapshot vs the document")
    ask(C, "provider_facets are the three Network-tab breakdowns in order?",
        ["R12"], [(r["facet"], r["value"], int(r["providers"])) for r in M["R12"].dicts()],
        [("transport", e["k"], e["v"]) for e in doc["transports"]]
        + [("version", e["k"], e["v"]) for e in doc["versions"]]
        + [("mint_host", e["k"], e["v"]) for e in doc["mints_top"]], "row-for-row")
    ask(C, "The transport facet sums to providers_announced?",
        ["R12", "R01"], sum(int(r["providers"]) for r in M["R12"].dicts() if r["facet"] == "transport"),
        int(r01["providers_announced"]), "SUM of the facet vs the headline")

    C = "Routstr - reference records"
    r13 = M["R13"].dicts()
    ask(C, "provider_profile has one row per provider, excluded ones flagged?",
        ["R13", "R04"], (len(r13), sum(r["excluded"] == "true" for r in r13)),
        (len(r04), sum(r["excluded"] == "true" for r in r04.values())), "directory vs leaderboard")
    ask(C, "exclusions is the document's ledger?",
        ["R17"], [(r["category"], int(r["providers"])) for r in M["R17"].dicts()],
        [(e["k"], e["v"]) for e in doc["exclusions"]], "row-for-row")
    ask(C, "model_catalog is the document's current curated list?",
        ["R16"], [r["model"] for r in M["R16"].dicts()], doc["catalog"]["models"], "row-for-row")
    r15 = M["R15"].dicts()
    ask(C, "catalog_versions has one row per archived version, newest first, ending at the current size?",
        ["R15"], (len(r15), int(r15[0]["model_count"]) if r15 else None),
        (len(doc["catalog"]["history"]), doc["catalog"]["history"][-1]["n"] if doc["catalog"]["history"] else None),
        "count + newest row vs the document's history")
    ask(C, "provider_reviews' newest rows are the document's timeline head?",
        ["R14"], [(r["label"], r["reviewed_at"][:10]) for r in M["R14"].dicts()[:len(doc["reviews"])]],
        [(r["label"], r["d"]) for r in doc["reviews"]], "prefix comparison, newest first")

    C = "Routstr - contract"
    ask(C, "A Routstr filter on a Zapstore metric is rejected (not silently ignored)?",
        ["R20"], M["R20"].error and "Routstr coordinate" in M["R20"].body, True, "tool error text")
    ask(C, "A Zapstore filter on a Routstr metric is rejected?",
        ["R21"], M["R21"].error and "keyed by provider" in M["R21"].body, True, "tool error text")

    C = "Live page - /routstr"
    for key, col in (("routstr_providers_announced", "providers_announced"),
                     ("routstr_providers_active_30d", "providers_active_30d"),
                     ("routstr_revenue_30d_sats", "revenue_30d_sats"),
                     ("routstr_revenue_alltime_sats", "revenue_alltime_sats"),
                     ("routstr_requests_30d", "requests_30d"),
                     ("routstr_tokens_30d", "tokens_30d")):
        ask(C, f"The live /routstr page's {col} equals network_summary?",
            ["R01"], int(r01[col]), W[key], "the SSR-embedded document the page renders")
    ask(C, "The live page and the tools read the SAME refresh?",
        ["R01"], r01["refresh_generated_at"], W["routstr_generated_utc"].replace(" ", "T"),
        "generated_utc embedded in the page vs the envelope's refresh instant")
