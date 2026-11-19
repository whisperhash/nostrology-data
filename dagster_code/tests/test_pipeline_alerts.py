"""pipeline/alerts.py: the Discord failure and recovery posts (pure, no dagster)."""

import os

from dagster_code.pipeline import alerts, lineage

# The data assets' edges as the live Dagster graph reports them (assetNodes, 2026-10-17, and the
# deletions extract's edge from 2026-11-04), for the three jobs these cases exercise, plus the
# declared consumers.
_DATA = {
    "marketplaces_snapshot": {"marketplaces_listings_extract", "marketplaces_shipping_extract",
                              "marketplaces_deletions_extract"},
    "marketplaces_listings_extract": set(), "marketplaces_shipping_extract": set(),
    "marketplaces_deletions_extract": {"marketplaces_listings_extract"},
    "lightning_profiles_table": {"lightning_profiles_extract"},
    "lightning_zaps_table": {"lightning_zaps_extract"},
    "lightning_mv": {"lightning_profiles_table", "lightning_zaps_table", "lightning_signers"},
    "lightning_signers": set(),
    "zapstore_catalog_table": {"zapstore_catalog_extract"},
    "zapstore_releases_table": {"zapstore_releases_extract"},
    "zapstore_stacks_table": {"zapstore_stacks_extract"},
    "zapstore_zaps_table": {"zapstore_zaps_extract"},
}
UPSTREAM = {**_DATA, **lineage.consumer_upstream()}
IDS = [lineage.consumer_id(c["key"]) for c in lineage.CONSUMERS]
LABELS = lineage.consumer_labels()

ENDED = 1792203392.0          # 2026-10-17 02:16:32 UTC, run 15d90e14's end
LAST_GOOD = 1792117018.0      # 2026-10-16 02:16:58 UTC, run 67834931's end
NEXT = 1792289700.0           # 2026-10-18 02:15:00 UTC
CAUSE = ("RuntimeError: flush k=5 returned HTTP 502 - a shard's publish FAILED; its rows are "
         "stranded in the WAL, NOT in S3. Refusing to extract SHORT. Investigate the producer, then re-run.")


def _failure(**over):
    kw = dict(job="marketplaces_refresh", run_id="15d90e14-03df-4f7a-a913-8c68c082318f",
              ended=ENDED, duration=88.0, trigger="scheduled", streak="1st failure after 38 ok",
              not_refreshed={"marketplaces_deletions_extract", "marketplaces_snapshot"},
              refreshed={"marketplaces_listings_extract", "marketplaces_shipping_extract"},
              upstream=UPSTREAM, consumer_ids=IDS, labels=LABELS,
              last_good={"marketplaces_snapshot": LAST_GOOD, "marketplaces_deletions_extract": LAST_GOOD},
              step="marketplaces_deletions_extract", cause=CAUSE, next_run=NEXT)
    kw.update(over)
    return alerts.failure_payload(**kw)


def _embed(payload):
    assert payload["allowed_mentions"] == {"parse": []}
    (e,) = payload["embeds"]
    return e


def test_last_nights_marketplaces_failure():
    e = _embed(_failure())
    assert e["title"] == "\u274c marketplaces_refresh failed: 2 surfaces affected"
    assert e["url"] == "http://localhost:23010/runs/15d90e14-03df-4f7a-a913-8c68c082318f"
    assert e["color"] == alerts.RED
    d = e["description"]
    assert "web: [/marketplaces](https://nostrolo.gy/marketplaces)" in d
    assert "landing: marketplaces" in d
    assert f"`marketplaces_snapshot` <t:{int(LAST_GOOD)}:f> (<t:{int(LAST_GOOD)}:R>)" in d
    assert "marketplaces_deletions_extract`" not in d.split("**Why:**")[0]  # an extract is not a product
    assert "Refreshed in this run" not in d  # the refreshed extracts feed no consumer directly
    assert "**Why:** `[marketplaces_deletions_extract] RuntimeError: flush k=5 returned HTTP 502" in d
    assert f"Next scheduled run <t:{int(NEXT)}:f>" in d
    assert ("Re-run now (safe, every step re-checks its inputs): `Deploy (dev)` with "
            "`repo=nostrology-data` and `action=materialize-marketplaces`, or in the Dagster UI "
            "(through a tunnel), open the run, Re-execute > All steps.") in d
    assert "https://github.com/" not in d  # the deploy plane is not this repository's to link
    assert e["footer"]["text"] == ("dev = nostrolo.gy | run 15d90e14 | scheduled | 1m28s | "
                                   "1st failure after 38 ok")
    assert e["timestamp"] == "2026-10-17T02:16:32Z"


def test_a_job_without_a_workflow_rerun_names_only_the_ui():
    d = _embed(_failure(job="lightning_refresh"))["description"]
    assert d.endswith("Re-run now (safe, every step re-checks its inputs): "
                      "in the Dagster UI (through a tunnel), open the run, Re-execute > All steps.")


def test_the_named_reruns_are_the_deployments_verbs():
    """Every action an alert names is the deployment's verb for its job, materialize-<surface> for
    <surface>_refresh, and these five are the ones it has. The script that holds those verbs left
    this tree on 2026-11-16, and the deployment's own checks hold each verb to a `dagster job launch`
    of exactly its job, and the set to its menu: so a renamed or added action fails here, on this
    side, not in a post that names an action the box does not have."""
    assert all(action == "materialize-" + job[:-len("_refresh")] and job.endswith("_refresh")
               for job, action in alerts.RERUN_ACTIONS.items())
    assert set(alerts.RERUN_ACTIONS) == {"marketplaces_refresh", "ngit_refresh", "nutzaps_refresh",
                                         "routstr_refresh", "zapstore_refresh"}


def test_affected_lines_come_first():
    d = _embed(_failure())["description"]
    assert d.index("**Affected**") < d.index("**Not refreshed**") < d.index("**Why:**") < d.index("**Heals:**")


def test_one_job_reaches_several_surfaces_through_the_lineage():
    """A lightning_refresh failure before its loads: /lightning, two landing panels, and through
    the kind-0 (profile-metadata) table /zapstore and the MCP zapstore domain."""
    stale = {"lightning_profiles_table", "lightning_zaps_table", "lightning_mv"}
    affected = alerts.affected_consumers(stale, UPSTREAM, IDS)
    assert affected == {"consumer/web/lightning", "consumer/web/zapstore",
                        "consumer/landing/lightningProviders", "consumer/landing/lightningShare",
                        "consumer/mcp/zapstore"}
    e = _embed(_failure(job="lightning_refresh", not_refreshed=stale, refreshed={"lightning_signers"},
                        last_good={}, step="lightning_zaps_table", cause="x"))
    assert e["title"].endswith("failed: 5 surfaces affected")
    assert "mcp: zapstore" in e["description"]
    assert "`lightning_profiles_table` never" in e["description"]


def test_a_partly_refreshed_surface_is_called_mixed():
    """zapstore: the catalog reloaded, the releases did not; /zapstore reads both."""
    e = _embed(_failure(job="zapstore_refresh",
                        not_refreshed={"zapstore_releases_table", "zapstore_stacks_table", "zapstore_zaps_table"},
                        refreshed={"zapstore_catalog_extract", "zapstore_catalog_table"},
                        last_good={}, step="zapstore_releases_table", cause="x"))
    assert "**Refreshed in this run:** `zapstore_catalog_table` - readers now combine it" in e["description"]


def test_no_declared_consumer_lists_everything_not_refreshed():
    e = _embed(_failure(job="nectar_refresh", not_refreshed={"nectar_native_releases_parquet"},
                        refreshed=set(), last_good={}, step="s", cause="c", next_run=None))
    assert e["title"] == "\u274c nectar_refresh failed: no declared surface affected"
    assert "`nectar_native_releases_parquet` never" in e["description"]
    assert "Next scheduled run" not in e["description"]


def test_error_text_cannot_ping_link_or_break_out():
    nasty = "boom @everyone [x](https://evil.example) `tick`\nsecond line " + "y" * 900
    d = _embed(_failure(cause=nasty))["description"]
    why = d.split("**Why:** ", 1)[1].split("\n", 1)[0]
    assert why.startswith("`") and why.endswith("`") and why.count("`") == 2
    assert "second line" in why and len(why) <= alerts.CAUSE_MAX + 2


def test_limits_hold_whatever_the_inputs():
    many = {f"asset_{i:04d}" for i in range(2000)}
    e = _embed(_failure(job="j" * 400, not_refreshed=many, refreshed=set(), last_good={}))
    assert len(e["title"]) <= alerts.TITLE_MAX and len(e["description"]) <= alerts.DESC_MAX


def test_recovery_says_what_is_current_again():
    e = _embed(alerts.recovery_payload(
        job="marketplaces_refresh", run_id="5a43d0b2-4bfe-4684-985c-b440e902ce52", ended=1792216973.0,
        duration=89.0, trigger="manual",
        refreshed={n for n in _DATA if n.startswith("marketplaces_")},
        upstream=UPSTREAM, consumer_ids=IDS, labels=LABELS, failed_runs=1, failing_since=1792203304.0))
    assert e["color"] == alerts.GREEN
    assert e["title"] == "\u2705 marketplaces_refresh recovered: 2 surfaces current again"
    assert "web: [/marketplaces](https://nostrolo.gy/marketplaces)" in e["description"]
    assert "Failing since <t:1792203304:f>" in e["description"] and "1 failed run in a row" in e["description"]
    assert e["footer"]["text"] == "dev = nostrolo.gy | run 5a43d0b2 | manual | 1m29s"


def test_streaks():
    assert alerts.streak_text(["FAILURE"] + ["SUCCESS"] * 38, 50) == "1st failure after 38 ok"
    assert alerts.streak_text(["FAILURE"] + ["SUCCESS"] * 49, 50) == "1st failure after 49+ ok"
    assert alerts.streak_text(["FAILURE", "FAILURE", "SUCCESS"], 50) == "2nd failure in a row"
    assert alerts.streak_text(["FAILURE"] * 3, 50) == "3rd failure in a row"
    assert alerts.streak_text(["FAILURE"], 50) == "first run"
    assert alerts.streak_text(["SUCCESS", "SUCCESS"], 50) == "1st failure after 2 ok"  # not listed yet


def test_next_daily_run():
    assert alerts.next_daily_run("15 2 * * *", ENDED) == NEXT
    assert alerts.next_daily_run("15 2 * * *", NEXT - 1) == NEXT
    assert alerts.next_daily_run("15 2 * * *", NEXT) == NEXT + 86400
    assert alerts.next_daily_run("*/5 * * * *", ENDED) is None
    assert alerts.next_daily_run("", ENDED) is None


def test_small_formatters():
    assert alerts.fmt_duration(88) == "1m28s" and alerts.fmt_duration(5) == "5s"
    assert alerts.fmt_duration(3720) == "1h02m" and alerts.fmt_duration(None) == "?"
    assert [alerts.ordinal(n) for n in (1, 2, 3, 4, 11, 12, 13, 21, 22)] == [
        "1st", "2nd", "3rd", "4th", "11th", "12th", "13th", "21st", "22nd"]
    assert alerts.trigger_label({"dagster/schedule_name": "marketplaces_daily"}) == "scheduled"
    assert alerts.trigger_label({"dagster/parent_run_id": "x", "dagster/schedule_name": "s"}) == "re-execution"
    assert alerts.trigger_label({}) == "manual"


def test_sources_are_ascii():
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for name in ("alerts.py", "lineage.py"):
        with open(os.path.join(here, "pipeline", name), encoding="utf-8") as fh:
            text = fh.read()
        assert all(ord(ch) < 128 for ch in text), name
