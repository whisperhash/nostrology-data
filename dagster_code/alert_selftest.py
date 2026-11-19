"""Post clearly marked TEST samples of the Dagster failure and recovery posts through the real
post path, from inside the dagster-code container, to prove the webhook is wired end to end
(container env -> egress -> Discord) and to show the format in the channel:

    docker exec dagster-code python -m dagster_code.alert_selftest            # post both
    docker exec dagster-code python -m dagster_code.alert_selftest --dry-run  # print the JSON
    docker exec dagster-code python -m dagster_code.alert_selftest --probe    # post nothing

The samples replay a real incident with the live lineage graph: marketplaces_refresh's
2026-10-17 02:15 UTC failure (run 15d90e14, a kind-5 (event-deletion-requests) shard's upload
failed under flush starvation) and its 06:01 recovery (run 5a43d0b2). Each title starts with
"TEST - " so nobody mistakes them for an incident. Exit 0 when both posted (or printed).

--probe is the check the deployment runs after every Dagster deploy: one line saying
whether the alerts can reach Discord from here, exit 0 only when they can.
"""

import argparse
import json
import os
import re
import sys
import types
import urllib.error
import urllib.request

from .pipeline import alerts, lineage
from .sensors import DISCORD_HEADERS, _post_discord, http_status

_FAILED_RUN = "15d90e14-03df-4f7a-a913-8c68c082318f"
_RECOVERY_RUN = "5a43d0b2-4bfe-4684-985c-b440e902ce52"
_CAUSE = ("RuntimeError: flush k=5 returned HTTP 502 - a shard's publish FAILED; its rows are "
          "stranded in the WAL, NOT in S3. Refusing to extract SHORT. Investigate the producer, then re-run.")


def samples() -> list:
    from .definitions import _CONSUMER_IDS, _UPSTREAM

    labels = lineage.consumer_labels()
    marketplaces = {n for n in _UPSTREAM if n.startswith("marketplaces_")}
    fail = alerts.failure_payload(
        job="marketplaces_refresh", run_id=_FAILED_RUN, ended=1792203392.0, duration=88.0,
        trigger="scheduled", streak="1st failure after 38 ok",
        not_refreshed={"marketplaces_deletions_extract", "marketplaces_snapshot"},
        refreshed={"marketplaces_listings_extract", "marketplaces_shipping_extract"},
        upstream=_UPSTREAM, consumer_ids=_CONSUMER_IDS, labels=labels,
        last_good={"marketplaces_snapshot": 1792117018.0, "marketplaces_deletions_extract": 1792117018.0},
        step="marketplaces_deletions_extract", cause=_CAUSE, next_run=1792289700.0)
    ok = alerts.recovery_payload(
        job="marketplaces_refresh", run_id=_RECOVERY_RUN, ended=1792216973.0, duration=89.0,
        trigger="manual", refreshed=marketplaces, upstream=_UPSTREAM, consumer_ids=_CONSUMER_IDS,
        labels=labels, failed_runs=1, failing_since=1792203304.0)
    for p in (fail, ok):
        p["embeds"][0]["title"] = "TEST - " + p["embeds"][0]["title"]
        p["embeds"][0]["footer"]["text"] += " | test post: a replay of 2026-10-17"
    return [fail, ok]


def _refusal_code(exc: urllib.error.HTTPError) -> str:
    """The numeric code in a refusal body, when there is one: Discord's own (10015 Unknown Webhook,
    50027 Invalid Webhook Token) or its edge's (1010, a refused User-Agent). Only the number is
    printed, never the body."""
    try:
        text = exc.read(512).decode("utf-8", "replace")
    except Exception:
        return ""
    m = re.search(r'"code":\s*(\d+)', text)
    if m:
        return f", Discord code {m.group(1)}"
    m = re.search(r"error code: (\d+)", text)
    return f", edge error code {m.group(1)}" if m else ""


def probe() -> tuple:
    """(ok, one line for the deploy log): is DAGSTER_DISCORD_WEBHOOK set here, and does Discord
    accept a request from here? A GET of a webhook reads its metadata and posts nothing, and it
    takes the alerts' own path - this container's env and egress, the post's headers - so it
    catches both faults that kept every alert out of Discord until 2026-10-17: no heartbeat.env,
    and the edge's 403 (forbidden) for urllib's default User-Agent. Never prints the URL, or the
    response body, which carries the token."""
    webhook = os.environ.get("DAGSTER_DISCORD_WEBHOOK", "").strip()
    if not webhook:
        return False, ("Discord alerts OFF: DAGSTER_DISCORD_WEBHOOK is empty in this container "
                       "(no readable heartbeat.env at deploy time) - every post is dropped")
    try:
        req = urllib.request.Request(webhook, headers=DISCORD_HEADERS, method="GET")
        with urllib.request.urlopen(req, timeout=10) as resp:
            status, meta = resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        return False, (f"Discord alerts BROKEN: Discord refused the request - HTTP "
                       f"{http_status(exc.code)}{_refusal_code(exc)} - every post will fail")
    except Exception as exc:  # str(exc) of a urllib error can carry the URL: name the type only
        return False, f"Discord alerts UNVERIFIED: no answer from Discord ({type(exc).__name__})"
    name = ascii(str(meta.get("name", "?"))[:80])
    channel = ascii(str(meta.get("channel_id", "?"))[:24])
    return True, (f"Discord alerts ON: Discord accepts the webhook - HTTP {http_status(status)}, "
                  f"webhook {name}, channel {channel}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="post TEST samples of the Dagster Discord alerts")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="print the payloads instead of posting")
    mode.add_argument("--probe", action="store_true",
                      help="post nothing; say whether Discord accepts the webhook from here")
    args = ap.parse_args(argv)
    if args.probe:
        ok, line = probe()
        print(line)
        return 0 if ok else 1
    payloads = samples()
    if args.dry_run:
        print(json.dumps(payloads, indent=2))
        return 0
    log = types.SimpleNamespace(info=print, warning=print)
    results = [_post_discord(types.SimpleNamespace(log=log), p) for p in payloads]
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
