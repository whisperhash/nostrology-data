#!/usr/bin/env python3
"""One row per product through BOTH /marketplaces implementations, on the same events, row by row.

compare_markets_snapshots.py compares what the archive holds, and the archive holds few of the
shapes where the two could part: on 2026-11-04, 13 of 14 deliberate one-sided changes to this rule
still gave FULL AGREEMENT there (no key scope, a child standing as a parent, the highest id breaking
a tie). A change on one side alone would surface only when a seller first published such a shape.
So this generates those shapes: a stranger's key, capitals, stray spaces, cycles, chains, ties, tags
with missing or NULL values, no d tag beside an empty one, deletions. It writes them to Parquet the
way the generator reads the archive, and hands production the same events as parsed JSON, the way
its extract delivers them, in another order, because neither Athena's part order nor DuckDB's scan
order is anything to rely on. Then each coordinate has to agree on what it read and how it was
classified and folded. It also runs production's blocking check_invariants on every seed, so a
stranger's event that would stop the nightly refresh fails here.

Run it as the gate's first step, from the repo root. No AWS and no network:
  .venv-crosscheck/bin/python -I crosscheck/compare_markets_fold.py

Exit 0 = every seed agreed; 1 = a difference or a broken invariant (printed, capped).
"""
import collections
import csv
import hashlib
import importlib.util
import json
import os
import random
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
# Either side can be pointed at a scratch copy, for a run with a deliberate one-sided change to a
# rule. markets_geolib can have none: both sides import the one module, and no field here reads it.
DATA_REPO = os.environ.get("NOSTROLOGY_DATA", os.path.dirname(HERE))
GENERATOR = os.environ.get("MARKETS_GENERATOR", os.path.join(HERE, "build_markets_snapshot.py"))
sys.path[:0] = [DATA_REPO]

import duckdb  # noqa: E402

from dagster_code.pipeline import marketplaces as production  # noqa: E402

_spec = importlib.util.spec_from_file_location("build_markets_snapshot", GENERATOR)
generator = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(generator)

NOW = 1_800_000_000
SEEDS = 30
EVENTS_PER_SEED = 400
# What both implementations read off a coordinate and what they make of it. `excl` and `price` are
# the rule's output; the others say which of the two read the event differently when those differ.
FIELDS = ("event_id", "created_at", "product_type", "parent", "title", "status", "vis", "stock", "client",
          "unrenderable", "synthetic", "nip09_deleted", "withdrawn", "excl", "price")
KEYS = [hashlib.sha256(f"key-{i}".encode()).hexdigest() for i in range(5)]
TYPES = [["type", "variation", "physical"]] * 8 + [
    ["type", "variation"], ["type", "variable", "physical"], ["type", "simple"], ["type"], ["type", None], ["type", ""],
    ["type", "Variation"], ["type", " variation"], ["type", "variation "], ["type", None, "variation"], [None, "variation"],
    ["Type", "variation"]]
PRICES = [["price", "9", "USD"]] * 4 + [
    ["price", "7", "sats"], ["price", "9"], ["price"], ["price", None], ["price", "9", None], ["price", "9", ""],
    ["price", "9", " "], ["price", "9", "\t"], ["price", "", "USD"], ["price", None, "USD"], ["price", "Infinity", "USD"],
    ["price", "9", "USD", "monthly"], ["Price", "9", "USD"]]


def a_tags(rng, pubkey, d_tags):
    """Mostly the seller's own parents, then everything that merely resembles one."""
    # A few parents take most of the children, so that families have several priced members to choose among.
    named = rng.choice(d_tags + ["absent", "TEE"] + [""] * 6 if rng.random() < .3 else ["tee", "d0", "d1", "d2", "absent"])
    coordinate = f"30402:{pubkey}:{named or ''}"
    return rng.choice([
        ["a", coordinate], ["a", coordinate], ["a", coordinate], ["a", coordinate], ["a", coordinate, "wss://relay.example"],
        ["a", f"30402:{rng.choice(KEYS)}:{named or ''}"], ["a", f"30402:{pubkey.upper()}:{named or ''}"],
        ["a", coordinate + " "], ["a", " " + coordinate], ["a", coordinate.upper()], ["a", f"304021:{pubkey}:{named or ''}"],
        ["a", f"30405:{pubkey}:shop"], ["a", f"30402:{pubkey}"], ["a", "30402:"], ["a"], ["a", None], ["a", ""],
        ["A", coordinate], [None, coordinate]])


def one_event(rng, seed, i):
    pubkey = rng.choice(KEYS)
    # A small space of d tags, so that coordinates collide into versions and families form.
    d_tags = [f"d{n}" for n in range(60)] + ["tee", "Tee", "tee ", "a:b:c", "", None]
    tags = [["title", f"listing {rng.randrange(2000)}" if rng.random() < .98 else "test"], rng.choice(TYPES)]
    if rng.random() < .08:
        tags.append(rng.choice(TYPES))
    for _ in range(rng.choice((0, 1, 1, 1, 1, 2, 3))):
        tags.append(a_tags(rng, pubkey, d_tags))
    if rng.random() < .6:
        price = list(rng.choice(PRICES))
        # Distinct amounts, or two children's prices are the same value and it cannot show whose was taken.
        tags.append(["price", str(rng.randrange(1, 10 ** 6)), "USD"] if price == ["price", "9", "USD"] else price)
    if rng.random() < .12:
        tags.append(["stock", rng.choice(["0", "-1", "100", "x"])])
    if rng.random() < .04:
        tags.append(["status", "sold"])
    if rng.random() < .04:
        tags.append(["visibility", rng.choice(["hidden", "Hidden ", "on-sale"])])
    if rng.random() < .05:
        tags.append([])
    rng.shuffle(tags)
    return {"id": hashlib.sha256(f"{seed}-{i}".encode()).hexdigest(), "pubkey": pubkey,
            "created_at": NOW - rng.choice((100, 100, 100, 200, 300, 400)),
            "content": rng.choice(("body", "body", "")), "tags": tags,
            # No d tag beside an empty one is one coordinate string, which is where a tie has to be stated.
            "d_tag": rng.choice(d_tags + ["", None] * 3 + ["tee", "d0", "d1", "d2"] * 2)}


def through_generator(events, requests):
    with tempfile.TemporaryDirectory() as td:
        generator.BUCKET = td
        tree = os.path.join(td, "c=addressable", "k=30402")
        os.makedirs(tree)
        ndjson = os.path.join(td, "events.ndjson")
        with open(ndjson, "w", encoding="utf-8") as fh:
            fh.writelines(json.dumps(e) + "\n" for e in events)
        columns = ("{'id':'VARCHAR','pubkey':'VARCHAR','created_at':'BIGINT','content':'VARCHAR',"
                   "'tags':'VARCHAR[][]','d_tag':'VARCHAR'}")
        duckdb.connect().execute(
            f"COPY (SELECT * FROM read_json('{ndjson}', format='newline_delimited', columns={columns})) "
            f"TO '{os.path.join(tree, 'part.parquet')}' (FORMAT parquet)")
        deletions = os.path.join(td, "deletions.csv")
        with open(deletions, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(("author", "tag", "value", "created_at"))
            w.writerows(requests)
        con = duckdb.connect()
        con.execute("SET lambda_syntax='ENABLE_SINGLE_ARROW'")
        rows, names = generator.load_listings(con, deletions, NOW)
    recs = [dict(zip(names, r)) for r in rows]
    generator.classify(recs)
    generator.fold_product_options(recs)
    return recs


def through_production(events, requests, rng):
    parsed = json.loads(json.dumps(events))
    rng.shuffle(parsed)
    rows = [dict(zip(("author", "tag", "value", "created_at"), r)) for r in requests]
    recs = production.build_records(parsed, production.parse_deletions(rows, parsed, NOW + 2 * 86400))
    production.classify(recs)
    production.fold_product_options(recs)
    return recs


def main():
    totals = collections.Counter()
    problems = []
    for seed in range(SEEDS):
        rng = random.Random(seed)
        events = [one_event(rng, seed, i) for i in range(EVENTS_PER_SEED)]
        requests = []
        for e in rng.sample(events, 12):
            requests.append((e["pubkey"], "a", f"30402:{e['pubkey']}:{e['d_tag'] or ''}", NOW) if rng.random() < .5
                            else (e["pubkey"], "e", e["id"], NOW))
        ours = {(r["pubkey"], r["d_tag"]): r for r in through_production(events, requests, rng)}
        theirs = {(r["pubkey"], r["d_tag"]): r for r in through_generator(events, requests)}
        if set(ours) != set(theirs):
            problems.append(f"seed {seed}: coordinates differ: {sorted(map(str, set(ours) ^ set(theirs)))[:4]}")
        for key in set(ours) & set(theirs):
            for f in FIELDS:
                if ours[key].get(f) != theirs[key].get(f):
                    problems.append(f"seed {seed} {key[0][:8]}:{key[1]!r} {f}: production "
                                    f"{ours[key].get(f)!r}, generator {theirs[key].get(f)!r}")
        totals["coordinates"] += len(ours)
        totals["folded"] += sum(1 for r in ours.values() if r["excl"] == "option")
        totals["children"] += sum(1 for r in ours.values() if r["product_type"] == "variation" and r["parent"])
        recs, data = production.build_snapshot(json.loads(json.dumps(events)), [],
                                               [dict(zip(("author", "tag", "value", "created_at"), r)) for r in requests], NOW)
        problems += [f"seed {seed} check_invariants: {p}" for p in production.check_invariants(recs, data)]
    for line in problems[:40]:
        print("FAILED", line)
    if len(problems) > 40:
        print(f"... and {len(problems) - 40} more")
    # A run in which the rule never fired would agree about nothing.
    if totals["folded"] < SEEDS:
        problems.append(f"only {totals['folded']} rows folded: the fixture no longer exercises the rule")
        print("FAILED", problems[-1])
    print(f"{SEEDS} seeds, {totals['coordinates']} coordinates, {totals['children']} children, "
          f"{totals['folded']} folded: {len(problems)} differences")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
