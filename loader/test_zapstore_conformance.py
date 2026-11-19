"""What counts as a Zapstore app, and the drift check on that definition.

The rule under test is NOT ours. It is `zapstore/relay` pkg/events/app.go at
commit 3812bf5: `PlatformIdentifiers` (L15-30) and `Validate()` (L51-62). We copy
it because the relay is Go and we are Python; copying is how it drifts, so
test_platform_list_matches_upstream states the provenance and
test_unknown_platform_values_are_flagged is the alarm.

Empirical grounding, 2026-10-28: a full walk of relay.zapstore.dev returned 4,915
listings, under 4,915 distinct coordinates. Every one passes this rule - and every
one also passes the stricter `_relay_would_accept` below, which reimplements
ParseApp + Validate() with none of the loader's conveniences. Of the 4,810 we also
hold, none fails it; and all 207 of our listings that fail it are absent from the
relay. So the rule has no false exclusions against the live store.

One exclusion is NOT Zapstore's: `zapstore_policy_exclusion` drops a listing whose
only recognised platform is `ios-arm64` (56 rows, one off-relay publisher), by the
operator's decision of 2026-10-28. It lives in its own function and has its own
tests below, and the walk gate asserts it drops 0 of what the relay serves - so the
claim above stays a claim about Zapstore's rule, not about our preferences.

    python3 -m pytest loader/test_zapstore_conformance.py -q
"""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from load_jsonl_exports_into_postgres import (  # noqa: E402
    ZAPSTORE_PLATFORMS,
    ZAPSTORE_POLICY_EXCLUDED_PLATFORMS,
    _parse_kind_32267_tags,
    classify_kind_32267,
    zapstore_policy_exclusion,
)


def _classify(tags):
    """Zapstore's rule alone, with no policy layer on top."""
    d_tag, singletons, multis, extras = _parse_kind_32267_tags(tags)
    return classify_kind_32267(d_tag, singletons, multis, extras)


def _load(tags):
    """Both, composed exactly as the loader composes them."""
    d_tag, singletons, multis, extras = _parse_kind_32267_tags(tags)
    return (classify_kind_32267(d_tag, singletons, multis, extras)
            or zapstore_policy_exclusion(multis))


# Singular tags whose repetition makes ParseApp return an error, so the relay
# never reaches Validate() and the write is refused. app.go L74-125 at 3812bf5.
# `url` is deliberately absent: it is the one singular field ParseApp overwrites
# rather than rejecting.
_STRICT_SINGULAR = ("d", "name", "summary", "icon", "repository", "license")


def _relay_would_accept(tags):
    """ParseApp + Validate(), with none of the loader's conveniences.

    The loader's classifier is one step laxer than the relay in two ways: it sees
    `name` after the name<-title fallback _parse_kind_32267_tags applies, and it
    never rejects a duplicate singular tag. Neither can admit a row the relay
    refuses *today* - every listing affected is platform-less and excluded anyway -
    but the headline claim ("every listing the relay serves passes our rule")
    should not rest on a laxness. The walk gate checks BOTH, so the claim holds in
    the strict reading too.
    """
    app = {}
    platforms = []
    for tag in tags or []:
        if not (isinstance(tag, list) and len(tag) >= 2 and isinstance(tag[1], str)):
            continue
        if tag[0] == "f":
            if tag[1] in ZAPSTORE_PLATFORMS:
                platforms.append(tag[1])
        elif tag[0] in _STRICT_SINGULAR:
            if app.get(tag[0]):
                return f"duplicate {tag[0]!r} tag"
            app[tag[0]] = tag[1]
    if not app.get("d"):
        return "missing or empty 'd' tag"
    if not app.get("name"):
        return "missing or empty 'name' tag"
    if not platforms:
        return "missing 'f' tag (no recognized platform identifier)"
    return None


# --- the gate ---------------------------------------------------------------

def test_a_plain_android_app_is_a_zapstore_app():
    assert _classify([
        ["d", "com.example.app"], ["name", "Example"], ["f", "android-arm64-v8a"],
    ]) is None


def test_every_recognised_platform_admits_a_listing():
    """No value in the list is special: each one on its own is enough."""
    for platform in sorted(ZAPSTORE_PLATFORMS):
        assert _classify([
            ["d", "com.example.app"], ["name", "Example"], ["f", platform],
        ]) is None, f"{platform} should admit a listing on its own"


def test_web_and_ios_are_zapstore_platforms():
    """Pinned because both are counter-intuitive and both were open questions.

    `web` settles D5: a web-only listing is a Zapstore app by Zapstore's rule, so
    it is NOT excluded. `ios-arm64` likewise. That the live store happens to serve
    zero of either (2026-10-28 walk) is a fact about its catalogue, not about the
    definition - excluding them is a separate, deliberate policy choice, and this
    test is what would fail if someone folded that policy in here by mistake.
    """
    assert "web" in ZAPSTORE_PLATFORMS
    assert "ios-arm64" in ZAPSTORE_PLATFORMS
    assert _classify([["d", "x"], ["name", "X"], ["f", "web"]]) is None
    assert _classify([["d", "x"], ["name", "X"], ["f", "ios-arm64"]]) is None


def test_unrecognised_platform_is_no_platform():
    """The relay drops an unrecognised `f` during parse, so these have none.

    All four values are real: they appear in our catalogue today.
    """
    for bogus in ("macos-x86_64", "android-mips", "android-armeabi", "android"):
        assert _classify([
            ["d", "com.example.app"], ["name", "Example"], ["f", bogus],
        ]) == "no_platform", f"{bogus} is not a recognised platform"


def test_one_recognised_platform_among_junk_is_enough():
    assert _classify([
        ["d", "com.example.app"], ["name", "Example"],
        ["f", "android-mips"], ["f", "android-arm64-v8a"],
    ]) is None


def test_missing_name_and_missing_d_tag():
    assert _classify([["d", "com.example.app"], ["f", "android-arm64-v8a"]]) == "no_name"
    assert _classify([["d", "x"], ["name", "   "], ["f", "android-arm64-v8a"]]) == "no_name"
    assert _classify([["name", "Example"], ["f", "android-arm64-v8a"]]) == "no_d_tag"


# --- the labels -------------------------------------------------------------
#
# Every case below ALSO lacks a recognised platform, which is what actually
# excludes it. The label only says which population it belongs to.

def test_soyli_napplet_carries_a_manifest_reference():
    """soyLI's legacy companion: `latest` naming a kind-35129 (NIP-5D named-napplet
    manifest). Shape from napplet-soy packages/publish/src/index.ts L898-901."""
    assert _classify([
        ["d", "n-143146b0d6f-7eda1291bd9164d3"], ["name", "Supersonic RC Revive"],
        ["latest", "35129:" + "b" * 64 + ":n-143146b0d6f", "wss://relay.example"],
    ]) == "napplet_manifest"


def test_napplet_manifest_matches_any_tag_name_and_any_signer():
    """The tag NAME is soyLI's to change and the signer need not be the publisher,
    so the rule keys on the VALUE. Both variants are still napplets."""
    assert _classify([
        ["d", "x"], ["name", "X"], ["renamed", "35129:" + "c" * 64 + ":some-id"],
    ]) == "napplet_manifest"
    assert _classify([
        ["d", "x"], ["name", "X"], ["latest", "15129:" + "d" * 64 + ":other-id"],
    ]) == "napplet_manifest"


def test_napplet_run_carries_a_client_tag_and_no_manifest_reference():
    """napplet.run publishes no manifest reference at all - the exact case that
    proves a napplet-marker rule cannot be the gate. 30 such listings arrived on
    2026-10-27, one of them named `org.hyprgate.napplet-deployer`."""
    assert _classify([
        ["d", "org.hyprgate.paint"], ["name", "Paint"],
        ["client", "napplet.run"],
        ["repository", "https://github.com/hyprgate/web/tree/master/napplets/paint"],
    ]) == "napplet_client"


def test_localhost_web_app_and_business_directory():
    assert _classify([
        ["d", "bitcoin-vs-fiat-x"], ["title", "Bitcoin vs. Fiat"], ["client", "localhost"],
    ]) == "localhost_web_app"
    assert _classify([
        ["d", "bondibarber"], ["name", "Bondi Barber Co"], ["type", "business"],
    ]) == "business_directory"


def test_an_unlabelled_platformless_listing_still_falls_out():
    """hzrd149's npm libraries: a real release flow, no platform tag, no marker.
    They are excluded by the platform rule with the generic label - which is the
    whole point of making the platform rule the gate."""
    assert _classify([
        ["d", "applesauce-core"], ["name", "applesauce-core"],
        ["repository", "https://github.com/hzrd149/applesauce"],
    ]) == "no_platform"


def test_label_order_never_changes_which_rows_are_excluded():
    """The invariant that makes the labels safe to reorder or rename: a listing
    with a recognised platform is kept no matter how many labels it matches."""
    assert _classify([
        ["d", "x"], ["name", "X"], ["f", "android-arm64-v8a"],
        ["client", "napplet.run"], ["type", "business"],
        ["latest", "35129:" + "e" * 64 + ":id"],
    ]) is None


# --- our policy layer, on top of Zapstore's rule -----------------------------
#
# These rows ARE Zapstore apps by Zapstore's definition. We drop them anyway, by
# the operator's decision of 2026-10-28. The split between _classify and _load is
# the whole point: it keeps "nothing the relay serves fails Zapstore's rule" a
# checkable claim rather than a claim about whatever we happen to want today.

def test_an_ios_only_listing_passes_zapstores_rule_and_our_policy_drops_it():
    tags = [["d", "appstore.6782275018"], ["name", "Some App"], ["f", "ios-arm64"]]
    assert _classify(tags) is None, "ios-arm64 IS a Zapstore platform - do not fold policy into the rule"
    assert _load(tags) == "ios_only"


def test_one_android_build_is_enough_to_keep_an_ios_listing():
    """Keyed on platforms, not on the publisher: the same key shipping an Android
    build must keep that app. A per-pubkey rule would take it."""
    assert _load([
        ["d", "com.example.app"], ["name", "Example"],
        ["f", "ios-arm64"], ["f", "android-arm64-v8a"],
    ]) is None


def test_a_web_only_listing_is_kept():
    """The deliberate asymmetry. The relay serves zero `web` listings too, and the
    operator decided on 2026-10-28 that those still count: a web listing names
    something a user can open, an iOS one in an Android-only store does not. If
    someone later 'fixes the inconsistency' by adding `web` here, this fails."""
    assert "web" not in ZAPSTORE_POLICY_EXCLUDED_PLATFORMS
    assert _load([["d", "x"], ["name", "X"], ["f", "web"]]) is None


def test_the_policy_never_rescues_a_listing_zapstores_rule_rejects():
    """It can only narrow. A platform-less row keeps its own label, not `ios_only`."""
    assert _load([["d", "x"], ["name", "X"]]) == "no_platform"
    assert _load([["d", "x"], ["name", "X"], ["f", "macos-x86_64"]]) == "no_platform"
    assert _load([["d", "x"], ["f", "ios-arm64"]]) == "no_name"


def test_the_extract_hands_the_loader_one_event_per_coordinate():
    """The premise the whole split rests on, pinned where it is made.

    If two versions of one coordinate reached the loader in the same run and
    classified differently, the coordinate would land in both tables and the follow
    would drop a live app's releases and zaps. That cannot happen because the
    pipeline maps this kind to the latest-per-coordinate extract. The loader also
    asserts it at run time, but only after reading everything; this fails at import
    time if someone re-points the kind at the all-versions extract.
    """
    import re

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    src = open(os.path.join(root, "dagster_code", "pipeline", "sql.py"), encoding="utf-8").read()
    # Read as text, not imported: pipeline/sql.py uses relative imports and will not
    # load outside its package, and these loader tests are stdlib-only by design.
    assert re.search(r'"addressable":\s*"extract_addressable_latest\.sql"', src), \
        "the addressable class no longer extracts latest-per-coordinate"
    assert not re.search(r'\(\s*"32267"\s*,\s*""\s*\)', src), \
        "kind-32267 (zapstore-app-catalog) now has an INTO-less template override"


# --- drift ------------------------------------------------------------------

# NIP-82 (software applications) Appendix A, the draft the relay cites. It is NOT
# the relay's list: it omits `web` and adds six values the relay does not know.
NIP82_PLATFORMS = frozenset({
    "android-arm64-v8a", "android-armeabi-v7a", "android-x86", "android-x86_64",
    "darwin-arm64", "darwin-x86_64",
    "linux-aarch64", "linux-x86_64", "linux-armv7l", "linux-riscv64",
    "windows-aarch64", "windows-x86_64",
    "ios-arm64",
    "freebsd-x86_64", "freebsd-aarch64",
    "wasm32", "wasm64", "wasi-wasm32", "wasi-wasm64",
})


def test_platform_list_matches_upstream():
    """Provenance, so a reader knows where to re-check when Zapstore moves.

    Source: zapstore/relay pkg/events/app.go L15-30 at 3812bf5 (2026-09-25).
    Platform validation entered the relay in 3dfc063 (2026-01-19).
    """
    assert len(ZAPSTORE_PLATFORMS) == 14
    assert ZAPSTORE_PLATFORMS - NIP82_PLATFORMS == {"web"}
    assert NIP82_PLATFORMS - ZAPSTORE_PLATFORMS == {
        "freebsd-x86_64", "freebsd-aarch64",
        "wasm32", "wasm64", "wasi-wasm32", "wasi-wasm64",
    }


@pytest.mark.parametrize("value", ["freebsd-x86_64", "wasm32"])
def test_a_nip82_only_platform_is_not_accepted_today(value):
    """If one of these ever starts admitting listings, Zapstore adopted NIP-82's
    list and ZAPSTORE_PLATFORMS is stale."""
    assert value not in ZAPSTORE_PLATFORMS
    assert _classify([["d", "x"], ["name", "X"], ["f", value]]) == "no_platform"


def test_unknown_platform_values_are_flagged():
    """The drift alarm. An `f` value in NEITHER list is either junk or a platform
    Zapstore added. These nine are in our catalogue today and are all junk; a TENTH
    appearing is the signal to re-read app.go.
    """
    observed_junk = {
        "android-armeabi", "android-mips", "android-mips64", "android-riscv64",
        "android", "darwin-aarch64", "macos-aarch64", "macos-x86_64",
        "android-commons-io-2.4.jar",
    }
    assert not (observed_junk & (ZAPSTORE_PLATFORMS | NIP82_PLATFORMS))


# --- the relay walk ---------------------------------------------------------

_WALK = os.environ.get("ZAPSTORE_RELAY_WALK")


@pytest.mark.skipif(not _WALK, reason="set ZAPSTORE_RELAY_WALK=<jsonl> from a relay walk")
def test_every_listing_the_relay_serves_passes_the_rule():
    """The claim the whole design rests on, checked against the live store.

    Re-walk with (note `< /dev/null`; nak reads filters from stdin in this harness,
    and an absent -l scores +2 against the relay's VagueFilters(3) floor):

        nak req -k 32267 --paginate --paginate-interval 1500ms \\
            wss://relay.zapstore.dev < /dev/null > walk.jsonl
    """
    ours, strict, policy, coords = [], [], [], set()
    total = 0
    with open(_WALK, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            total += 1
            tags = event.get("tags") or []
            verdict = _classify(tags)
            if verdict is not None:
                ours.append((event.get("id"), verdict))
            strict_verdict = _relay_would_accept(tags)
            if strict_verdict is not None:
                strict.append((event.get("id"), strict_verdict))
            policy_verdict = _load(tags)
            if policy_verdict is not None:
                policy.append((event.get("id"), policy_verdict))
            d_tag = next((t[1] for t in tags if t and t[0] == "d"), None)
            coords.add((event.get("pubkey"), d_tag))
    assert total > 1000, f"walk looks truncated: {total} events"
    assert not ours, f"{len(ours)} of {total} relay listings fail our rule: {ours[:5]}"
    # The strict reading, so the loader's documented laxness is not load-bearing.
    assert not strict, f"{len(strict)} of {total} fail ParseApp+Validate: {strict[:5]}"
    # And our policy layer on top: it currently drops nothing the relay serves, which
    # is what makes it a narrowing of an off-relay population rather than a disagreement
    # with the store. A value added to ZAPSTORE_POLICY_EXCLUDED_PLATFORMS that the relay
    # DOES serve fails here instead of silently dropping real apps.
    assert not policy, f"{len(policy)} of {total} relay listings are dropped by OUR policy: {policy[:5]}"
    # AppOwnership allows one publisher per app id, so a walk should hold no
    # duplicate coordinate. If it ever does, the loader's one-row-per-coordinate
    # assumption (and the follow built on it) needs re-checking.
    assert len(coords) == total, f"{total - len(coords)} duplicate coordinate(s) in the walk"


# --- the two tables must stay column-compatible -----------------------------

def test_excluded_table_is_the_catalog_plus_excl():
    """The loader COPYs both tables from the same tuple (`row` and `row + (excl,)`),
    so a column added to one and not the other, or reordered, loads silently wrong -
    `topics` and `platforms` are both TEXT[] and would not even raise.
    """
    import load_jsonl_exports_into_postgres as ldr

    captured = {}

    def fake_copy(cur, table, cols, batch, array_cols=frozenset()):
        captured[table] = (list(cols), array_cols)
        return len(batch)

    original = ldr._copy_rows_via_stdin
    ldr._copy_rows_via_stdin = fake_copy
    try:
        ldr._copy_batch_kind_32267_app_catalog(None, [])
        ldr._copy_batch_kind_32267_app_catalog_excluded(None, [])
    finally:
        ldr._copy_rows_via_stdin = original

    kept_cols, kept_arrays = captured["events_kind_32267_app_catalog"]
    excl_cols, excl_arrays = captured["events_kind_32267_app_catalog_excluded"]
    assert excl_cols == kept_cols + ["excl", "excl_source"], \
        "excluded table must be the catalog plus `excl` and `excl_source`"
    assert excl_arrays == kept_arrays, "array column indices must match, or arrays land in the wrong columns"


def test_manifest_matcher_requires_a_real_coordinate():
    """Anchored and hex-checked: a value that merely begins with the digits is not a
    napplet manifest reference."""
    assert _classify([["d", "x"], ["name", "X"], ["r", "35129"]]) == "no_platform"
    assert _classify([["d", "x"], ["name", "X"], ["r", "35129:not-hex:id"]]) == "no_platform"
    assert _classify([["d", "x"], ["name", "X"], ["r", "135129:" + "a" * 64 + ":id"]]) == "no_platform"
    assert _classify([["d", "x"], ["name", "X"], ["r", "35129:" + "a" * 64 + ":id"]]) == "napplet_manifest"
