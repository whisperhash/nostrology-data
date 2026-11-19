"""The /git job's wiring: what the pure compute's unit tests cannot see.

A missing parse label or build argument fails the run loudly at run time (KeyError, or
TypeError for build_snapshot's keyword-only arguments). These pin the parts that would fail
SILENTLY instead: an extract left out of the job or the compute's deps, or a subset extract
whose emptiness only warns. Needs dagster, so it skips on a workstation (run it in the image).
"""

import pytest

dg = pytest.importorskip("dagster", reason="dagster is not installed locally")
from dagster_code import assets, definitions  # noqa: E402

# kind -> (asset name, may its UNLOAD part set be empty?)
_EXTRACTS = {
    "30617": ("ngit_announcements_extract", False),
    "30618": ("ngit_states_extract", False),
    "10317": ("ngit_grasp_lists_extract", False),
    "1111": ("ngit_comments_extract", False),
    "7": ("ngit_reactions_extract", True),
    # kind-17 (external-content-reactions), Iris Git's stars: blocking from its first run,
    # when the anti-shrink gate has no served value to compare with (pre-deploy review M1)
    "17": ("ngit_external_reactions_extract", False),
    "9735": ("ngit_zaps_extract", True),
    "5": ("ngit_deletions_extract", True),
}


def test_every_ngit_extract_is_a_labelled_part_with_its_emptiness_rule():
    assert sorted(kind for kind, _may in assets._NGIT_PARTS.values()) == sorted(_EXTRACTS)
    for _label, (kind, may_be_empty) in assets._NGIT_PARTS.items():
        assert may_be_empty is _EXTRACTS[kind][1], kind
    assert assets._NGIT_PARTS["external_reactions"] == ("17", False)


def test_every_ngit_extract_is_in_the_job_and_feeds_the_snapshot():
    job_assets = {a.key.to_user_string() for a in definitions._NGIT_ASSETS}
    deps = {k.to_user_string() for k in assets.ngit_snapshot.dependency_keys}
    for kind, (name, _may) in _EXTRACTS.items():
        assert name in job_assets, kind
        assert name in deps, kind
    assert "ngit_snapshot" in job_assets


def test_only_the_three_unmeasured_subsets_warn_on_an_empty_extract():
    for kind, (name, may_be_empty) in _EXTRACTS.items():
        extract = getattr(assets, name)
        (check,) = [c for c in extract.check_specs if c.name == "extract_nonempty"]
        assert check.blocking is (not may_be_empty), kind


def test_the_held_keys_check_is_declared_blocking():
    """The run is held by the raise before write_pg (test_both_write_gates_run_before_write_pg
    in test_pipeline_ngit.py), not by this flag: the check never reports passed=False, and
    ngit_snapshot has nothing downstream. The flag pins the declared contract, as
    snapshot_invariants' does, so a later change that reports a failure instead of raising
    still holds the run."""
    by_name = {c.name: c for c in assets.ngit_snapshot.check_specs}
    assert by_name["held_keys_absent"].blocking is True
    assert by_name["snapshot_invariants"].blocking is True
