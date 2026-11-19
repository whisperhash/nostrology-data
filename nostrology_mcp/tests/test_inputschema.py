"""Unit tests for the advertised-schema shaping.

The wired-up behaviour is covered against the real tools in test_server_tool.py;
these cover the shaper itself, including the failure mode a wired test cannot
reach - pydantic handing us a shape we did not expect."""

import pytest

from nostrology_mcp.inputschema import optional_choice, optional_value
from nostrology_mcp.metric import GRAIN_NAMES, grain_choices


def _pydantic_optional(**extra) -> dict:
    """What pydantic emits for `str | None = None` with a description."""
    return {
        "anyOf": [{"type": "string"}, {"type": "null"}],
        "default": None,
        "description": "what this parameter means",
        "title": "Some Param",
        **extra,
    }


def test_optional_value_authors_both_branches():
    schema = _pydantic_optional()
    optional_value(
        omit_title="Start date: none",
        value_title="Start date: pick a day",
        value_description="inclusive ISO UTC calendar day",
        value_format="date",
    )(schema)
    # Order matters as much as content: a form preselects the branch closest to
    # the current (empty) value, so "leave it out" must come first.
    assert schema["anyOf"] == [
        {"type": "null", "title": "Start date: none"},
        {
            "type": "string",
            "title": "Start date: pick a day",
            "description": "inclusive ISO UTC calendar day",
            "minLength": 1,
            "format": "date",
        },
    ]


def test_the_field_keeps_its_own_title_and_description():
    """Those two are what a model reads. Only the branches are ours to author -
    dropping the field description to save bytes would degrade the tool for every
    client that is not a form."""
    schema = _pydantic_optional()
    optional_value(omit_title="a", value_title="b", value_description="c")(schema)
    assert schema["title"] == "Some Param"
    assert schema["description"] == "what this parameter means"


def test_the_default_and_any_sibling_enum_are_removed():
    """The sibling `enum` is the one that made the null branch fail validation."""
    schema = _pydantic_optional(enum=["a", "b"])
    optional_value(omit_title="a", value_title="b", value_description="c")(schema)
    assert "default" not in schema
    assert "enum" not in schema


def test_optional_choice_makes_one_single_value_branch_per_value():
    schema = _pydantic_optional()
    optional_choice(
        omit_title="Grain: default for this metric",
        choices=[("day", "Grain: day"), ("total", "Grain: total")],
    )(schema)
    assert schema["anyOf"] == [
        {"type": "null", "title": "Grain: default for this metric"},
        {"type": "string", "enum": ["day"], "title": "Grain: day"},
        {"type": "string", "enum": ["total"], "title": "Grain: total"},
    ]


def test_an_unexpected_pydantic_shape_fails_loudly():
    """A silent no-op would ship an unshaped schema and re-break the form, and the
    only thing that can change this shape is a library upgrade - which is exactly
    when a loud failure in CI is cheapest."""
    for shape in ({"type": "string"}, {"anyOf": "not-a-list"}, {}):
        with pytest.raises(RuntimeError, match="anyOf"):
            optional_value(omit_title="a", value_title="b", value_description="c")(dict(shape))


def test_grain_choices_cover_every_registered_grain_in_order():
    """Registry-derived, never retyped: every registered grain gets a pair, in
    GRAIN_NAMES order, each with a non-empty human title."""
    pairs = grain_choices()
    assert [value for value, _ in pairs] == list(GRAIN_NAMES)
    assert all(title.strip() for _, title in pairs)
    assert "daily series" in dict(pairs)["day"]


def test_a_grain_with_no_prose_still_gets_its_pair(monkeypatch):
    """The fallback that keeps a missing phrase from dropping a legal value out of
    the advertised schema - i.e. from making a real grain uncallable by a client
    that reads the branches."""
    from nostrology_mcp import metric as metric_mod

    monkeypatch.setattr(metric_mod, "GRAIN_NAMES", ("day", "fortnight"))
    assert dict(metric_mod.grain_choices())["fortnight"] == "Grain: fortnight"
