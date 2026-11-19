"""Shaping of the ADVERTISED input schema for the tools' OPTIONAL parameters.

Every optional parameter is `str | None` in Python, and pydantic renders that as

    {"anyOf": [{"type": "string"}, {"type": "null"}], "default": null, ...}

which is correct JSON Schema and fine for a model, but hostile to the one client
that renders our schema as a FORM: contextvm.org. Its form library
(`@sjsf/form`, resolver `resolvers/basic`) routes any field carrying `anyOf` to a
combination field - a branch selector, with ONLY the selected branch subschema
rendered underneath. Two consequences, both verified by running their deployed
stack locally:

  * a branch with no `title` is labelled `"<field title> option <n>"`, so our
    parameters showed up as "Grain option 1", "From option 2" - and the field's
    own title, description and `format` never reached the rendered input at all;
  * worse, a sibling keyword next to `anyOf` still VALIDATES. `grain` carried
    `enum: [current, day, total]` beside `anyOf`, so the null branch - its own
    documented default - failed its own enum. ajv rejected it with "must be
    equal to one of the allowed values" anchored to nothing, and the form could
    not be submitted at all in its default state.

So we author the branches ourselves: every branch titled in plain English, the
null branch spelled out as the "leave this out" affordance, and the keywords a
form needs (`format`, per-branch `description`) moved INSIDE the value branch
where the renderer actually reads them.

WHAT THIS DOES NOT CHANGE. The Python annotations stay `str | None`, so nothing
about what the tool accepts moves; an off-contract value still reaches the tool
body and the call log, which is the demand-logging contract the signatures in
server.py exist to protect. `json_schema_extra` shapes only what we ADVERTISE -
it is not a validator, so nothing here can reject a call.

KEYWORD CHOICES, all made the same way: where two spellings render identically,
take the one more clients understand.

  * `anyOf`, never `oneOf`. The form library treats them identically (tested),
    while `oneOf` is outside the JSON Schema subset strict/constrained
    tool-calling clients accept.
  * a one-value `enum` per branch, never `const`. Same meaning, same rendering,
    but `enum` is in every published tool-calling subset (OpenAI structured
    outputs, Google's OpenAPI-flavoured Schema) and `const` is in none of them,
    so a client that transcodes our schema keeps the value list instead of
    dropping it.
  * NO sibling `enum` beside the branches, tempting as it is as a flat value
    list for a transcoder that drops `anyOf` entirely: a sibling `enum` is
    exactly what broke the form (it has to include `null` to admit the omit
    branch, and the first person to "tidy" that `null` away reproduces the
    outage silently). The prose value list in each parameter's field-level
    `description` carries that job instead, and a test pins it.
"""

from collections.abc import Callable, Sequence

# A pydantic `json_schema_extra` callable: it receives the generated schema for
# ONE field and mutates it in place.
SchemaShaper = Callable[[dict], None]


def _set_branches(schema: dict, branches: list[dict]) -> None:
    """Replace pydantic's Optional branches with authored ones.

    Raises if the incoming shape is not what pydantic has always produced - a
    silent no-op here would ship an unshaped schema and re-break the form, and
    the only thing that can change this shape is a pydantic upgrade, which is
    exactly when we want to be told. Raising is safe *because* of when it runs:
    the schema is built once, when `@server.tool` registers the function at
    import, so a mismatch aborts start-up rather than surfacing as a failed
    `tools/list` in front of a caller."""
    if not isinstance(schema.get("anyOf"), list):
        raise RuntimeError(
            "expected pydantic to render this optional parameter as anyOf, got "
            f"{sorted(schema)}; the shaping in inputschema.py needs updating"
        )
    # `default: null` and any sibling `enum` go: the default is implied by the
    # parameter being absent from `required`, and a sibling enum is precisely
    # the keyword that made the null branch unsubmittable.
    schema.pop("default", None)
    schema.pop("enum", None)
    schema["anyOf"] = branches


def optional_value(
    *,
    omit_title: str,
    value_title: str,
    value_description: str,
    value_format: str | None = None,
    value_max_length: int | None = None,
) -> SchemaShaper:
    """A free-text optional parameter: "leave it out" or "give a value".

    `value_format` lands on the VALUE branch, which is the only place a form
    reads it - `"date"` there is what renders a date picker. It is a named
    parameter rather than a `**kwargs` splat on purpose: a splat would let a
    typo ship a junk keyword, or let a stray `description=` silently outrank
    `value_description`, with nothing to catch either.

    `value_description` is deliberately NOT the field-level description. The
    field text is model-facing and ends "...omit for the series start"; the
    branch text is read next to a title that already says "leave it out" in
    words, so repeating the omit clause there would be noise."""

    def _shape(schema: dict) -> None:
        value_branch = {
            "type": "string",
            "title": value_title,
            "description": value_description,
            # No legal value of any of these parameters is the empty string, and
            # a cleared form field submits "" rather than omitting the key. This
            # lets a form catch that against the field instead of sending it to
            # us to be rejected as an unparseable day / unknown publisher. It
            # constrains nothing on our side (this is advertisement, not
            # validation), so "" still reaches the body and the call log.
            "minLength": 1,
        }
        if value_format is not None:
            value_branch["format"] = value_format
        if value_max_length is not None:
            # Advertisement of the server-side hard cap (the body enforces it
            # regardless - like everything here, this validates nothing on our
            # side). The paid tool's credential parameter is the consumer: its
            # cap must be VISIBLE in the schema.
            value_branch["maxLength"] = value_max_length
        _set_branches(schema, [{"type": "null", "title": omit_title}, value_branch])

    return _shape


def optional_choice(*, omit_title: str, choices: Sequence[tuple[str, str]]) -> SchemaShaper:
    """An optional parameter with a closed value set: "leave it out", or one of
    the values, each a single-value branch carrying its own human title.

    Those branches are what a form turns into a real dropdown of the actual
    values, and they carry the machine-readable value list that the sibling
    `enum` used to - so pass `choices` DERIVED from the registry, never retyped,
    to keep the drift guard the enum gave us."""

    def _shape(schema: dict) -> None:
        _set_branches(
            schema,
            [{"type": "null", "title": omit_title}]
            + [
                {"type": "string", "enum": [value], "title": title}
                for value, title in choices
            ],
        )

    return _shape
