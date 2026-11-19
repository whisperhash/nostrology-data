"""Does a listing carry an image Conduit Market will load? - the `market_image` column.

WHY THIS LIVES HERE AT ALL, since it is one marketplace's rule and not a fact about the
event. nostrology-web's Recent listings section sends each row to a marketplace that can
actually render it, and Conduit hides a listing whose images its own
`normalizePublicMediaUrl` rejects ("Listing not available - Current state: Hidden"). The
decision needs the event's CONTENT, which the serving table does not carry and the web never
sees, so the question cannot be answered on the web side at all - and a rule split across two
repositories is worse than a whole one in the second-best place. The web keeps every other
marketplace rule ($lib/markets/links.ts) and reads this column as one fact
(`market_image?: boolean | null`, where null means "the row does not carry it").
nostrology-web#25 (Re-read each marketplace client's listing-visibility rules periodically,
at the versions they are serving) is the re-read that covers this file drifting from theirs.

TRANSCRIBED FROM THE DEPLOYED SOURCE, not from observed behaviour:
Conduit-BTC/conduit-mono @ 3a4dbe3b5d56754ed96a9b797535428e8a82fe59 (the tip of main,
committed 2026-10-22 16:31Z; the deployed bundle names that commit in the
bug-report link it builds on every page).
  * packages/core/src/network-target-safety.ts  - normalizePublicMediaUrl, isPublicNetworkHostname
  * packages/core/src/protocol/products.ts      - parseProductFields, getProductProtocolImages
  * packages/core/src/schemas/index.ts          - productSchema

WHICH IMAGES ARE EVEN LOOKED AT is the half a first pass got wrong (found in review,
2026-10-23). `parseProductFields` tries the LEGACY JSON path first: it JSON-parses the
event content, builds a candidate whose images come from `content.images` AND NEVER FROM THE
`image` TAGS, and keeps that candidate if `productSchema.safeParse` accepts it. Only if the
content is not JSON, or the candidate fails that schema, does the NIP-99 fallback path read
the `image` tags. So "a listing with an image tag has an image" is a FALSE POSITIVE for any
listing whose JSON content Conduit accepts: `product.images` is empty, availability pushes
`missing_market_image`, and the reader gets "Listing not available".

MEASURED, so the residual is known rather than assumed. Over all 18,030 newest-version
kind-30402 (markets-product-listings) events in the archive from 2026-08-01 (read 2026-10-24):

    content is JSON          1,747   of which the legacy candidate is schema-complete:   90
    legacy path wins, an accepted URL in content.images AND in an image tag              21
    legacy path wins, no accepted URL in either                                          69
    content is JSON, candidate short of the schema, image tags accepted, content.images not  150

so there is NOT ONE row today where the choice of source changes the answer, in either
direction. The path is modelled anyway, because the case the review found is reachable from a
single JSON field and this measurement cannot see the absence of it.

THE RESIDUAL, named: `_legacy_candidate_complete` checks the schema's two seller-supplied
required fields - a title of 1..200 UTF-16 units from the JSON, and a price, whose tag rule is
ported exactly from `parseSignedProductPriceTag` since review on 2026-10-24. A JSON content
that satisfies both and then fails the schema on some OTHER field - a `summary` over 5,000
characters, a `visibility` outside public/private - would send Conduit to the `image` tags
while this reads `content.images`. That is the 0-of-18,030 class above. Porting the whole Zod
schema to close it would be a large amount of code for a boolean, and the error it would fix is
the safe direction in 69 of the 90 cases (no image either way).

NOTHING HERE FETCHES ANYTHING. Every rule is lexical, on the URL string, exactly as
Conduit's is: it decides whether Conduit would be willing to load the URL, never whether the
URL resolves or what it serves.
"""
import json
import re
import urllib.parse

# --- normalizePublicMediaUrl (network-target-safety.ts) ------------------------------------

MAX_URL = 4096

# JS String.prototype.trim()'s whitespace set, written out. Python's str.strip() is NOT the
# same set: it strips U+00A0 but not U+FEFF, and Conduit's `raw !== raw.trim()` test refuses
# a URL padded with a BOM. Using Python's default would accept one and claim an image
# Conduit will not load.
_JS_WS = (
    "\u0009\u000a\u000b\u000c\u000d   "
    "           "
    "    　﻿"
)

SPECIAL_USE_SUFFIXES = (
    "localhost", "local", "localdomain", "internal", "home", "lan", "arpa",
    "test", "invalid", "example", "example.com", "example.net", "example.org",
    "alt", "onion",
)
_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")
_NUMERIC_HOST = re.compile(r"^(?:0x[0-9a-f]+|[0-9.]+)$", re.I)
_NUMERIC_LABEL = re.compile(r"^(?:0x[0-9a-f]+|[0-9]+)$", re.I)
_IPV6_GROUP = re.compile(r"^[0-9a-f]{1,4}$", re.I)


def _ipv4(host: str):
    """parseIpv4Address: FOUR decimal octets and nothing else."""
    parts = host.split(".")
    if len(parts) != 4:
        return None
    value = 0
    for part in parts:
        if not re.fullmatch(r"\d{1,3}", part):
            return None
        octet = int(part)
        if octet > 255:
            return None
        value = value * 256 + octet
    return value


def _public_ipv4(value: int) -> bool:
    """isPublicIpv4Address, range for range."""
    a, b, c = (value >> 24) & 0xFF, (value >> 16) & 0xFF, (value >> 8) & 0xFF
    if a in (0, 10, 127):
        return False
    if a == 100 and 64 <= b <= 127:
        return False
    if a == 169 and b == 254:
        return False
    if a == 172 and 16 <= b <= 31:
        return False
    if a == 192 and b == 0 and c in (0, 2):
        return False
    if a == 192 and b == 88 and c == 99:
        return False
    if a == 192 and b == 168:
        return False
    if a == 192 and b == 31 and c == 196:
        return False
    if a == 192 and b == 52 and c == 193:
        return False
    if a == 198 and b in (18, 19):
        return False
    if a == 198 and b == 51 and c == 100:
        return False
    if a == 203 and b == 0 and c == 113:
        return False
    if a >= 224:
        return False
    return True


def _ipv6(host: str):
    """parseIpv6Address - THEIR parser, not Python's. Each group must be 1 to 4 hex digits,
    so an IPv4-mapped form (`::ffff:1.2.3.4`) is not an address to them at all; it then
    fails the `includes(":")` gate and the host is refused. Python's ipaddress accepts it,
    which is why this is ported rather than delegated."""
    un = host[1:-1] if host.startswith("[") and host.endswith("]") else host
    if ":" not in un:
        return None
    dbl = un.find("::")
    if dbl != -1 and dbl != un.rfind("::"):
        return None
    if dbl == -1:
        left, right = un.split(":"), []
    else:
        left_raw, right_raw = un[:dbl], un[dbl + 2:]
        left = left_raw.split(":") if left_raw else []
        right = right_raw.split(":") if right_raw else []
    missing = 8 - len(left) - len(right)
    if (dbl == -1 and missing != 0) or (dbl != -1 and missing < 1):
        return None
    groups = left + ["0"] * max(0, missing) + right
    if len(groups) != 8:
        return None
    value = 0
    for g in groups:
        if not _IPV6_GROUP.fullmatch(g):
            return None
        value = (value << 16) | int(g, 16)
    return value


def _has_prefix(value: int, prefix: int, bits: int) -> bool:
    shift = 128 - bits
    return (value >> shift) == (prefix >> shift)


_IPV6_DOCUMENTATION = 0x20010DB8000000000000000000000000
_IPV6_ORCHID = 0x20010010000000000000000000000000
_IPV6_ORCHID_V2 = 0x20010020000000000000000000000000
_IPV6_TEREDO = 0x20010000000000000000000000000000
_IPV6_6TO4 = 0x20020000000000000000000000000000
_IPV6_BENCHMARK = 0x20010002000000000000000000000000
_IPV6_DOCUMENTATION_V2 = 0x3FFF0000000000000000000000000000
_IPV6_IETF_ASSIGNMENTS = 0x20010000000000000000000000000000
_IPV6_PCP_ANYCAST = 0x20010001000000000000000000000001
_IPV6_TURN_ANYCAST = 0x20010001000000000000000000000002
_IPV6_DNS_SD_ANYCAST = 0x20010001000000000000000000000003
_IPV6_AMT = 0x20010003000000000000000000000000
_IPV6_AS112 = 0x20010004011200000000000000000000
_IPV6_DET = 0x20010030000000000000000000000000


def _public_ietf_assignment(value: int) -> bool:
    return (
        value in (_IPV6_PCP_ANYCAST, _IPV6_TURN_ANYCAST, _IPV6_DNS_SD_ANYCAST)
        or _has_prefix(value, _IPV6_AMT, 32)
        or _has_prefix(value, _IPV6_AS112, 48)
        or _has_prefix(value, _IPV6_DET, 28)
    )


def _public_ipv6(value: int) -> bool:
    """isPublicIpv6Address: 2000::/3 only, minus the documentation, ORCHID, Teredo, 6to4,
    benchmark and 2001::/23 gaps."""
    if (value >> 125) != 1:
        return False
    if _has_prefix(value, _IPV6_DOCUMENTATION, 32):
        return False
    if _has_prefix(value, _IPV6_ORCHID, 28) or _has_prefix(value, _IPV6_ORCHID_V2, 28):
        return False
    if _has_prefix(value, _IPV6_TEREDO, 32) or _has_prefix(value, _IPV6_6TO4, 16):
        return False
    if _has_prefix(value, _IPV6_BENCHMARK, 48):
        return False
    if _has_prefix(value, _IPV6_DOCUMENTATION_V2, 20):
        return False
    if _has_prefix(value, _IPV6_IETF_ASSIGNMENTS, 23) and not _public_ietf_assignment(value):
        return False
    return True


def _normalize_host(host: str) -> str:
    """normalizeHostnameForClassification: trim, lowercase, fold the three ideographic full
    stops to '.', drop one trailing dot, de-bracket."""
    h = host.strip(_JS_WS).lower()
    h = h.replace("。", ".").replace("．", ".").replace("｡", ".")
    h = re.sub(r"\.$", "", h)
    if h.startswith("[") and h.endswith("]"):
        h = h[1:-1]
    return h


def is_public_network_hostname(host: str) -> bool:
    """isPublicNetworkHostname, plus the part of the WHATWG URL PARSER that runs before it.

    Their function is handed `url.hostname`, which WHATWG has already canonicalised: an IDN
    punycoded, an IPv4 in any notation rewritten to dotted-quad, and the whole URL REJECTED for
    a host that fails either. Python's urlsplit does none of that and rejects nothing, so a
    faithful port has to do that work here - and every place it cannot be exact is written out
    below, always in the same direction. A FALSE POSITIVE (we say yes, Conduit says no) costs a
    reader a dead link, which is what the openability cut exists to prevent; a false negative
    costs that marketplace one row's traffic.

    FOUR DELIBERATE STRICTNESSES, each refusing what WHATWG would have canonicalised or thrown
    on. Three came out of review on 2026-10-24, after a differential test against the real
    TypeScript over 60,012 random Unicode hosts found 1,912 false positives:

    - A NON-ASCII HOST IS REFUSED OUTRIGHT. WHATWG applies UTS#46 and Python has no UTS#46:
      `str.encode("idna")` is IDNA-2003, which accepts hosts UTS#46 rejects - a zero-width
      joiner, a leading combining mark, an Arabic-Indic digit - and `new URL` THROWS on each of
      those, so Conduit refuses them. Punycoding here with the wrong standard was the source of
      all 1,912. What it costs: a listing whose image host is a non-ASCII IDN written as such
      loses its Conduit link. Written in its punycode form, which is how such a host normally
      reaches a URL, it still passes.
    - A LABEL THAT CLAIMS TO BE PUNYCODE MUST DECODE. `xn--a` is not valid punycode, so WHATWG's
      ToASCII fails and `new URL` throws, while a bare label regex accepts it.
    - A HOST WHOSE LAST LABEL IS A NUMBER IS REFUSED (WHATWG's endsInANumber): such a host goes
      to its IPv4 parser, and `images.myshop.1` fails there, which is a parse failure rather
      than a classification - so `new URL` throws rather than returning a name.
    - AND THE ALL-NUMERIC HOST, the first of these: `0x7f.0.0.1` or `2130706433` is an address
      in some notation, never a name. WHATWG rewrites it to dotted-quad and theirs then
      classifies it, so a loopback in hex reaches their refusal - and would have reached our
      ACCEPTANCE through the label check below. This is also the one strictness that refuses
      something PUBLIC: `0x08.08.08.08` is 8.8.8.8 to WHATWG.
    """
    h = _normalize_host(host)
    if not h or len(h) > 253:
        return False
    v4 = _ipv4(h)
    if v4 is not None:
        return _public_ipv4(v4)
    v6 = _ipv6(h)
    if v6 is not None:
        return _public_ipv6(v6)
    if ":" in h:
        return False
    # see the docstring: with no UTS#46 here, a non-ASCII host is not classified at all. It is
    # also what keeps the 253-character bound above honest, since an ASCII host is its own
    # punycode and cannot grow under one.
    if not h.isascii():
        return False
    if _NUMERIC_HOST.fullmatch(h):
        return False
    labels = h.split(".")
    # an all-numeric host, and then WHATWG's endsInANumber: a host whose LAST label is a number
    if labels and all(_NUMERIC_LABEL.fullmatch(l) for l in labels):
        return False
    if labels and _NUMERIC_LABEL.fullmatch(labels[-1]):
        return False
    if any(h == suffix or h.endswith("." + suffix) for suffix in SPECIAL_USE_SUFFIXES):
        return False
    for label in labels:
        if label.startswith("xn--"):
            try:
                label.encode("ascii").decode("idna")
            except Exception:
                return False
    if len(labels) < 2:
        return False
    return all(1 <= len(l) <= 63 and _LABEL.fullmatch(l) for l in labels)


def normalize_public_media_url(raw) -> bool:
    """normalizePublicMediaUrl(raw) !== null, i.e. Conduit is willing to load this URL.

    A FRAGMENT IS ALLOWED (`allowFragment: true` for media, which is the one option media
    URLs differ on), so refusing one would be over-strict - that was wrong in the first
    transcription of this rule and was corrected in review on 2026-10-23.
    """
    if not isinstance(raw, str):
        return False
    # `raw.length > 4096` over there, which is UTF-16 units: 2,037 emoji after a short origin
    # is 4,098 to JavaScript and 2,061 to Python's len(), and Conduit refuses it (review,
    # 2026-10-24 - this was the third bound that needed _utf16_len and did not have it).
    if not raw or raw != raw.strip(_JS_WS) or _utf16_len(raw) > MAX_URL:
        return False
    # urlsplit on a malformed IPv6 literal raises
    try:
        u = urllib.parse.urlsplit(raw)
    except ValueError:
        return False
    if u.scheme not in ("http", "https"):
        return False
    try:
        if u.username or u.password:
            return False
        host = u.hostname or ""
    except ValueError:
        return False
    return is_public_network_hostname(host)


# --- which images Conduit would read (products.ts) ----------------------------------------

TITLE_MIN, TITLE_MAX = 1, 200


def _utf16_len(s: str) -> int:
    """JavaScript's `String.length`, which is UTF-16 code units - Zod's `.min`/`.max` count
    those. Python's len() counts code points, so one emoji is 1 here and 2 there, and a
    title of 200 emoji passes in Python and fails in Zod.

    `surrogatepass` IS LOAD-BEARING, and not a tidy-up: a lone surrogate reaches this from JSON
    content (Python's json turns a `\\ud800` escape into one, while JavaScript's JSON.parse
    keeps it happily and Zod then sees length 1), and a plain encode raises UnicodeEncodeError
    there - which would abort build_records and so the whole marketplaces_refresh, from one
    seller's event, with an all-ASCII line in the extract. Found in review 2026-10-24. With
    surrogatepass the answer is 1, which is what JavaScript counts."""
    return len(s.encode("utf-16-le", "surrogatepass")) // 2


def _not_json(constant):
    """`JSON.parse` has no NaN and no infinities; Python's json does. Raising makes the parse
    fail exactly where JavaScript's would."""
    raise ValueError("JSON.parse would refuse " + str(constant))


def _protocol_images(parsed) -> list:
    """getProductProtocolImages over `content.images`: an array of {url, alt} objects, each
    kept only if `url` is a non-empty, trimmed, <=4096-character http(s) string. The
    structural filter is a subset of normalize_public_media_url's, so the caller's test
    below is the whole of it - this only has to pick the URLs out of the shape."""
    out = []
    images = parsed.get("images") if isinstance(parsed, dict) else None
    for image in images if isinstance(images, list) else []:
        if isinstance(image, dict) and isinstance(image.get("url"), str):
            out.append(image["url"])
    return out


def _tag_images(tags) -> list:
    """The `image` TAGS' values, in tag order - the NIP-99 fallback path's source."""
    return [t[1] for t in (tags or []) if t and len(t) >= 2 and t[0] == "image"]


# parseSignedProductPriceTag's own amount pattern (product-event-evidence.ts): a non-negative
# decimal, no sign, no exponent, no leading zeroes.
_NON_NEGATIVE_DECIMAL = re.compile(r"^(?:0|[1-9]\d*)(?:\.\d+)?$")


def _signed_price_tag(tags):
    """`parseSignedProductPriceTag`, which products.ts imports as `parsePriceTag` and whose
    result OVERWRITES the candidate's price. EXACTLY one `price` tag, a trimmed amount matching
    the pattern above, and a trimmed NON-EMPTY currency at tag[2] - otherwise there is no
    standard price at all. Returns the amount, or None.

    Ported in full after review on 2026-10-24: a looser reading (first tag, any float, currency
    ignored) called `["price","5"]`, `["price","1e3","USD"]`, `["price",".5","USD"]` and two
    price tags valid, and each of those sends the image source the wrong way."""
    price_tags = [t for t in (tags or []) if t and t[0] == "price"]
    if len(price_tags) != 1:
        return None
    tag = price_tags[0]
    raw = tag[1].strip() if len(tag) >= 2 and isinstance(tag[1], str) else ""
    currency = tag[2].strip() if len(tag) >= 3 and isinstance(tag[2], str) else ""
    if not currency or not _NON_NEGATIVE_DECIMAL.fullmatch(raw):
        return None
    amount = float(raw)
    return amount if amount >= 0 else None


def _legacy_candidate_complete(parsed, tags) -> bool:
    """Whether `productSchema.safeParse` would accept the legacy JSON candidate, as far as its
    two seller-supplied required fields go - `title` and `price`. The rest of the schema's
    required fields are set by the candidate itself (id, pubkey, createdAt, updatedAt) and the
    rest of its fields are optional or defaulted.

    `title` must be a string of 1..200 UTF-16 units FROM THE JSON CONTENT: the `title` TAG is
    not read on this path at all.

    `price` must be a number at or above zero. It can arrive two ways, and the ORDER matters:
    `applyCanonicalPriceFields` overwrites the candidate's price whenever the signed price tag
    parses, so a valid tag makes the price valid whatever the JSON says, and the JSON's own
    `price` only has to stand up when there is no such tag.

    WHAT IS STILL NOT MODELLED, and why it is safe: the schema's other fields. A JSON content
    that satisfies title and price and then fails on, say, a `visibility` outside
    public/private, or a `summary` over 5,000, would send Conduit to the `image` TAGS while
    this reads `content.images`. Every such case moves a row from the content source to the tag
    source, i.e. it can only make this read the WRONG source for a row where the two agree -
    measured 0 of 18,030 on 2026-10-24 (21 rows accepted in both sources, 69 in neither)."""
    if not isinstance(parsed, dict):
        return False
    title = parsed.get("title")
    if not isinstance(title, str) or not (TITLE_MIN <= _utf16_len(title) <= TITLE_MAX):
        return False
    if _signed_price_tag(tags) is not None:
        return True
    price = parsed.get("price")
    if isinstance(price, bool) or not isinstance(price, (int, float)):
        return False
    return price == price and price >= 0  # NaN is not a price


def image_source(content, tags) -> tuple:
    """Which images Conduit would look at for this event, and where they came from:
    ('content', [...]) on the legacy JSON path, ('tags', [...]) on the NIP-99 fallback.

    `JSON.parse(event.content || "{}")` - so an EMPTY content parses, to `{}`, and takes the
    legacy path as far as the schema check (which `{}` then fails for want of a title). A
    content that is valid JSON but not an object (`5`, `"x"`, `[1]`) spreads to `{}` in
    their `{...parsed}` and behaves the same way.
    """
    try:
        # parse_constant: Python's json accepts NaN, Infinity and -Infinity, and JSON.parse
        # THROWS on all three - so a content carrying one takes the tag path over there and
        # would have taken the content path here (review, 2026-10-24).
        parsed = json.loads(content or "{}", parse_constant=_not_json)
    except (ValueError, TypeError):
        return ("tags", _tag_images(tags))
    if _legacy_candidate_complete(parsed, tags):
        return ("content", _protocol_images(parsed))
    return ("tags", _tag_images(tags))


def market_image(content, tags) -> bool:
    """The `market_image` column: does this listing present an image URL that Conduit's own
    normalizePublicMediaUrl accepts, from the source Conduit would actually read?"""
    _, urls = image_source(content, tags)
    return any(normalize_public_media_url(u) for u in urls)


def summary_tag_len(tags):
    """The `summary` tag's length in UTF-16 units, or None when the listing has no summary
    tag. Conduit's schema caps `summary` at 5,000 and the NIP-99 fallback path passes the tag
    through unsliced, so a longer one THROWS at parse and the reader gets "Product not
    found" - a dead link for anyone we send there. The length is enough: no reader of this
    column wants the text, and the text would be the largest thing in the row."""
    for t in tags or []:
        if t and len(t) >= 2 and t[0] == "summary" and isinstance(t[1], str):
            return _utf16_len(t[1])
    return None


def title_tag_len(s):
    """The `title` tag's length in UTF-16 units - exported for the tests, and for anyone
    tempted to measure a title with Python's len()."""
    return None if not isinstance(s, str) else _utf16_len(s)


def tag_value_as_conduit_reads_it(tags, name):
    """Their `getTagValue`: the first tag of this name that HAS a string value, skipping one
    that does not - `[["title"], ["title", "Mug"]]` is "Mug" over there.

    NOT the same as this pipeline's own `tag_value`, which returns None for a first match with
    no value, because that one is a DuckDB-parity primitive the metrics depend on and must not
    move. The difference matters for exactly one column: `title_tag` is read to decide whether
    Conduit's 1..200 bound refuses the listing, so it has to be the string Conduit measures.
    Reading None there would skip the check and hand a reader a "Product not found" page
    (review, 2026-10-24)."""
    for t in tags or []:
        if t and t[0] == name and len(t) >= 2 and isinstance(t[1], str):
            return t[1]
    return None
