"""Unit tests for pipeline/markets_medialib - the `market_image` column.

Every case here is a refusal (or an acceptance) written in Conduit Market's own deployed
source, at the commit markets_medialib.py pins. The expensive mistakes this file exists to
catch are the two directions of being wrong about one marketplace:

  * TOO PERMISSIVE costs a reader a dead link. nostrology-web drops a listing no marketplace
    can show rather than drawing a name that opens "Listing not available", so a `True` here
    that Conduit would answer `False` to defeats that whole mechanism.
  * TOO STRICT costs the rotation. Every row this declines is a row that can only go to the
    marketplaces that have no conditions, which is how all of the traffic ended up at one.
"""
import json

import pytest

from dagster_code.pipeline import markets_medialib as M

GOOD = "https://cdn.example-shop.com/a.png"


def _img(*urls):
    return [["image", u] for u in urls]


# --- normalizePublicMediaUrl: the URL itself -------------------------------------------------

@pytest.mark.parametrize("url", [
    GOOD,
    "http://cdn.example-shop.com/a.png",                      # http is allowed for media
    "https://cdn.example-shop.com/a.png#x",                    # allowFragment: true
    "https://cdn.example-shop.com./a.png",                     # one trailing dot is dropped
    "https://8.8.8.8/a.png",                                   # a public IPv4 literal
    "https://[2606:4700:4700::1111]/a.png",                    # inside 2000::/3
    "https://xn--caf-dma.example-shop.com/a.png",              # an IDN in its punycode form
    GOOD + "\x1c",                                             # not JS whitespace: WHATWG strips it
    GOOD + "\u0085",                                           # nor is NEL
    "https://" + "a" * 63 + ".example-shop.com/a.png",         # a label of exactly 63
])
def test_accepts_what_conduit_would_load(url):
    assert M.normalize_public_media_url(url) is True


@pytest.mark.parametrize("url,why", [
    (None, "not a string"),
    (7, "not a string"),
    ("", "empty"),
    (" " + GOOD, "not equal to its own trim()"),
    (GOOD + " ", "not equal to its own trim()"),
    ("\ufeff" + GOOD, "a LEADING U+FEFF - refused by the scheme check either way"),
    (GOOD + "\ufeff", "a TRAILING U+FEFF: JS trim() strips it, Python's str.strip() does not"),
    ("\u00a0" + GOOD, "JS trim() strips NBSP"),
    (GOOD + "\u00a0", "and strips it at the end too"),
    ("https://cdn.example-shop.com/" + "a" * 4096, "over 4096 characters"),
    ("ftp://cdn.example-shop.com/a.png", "protocol is not http(s)"),
    ("data:image/png;base64,iVBOR", "protocol is not http(s)"),
    ("//cdn.example-shop.com/a.png", "no protocol at all"),
    ("https://user:pw@cdn.example-shop.com/a.png", "carries credentials"),
    ("https://user@cdn.example-shop.com/a.png", "carries a username"),
    ("https://127.0.0.1/a.png", "loopback"),
    ("https://10.1.2.3/a.png", "private"),
    ("https://192.168.1.1/a.png", "private"),
    ("https://172.16.0.1/a.png", "private"),
    ("https://169.254.1.1/a.png", "link-local"),
    ("https://100.64.0.1/a.png", "CGNAT"),
    ("https://203.0.113.4/a.png", "TEST-NET-3"),
    ("https://224.0.0.1/a.png", "multicast"),
    ("https://0.0.0.0/a.png", "this host"),
    ("https://shop/a.png", "a single label is not a public name"),
    ("https://shop.local/a.png", "special-use suffix"),
    ("https://shop.internal/a.png", "special-use suffix"),
    ("https://abcdefg.onion/a.png", "special-use suffix"),
    ("https://example.com/a.png", "example.com is itself a special-use suffix"),
    ("https://cdn_1.example-shop.com/a.png", "an underscore is not a label character"),
    ("https://-cdn.example-shop.com/a.png", "a label cannot start with a hyphen"),
    ("https://" + "a" * 64 + ".example-shop.com/a.png", "a label over 63"),
    ("https://" + ("a" * 60 + ".") * 4 + "example-shop.com/a.png", "a host over 253"),
    ("https://[::1]/a.png", "loopback, and outside 2000::/3"),
    ("https://[2001:db8::1]/a.png", "the IPv6 documentation prefix"),
    ("https://[2002::1]/a.png", "6to4"),
    ("https://[::ffff:8.8.8.8]/a.png", "their IPv6 parser takes 1-4 hex digits per group"),
    ("https://2130706433/a.png", "loopback as a single decimal, which WHATWG canonicalises"),
    ("https://0x7f.0.0.1/a.png", "loopback in hex, which WHATWG canonicalises"),
    # --- the three strictnesses review added on 2026-10-24, each a `new URL` THROW over there
    ("https://caf\u00e9.example-shop.com/a.png", "a non-ASCII host: no UTS#46 here, so it is not classified at all"),
    ("https://cdn\u200cshop.net/a.png", "a zero-width non-joiner: UTS#46 refuses it, IDNA-2003 would not"),
    ("https://cdn\u200dshop.net/a.png", "a zero-width joiner, likewise"),
    ("https://\u0301cdnshop.net/a.png", "a leading combining mark: UTS#46 refuses it"),
    ("https://cdn\u0660shop.net/a.png", "an Arabic-Indic digit: UTS#46 refuses it"),
    ("https://xn--a.example-shop.com/a.png", "claims to be punycode and does not decode"),
    ("https://xn--.example-shop.com/a.png", "an empty punycode label"),
    ("https://images.myshop.1/a.jpg", "endsInANumber: WHATWG sends it to the IPv4 parser, which fails"),
    ("https://cdn.example.0x1f/a.jpg", "endsInANumber, in hex"),
    ("https://cdn.shop-xyz.io/" + "\U0001f600" * 2037, "4,098 UTF-16 units, over the 4,096 bound"),
])
def test_refuses_what_conduit_would_refuse(url, why):
    assert M.normalize_public_media_url(url) is False, why


def test_a_host_over_253_is_refused_even_when_every_label_is_legal():
    host = ".".join(["a" * 61] * 4) + ".example-shop.com"
    assert len(host) > 253
    assert M.is_public_network_hostname(host) is False


@pytest.mark.parametrize("host,public,why", [
    ("2606:4700:4700::1111", True, "globally routable, inside 2000::/3"),
    ("2001:4860:4860::8888", True, "globally routable"),
    ("2a00:1450:4001:80f::200e", True, "globally routable"),
    ("2606:4700:4700:0000:0000:0000:0000:1111", True, "written out in full"),
    ("2001:1::1", True, "PCP anycast: their policy admits it inside 2001::/23"),
    ("2001:3::1", True, "the AMT prefix: admitted"),
    ("2001:5::1", False, "an unallocated gap in 2001::/23: default-deny"),
    ("::1", False, "loopback, and outside 2000::/3"),
    ("fe80::1", False, "link-local"),
    ("fc00::1", False, "a unique local address"),
    ("2001:db8::1", False, "the documentation prefix"),
    ("3fff::1", False, "the second documentation prefix"),
    ("2002::1", False, "6to4"),
    ("2001:10::1", False, "ORCHID"),
    ("2001:20::1", False, "ORCHIDv2"),
    ("2001:0:1234::1", False, "Teredo"),
    ("2001:2::1", False, "the benchmarking prefix"),
    ("::ffff:8.8.8.8", False, "their parser takes 1-4 hex digits per group, so this is no address to them"),
    ("2606:4700:4700::1111:", False, "a trailing colon"),
    ("2606::4700::1111", False, "two :: runs"),
    ("2606:4700:4700::g111", False, "a non-hex group"),
])
def test_the_ipv6_policy_is_theirs_prefix_for_prefix(host, public, why):
    """isPublicIpv6Address, the longest single rule in their file: 2000::/3 only, minus the
    documentation, ORCHID, Teredo, 6to4 and benchmarking ranges, and minus the unallocated gaps
    of 2001::/23 with its handful of admitted suballocations kept. Ported rather than handed to
    Python's ipaddress module, which parses forms their own parser refuses."""
    assert M.is_public_network_hostname(host) is public, why
    assert M.normalize_public_media_url("https://[" + host + "]/a.png") is public, why


def test_the_stricter_numeric_branch_only_ever_declines():
    """An all-numeric host is an address in some notation, never a name. We refuse the lot,
    including the public ones WHATWG would canonicalise and Conduit would then accept - the
    direction that costs a link rather than offers a dead one."""
    assert M.is_public_network_hostname("0x08.08.08.08") is False   # = 8.8.8.8 to WHATWG
    assert M.is_public_network_hostname("8.8.8.8") is True          # the canonical form works


# --- which images Conduit would even look at -------------------------------------------------

def test_content_that_is_not_json_sends_it_to_the_image_tags():
    where, urls = M.image_source("a markdown listing body", _img(GOOD))
    assert (where, urls) == ("tags", [GOOD])


def test_empty_content_parses_to_an_empty_object_and_still_falls_back():
    """JSON.parse(event.content || "{}") - so an empty content parses, and `{}` then fails
    the schema for want of a title, which is what sends it to the tags."""
    for content in ("", None):
        assert M.image_source(content, _img(GOOD)) == ("tags", [GOOD])


def test_json_content_with_a_title_and_a_json_price_reads_content_images():
    content = json.dumps({"title": "Tea", "price": 5, "images": [{"url": GOOD}]})
    assert M.image_source(content, _img("https://other.example-shop.com/b.png")) == (
        "content", [GOOD])


def test_the_price_may_come_from_the_tag():
    content = json.dumps({"title": "Tea", "images": [{"url": GOOD}]})
    assert M.image_source(content, [["price", "5", "USD"]])[0] == "content"
    assert M.image_source(content, [])[0] == "tags"


@pytest.mark.parametrize("title", ["", "x" * 201, 7, None, ["Tea"]])
def test_a_title_the_schema_refuses_sends_it_to_the_tags(title):
    content = json.dumps({"title": title, "price": 5, "images": [{"url": GOOD}]})
    assert M.image_source(content, _img(GOOD))[0] == "tags"


def test_a_title_of_200_emoji_is_400_utf16_units_and_fails_the_schema():
    """Zod counts String.length, i.e. UTF-16 code units. Measured with Python's len() a
    200-emoji title passes; measured as Conduit measures it, it does not."""
    emoji = "\U0001f600" * 200
    assert len(emoji) == 200 and M.title_tag_len(emoji) == 400
    content = json.dumps({"title": emoji, "price": 5, "images": [{"url": GOOD}]})
    assert M.image_source(content, _img(GOOD))[0] == "tags"
    assert M.image_source(json.dumps({"title": "\U0001f600" * 100, "price": 5,
                                      "images": [{"url": GOOD}]}), [])[0] == "content"


@pytest.mark.parametrize("content", ["5", '"x"', "[1,2]", "null", "true"])
def test_json_that_is_not_an_object_behaves_as_an_empty_one(content):
    assert M.image_source(content, _img(GOOD)) == ("tags", [GOOD])


def test_a_negative_or_unparseable_price_is_no_price():
    for price in (-1, "5", None, True):
        content = json.dumps({"title": "Tea", "price": price, "images": [{"url": GOOD}]})
        assert M.image_source(content, [])[0] == "tags", price
    assert M.image_source(json.dumps({"title": "Tea", "price": 0,
                                      "images": [{"url": GOOD}]}), [])[0] == "content"


def test_content_images_shape_is_objects_with_a_string_url():
    content = json.dumps({"title": "Tea", "price": 5,
                          "images": [GOOD, {"alt": "no url"}, {"url": 7}, None,
                                     {"url": GOOD, "alt": "ok"}]})
    assert M.image_source(content, []) == ("content", [GOOD])


# --- the column -----------------------------------------------------------------------------

def test_the_false_positive_the_review_found():
    """A listing with JSON content Conduit's schema accepts, a price tag and an `image` TAG,
    but nothing in content.images: Conduit's product.images is EMPTY, availability pushes
    missing_market_image, and the reader gets "Listing not available". An image-tag-only rule
    would have linked it."""
    content = json.dumps({"title": "Tea"})
    tags = [["price", "5", "USD"]] + _img(GOOD)
    assert M.market_image(content, tags) is False


def test_the_plain_cases():
    assert M.market_image("markdown body", _img(GOOD)) is True
    assert M.market_image("markdown body", _img("https://127.0.0.1/a.png")) is False
    assert M.market_image("markdown body", []) is False
    assert M.market_image(json.dumps({"title": "Tea", "price": 5,
                                      "images": [{"url": GOOD}]}), []) is True


def test_one_accepted_url_among_rejected_ones_is_enough():
    assert M.market_image("body", _img("https://10.0.0.1/a.png", "ftp://x/a.png", GOOD)) is True


def test_a_one_element_image_tag_is_not_a_url():
    assert M.market_image("body", [["image"], ["image", GOOD]]) is True
    assert M.market_image("body", [["image"]]) is False


# --- the summary bound ----------------------------------------------------------------------

def test_summary_tag_len_counts_utf16_units_of_the_first_tag():
    assert M.summary_tag_len([]) is None
    assert M.summary_tag_len([["summary"]]) is None
    assert M.summary_tag_len([["title", "x"]]) is None
    assert M.summary_tag_len([["summary", "abc"]]) == 3
    assert M.summary_tag_len([["summary", "\U0001f600"]]) == 2
    assert M.summary_tag_len([["summary", "first"], ["summary", "second"]]) == 5


# --- the bounds measured in UTF-16, and the one that would stop a refresh -------------------

def test_a_url_is_bounded_in_utf16_units_like_the_other_two():
    """`raw.length > 4096` over there. In code points 2,037 emoji after a short origin is 2,061
    and passes; as Conduit measures it, 4,098 and refused."""
    long_url = "https://cdn.shop-xyz.io/" + "\U0001f600" * 2037
    assert len(long_url) == 2061 and M._utf16_len(long_url) == 4098
    assert M.normalize_public_media_url(long_url) is False
    assert M.normalize_public_media_url("https://cdn.shop-xyz.io/" + "\U0001f600" * 2036) is True


def test_a_lone_surrogate_does_not_stop_the_refresh():
    """Python's json turns a `\\ud800` escape into a lone surrogate and a plain utf-16 encode
    RAISES on one, which would abort build_records and so the whole marketplaces_refresh from a
    single seller's event. JavaScript's JSON.parse keeps it and Zod sees length 1."""
    lone = json.loads('{"t": "\\ud800"}')["t"]
    assert M._utf16_len(lone) == 1
    assert M.title_tag_len(lone) == 1
    assert M.summary_tag_len([["summary", lone]]) == 1
    # and through the whole path: a one-surrogate title is a valid 1-character title there
    content = json.dumps({"title": lone, "price": 1, "images": [{"url": GOOD}]})
    assert M.image_source(content, [])[0] == "content"
    assert M.market_image(content, []) is True


def test_the_json_constants_javascript_refuses():
    """JSON.parse throws on NaN, Infinity and -Infinity; Python's json accepts all three, so
    without parse_constant such a content took the content path here and the tag path there."""
    images = '"images": [{"url": "' + GOOD + '"}]'
    for constant in ("NaN", "Infinity", "-Infinity"):
        content = '{"title": "T", "price": ' + constant + ", " + images + "}"
        assert M.image_source(content, _img(GOOD)) == ("tags", [GOOD]), constant


# --- the price tag, which decides WHICH image source is read -------------------------------

@pytest.mark.parametrize("tags,where,why", [
    ([["price", "5", "USD"]], "content", "one tag, a plain amount, a currency"),
    ([["price", " 5 ", "USD"]], "content", "their parser trims the amount"),
    ([["price", "5", " USD "]], "content", "and the currency"),
    ([["price", "0", "USD"]], "content", "zero is a price"),
    ([["price", "5.50", "USD"]], "content", "a decimal"),
    ([["price", "5"]], "tags", "no currency: there is no standard price at all"),
    ([["price", "5", ""]], "tags", "an empty currency is no currency"),
    ([["price", "5", "USD"], ["price", "6", "USD"]], "tags", "exactly one price tag or none"),
    ([["price", "1e3", "USD"]], "tags", "no exponent in their pattern"),
    ([["price", ".5", "USD"]], "tags", "no leading dot"),
    ([["price", "05", "USD"]], "tags", "no leading zero"),
    ([["price", "-0", "USD"]], "tags", "no sign"),
    ([["price", "-5", "USD"]], "tags", "and certainly not a negative"),
    ([["price"]], "tags", "a one-element tag has no amount"),
    ([], "tags", "no price tag and no price in the JSON"),
])
def test_the_price_tag_rule_is_theirs(tags, where, why):
    """parseSignedProductPriceTag, which products.ts imports as parsePriceTag and whose result
    OVERWRITES the candidate's price - so it decides whether the legacy JSON path wins, and so
    which images Conduit reads. A looser reading called five of these valid (review, 2026-10-24),
    and each one sends the image source the wrong way."""
    content = json.dumps({"title": "Tea"})
    assert M.image_source(content, tags + _img(GOOD))[0] == where, why


def test_the_json_price_stands_only_when_no_tag_does():
    """Their order: applyCanonicalPriceFields overwrites the candidate's price whenever the tag
    parses, so the JSON's own price only has to stand up when there is no standard price."""
    content = json.dumps({"title": "Tea", "price": -5, "images": [{"url": GOOD}]})
    # a valid tag overwrites the JSON's bad price, so the candidate is complete
    assert M.image_source(content, [["price", "5", "USD"]])[0] == "content"
    # with no valid tag, the JSON's own -5 fails z.number().min(0)
    assert M.image_source(content, [["price", "5"]])[0] == "tags"
