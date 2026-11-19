"""Unit tests for loader/nutzap_mints.py: mint URL normalization, the private-host rule, the
reviewed registry and classification precedence (nostrology-data#9). Pure: no database, no
network. The real-Postgres half (the classification table and the MAU rebuild that reads it)
is in test_nutzap_load_pg.py.

Run: .venv-mcp/bin/python -m pytest -q loader/test_nutzap_mints.py (the tests that read the
shipped YAML registry skip without PyYAML; the Dagster image has it)
"""

import copy
import re
import datetime as dt
import os
import sys

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

import nutzap_mints as M  # noqa: E402

_REGISTRY_PATH = os.path.join(os.path.dirname(_HERE), "data", "nutzaps", "mint_registry.yaml")


# -- normalize_mint_url --

@pytest.mark.parametrize("raw, stored", [
    # Unchanged: already the stored spelling (the common case, every live mint).
    ("https://mint.minibits.cash/Bitcoin", "https://mint.minibits.cash/Bitcoin"),
    ("http://127.0.0.1:3338", "http://127.0.0.1:3338"),
    ("https://8333.space:3338", "https://8333.space:3338"),
    ("https://stpaulinternet.net/cashu/api/v1/TTcmiEDSWmkefef3XqERKv",
     "https://stpaulinternet.net/cashu/api/v1/TTcmiEDSWmkefef3XqERKv"),
    # Folded: the spellings found in the archive (2026-09-18) that change.
    ("https://Mint.lnserver.com", "https://mint.lnserver.com"),
    ("HTTPS://mint.minibits.cash/Bitcoin", "https://mint.minibits.cash/Bitcoin"),
    # Whitespace and trailing slashes.
    ("  https://mint.coinos.io/ ", "https://mint.coinos.io"),
    ("https://mint.coinos.io//", "https://mint.coinos.io"),
    # Default ports go, others stay.
    ("https://mint.coinos.io:443", "https://mint.coinos.io"),
    ("https://mint.coinos.io:443/", "https://mint.coinos.io"),
    ("http://mint.example.org:80/x", "http://mint.example.org/x"),
    ("http://mint.example.org:443", "http://mint.example.org:443"),
    ("https://mint.example.org:80", "https://mint.example.org:80"),
    ("https://MINT.Example.org:3338/Path", "https://mint.example.org:3338/Path"),
    # IPv6 keeps its brackets; the host is lowercased.
    ("http://[::1]:3338", "http://[::1]:3338"),
    ("http://[FE80::1]", "http://[fe80::1]"),
    # Query and fragment are kept as written.
    ("https://mint.example.org/a?B=1#C", "https://mint.example.org/a?B=1#C"),
    # .onion is an ordinary host here.
    ("http://lbutlh5lfggq5r7xpiwhrajdl7sxpupgagazxl65w4c5cg72wtofasad.onion:3338",
     "http://lbutlh5lfggq5r7xpiwhrajdl7sxpupgagazxl65w4c5cg72wtofasad.onion:3338"),
])
def test_normalize_mint_url(raw, stored):
    assert M.normalize_mint_url(raw) == stored


def test_path_case_is_kept_because_the_server_treats_it_as_a_different_mint():
    """https://mint.minibits.cash/Bitcoin answers /v1/info; /bitcoin returns 404
    (checked 2026-09-18). Folding them would credit Minibits with 23 users whose config
    points at a dead URL."""
    assert M.normalize_mint_url("https://mint.minibits.cash/bitcoin") == "https://mint.minibits.cash/bitcoin"
    assert (M.normalize_mint_url("https://mint.minibits.cash/Bitcoin")
            != M.normalize_mint_url("https://mint.minibits.cash/bitcoin"))


@pytest.mark.parametrize("raw", [
    None, "", "   ", 42, ["https://x.example"],
    "mint.coinos.io",                 # no scheme
    "ftp://mint.example.org",         # not http(s)
    "file:///etc/passwd",
    "https:/(mint.macadamia.cash",    # archived typo
    "https:\\\\lekmint.cash\\api",    # archived typo
    "https://",                       # no host
    "https://:3338",
    "https://mint.example.org:99999",  # port out of range
    "https://mint.example.org:abc",    # port not a number
    "https://mint.example\x00.org",    # NUL
    "https://mint.example.org" + chr(0xD83C),  # lone surrogate
    "https://mint.exa\tmple.org",      # urlsplit would silently delete the tab
    "https://mint.example.org/a\nb",
    "https://[::1",                    # unclosed bracket
    # Review round (2026-09-18): each would store a different host or path than written.
    "https://testnut.cashu.space\\@evil.com",  # Python: host evil.com; browsers: testnut
    "https://127.0.0.1\\@mint.example.org",
    "https://[v1.fe]",                 # IPvFuture brackets dropped -> a DNS name
    "https://[::1]x",                  # text after ] dropped -> merges into [::1]
    "https://mint.example.org/../../wip",  # a browser resolves the link to /wip
    "https://mint.example.org/a/./b",
    "https://mint.example.org/a/%2E%2E/b",
    "https://mint.example.org/..",
    "https://example.com\\..\\",          # the one archived URL this round drops (junk)
])
def test_unusable_urls_are_none(raw):
    assert M.normalize_mint_url(raw) is None


def test_the_byte_cap_holds_after_normalization():
    base = "https://mint.example.org/"
    assert M.normalize_mint_url(base + "a" * (M.MAX_URL_BYTES - len(base))) is not None
    assert M.normalize_mint_url(base + "a" * (M.MAX_URL_BYTES - len(base) + 1)) is None


@pytest.mark.parametrize("raw", [
    "https://Mint.lnserver.com:443/", "https://mint.minibits.cash/Bitcoin", "http://[::1]:3338",
    "https://mint.example.org/a?B=1#C", "http://user@Mint.Example.org:8080/p",
])
def test_normalization_is_idempotent(raw):
    once = M.normalize_mint_url(raw)
    assert once is not None and M.normalize_mint_url(once) == once


def test_userinfo_is_kept_and_only_the_host_is_folded():
    assert M.normalize_mint_url("http://User@Mint.Example.org:8080/p") == "http://User@mint.example.org:8080/p"


# -- private_host_reason --

@pytest.mark.parametrize("host", [
    # Loopback, v4 and v6, and every spelling a browser also reads as loopback.
    "127.0.0.1", "127.255.255.254", "::1", "::ffff:127.0.0.1", "localhost", "LOCALHOST",
    "localhost.", "foo.localhost", "127.1", "0x7f.1", "2130706433",
    # RFC 1918 at both edges of each range, incl. Docker's bridge.
    "10.0.0.0", "10.255.255.255", "172.16.0.0", "172.17.0.1", "172.31.255.255",
    "192.168.0.1", "192.168.255.255",
    # Carrier-grade NAT (Tailscale), link-local (cloud metadata), unspecified.
    "100.64.0.1", "100.127.255.255", "169.254.169.254", "0.0.0.0", "::",
    # IPv6 private ranges, and a zone id.
    "fc00::1", "fd12:3456::1", "fe80::1", "fe80::1%eth0",
    # IPv6 spellings of a private IPv4 address: compatible, NAT64, 6to4.
    "::127.0.0.1", "64:ff9b::7f00:1", "2002:c0a8:101::", "64:ff9b::a9fe:a9fe",
    # Documentation, benchmarking, multicast, reserved, broadcast.
    "192.0.2.1", "198.51.100.1", "203.0.113.1", "198.18.0.1", "224.0.0.1",
    "240.0.0.1", "255.255.255.255", "2001:db8::1", "ff02::1",
    # Names that never resolve publicly.
    "umbrel", "mint", "umbrel.local", "host.docker.internal", "nas.lan", "router.home",
    "box.home.arpa", "mint.corp", "mint.test", "devteam.example", "x.invalid",
])
def test_private_hosts(host):
    assert M.private_host_reason(host) is not None


@pytest.mark.parametrize("host", [
    # Just outside each private range.
    "9.255.255.255", "11.0.0.0", "172.15.255.255", "172.32.0.0", "192.167.255.255",
    "192.169.0.0", "100.63.255.255", "100.128.0.0", "169.253.255.255", "169.255.0.0",
    "126.255.255.255", "128.0.0.0", "8.8.8.8", "2606:4700::1111", "::ffff:8.8.8.8",
    "64:ff9b::808:808", "2002:808:808::",
    # Public names, including ones that merely contain a private-looking word.
    "mint.minibits.cash", "8333.space", "testnut.cashu.space", "localhost.example.com.au",
    "mint.local-bitcoiners.org", "internal.mint.io", "lan.party.cash",
    "lbutlh5lfggq5r7xpiwhrajdl7sxpupgagazxl65w4c5cg72wtofasad.onion",
])
def test_public_hosts(host):
    assert M.private_host_reason(host) is None


def test_the_rule_uses_its_own_ranges_not_ipaddress_is_private():
    """ipaddress's is_private/is_global ranges changed across 3.11 / 3.12.4 / 3.13, and the
    image (3.12) and a workstation (3.11) must classify alike. 100.64/10 is the telling one:
    is_private is False for it on every version, yet a Tailscale mint is private."""
    import ipaddress
    assert not ipaddress.ip_address("100.64.0.1").is_private
    assert M.private_host_reason("100.64.0.1") is not None


# -- the registry --

def _entry(**over):
    e = {"host": "testnut.example.org", "class": "fake-backend",
         "evidence": "its info page says FakeWallet", "as_of": dt.date(2026, 9, 18),
         "reviewer": "agent"}
    e.update(over)
    return {k: v for k, v in e.items() if v is not None}


def _reg(*entries):
    return M.parse_registry({"mints": list(entries)})


def test_a_valid_registry_parses():
    reg = _reg(_entry(), _entry(host=None, url="https://8333.space:3338", **{"class": "test-wording-live"}),
               _entry(host="x.example.org", as_of="2026-09-01"))
    assert reg.by_host["testnut.example.org"].mint_class == "fake-backend"
    assert reg.by_url["https://8333.space:3338"].mint_class == "test-wording-live"
    assert reg.by_host["x.example.org"].source == "registry"


@pytest.mark.parametrize("doc, match", [
    (None, "top-level 'mints' list"),
    ({"mint": []}, "top-level 'mints' list"),
    ({"mints": {}}, "top-level 'mints' list"),
    ({"mints": ["https://x.example.org"]}, "not a mapping"),
    ({"mints": [_entry(**{"class": "fake"})]}, "class 'fake'"),
    ({"mints": [_entry(**{"class": "private-host"})]}, "class 'private-host'"),  # automatic only
    ({"mints": [_entry(**{"class": "unreviewed"})]}, "class 'unreviewed'"),      # the default
    ({"mints": [_entry(evidence=None)]}, "missing 'evidence'"),
    ({"mints": [_entry(evidence="  ")]}, "missing 'evidence'"),
    ({"mints": [_entry(as_of=None)]}, "'as_of'"),
    ({"mints": [_entry(as_of="last week")]}, "'as_of'"),
    ({"mints": [_entry(reviewer=None)]}, "missing 'reviewer'"),
    ({"mints": [_entry(url="https://x.example.org")]}, "exactly one of"),
    ({"mints": [_entry(host=None)]}, "exactly one of"),
    ({"mints": [_entry(host=None, url="https://X.example.org/")]}, "not normalized"),
    ({"mints": [_entry(host=None, url="mint.example.org")]}, "not normalized"),
    ({"mints": [_entry(host="Testnut.example.org")]}, "bare lowercase host"),
    ({"mints": [_entry(host="testnut.example.org:3338")]}, "bare lowercase host"),
    ({"mints": [_entry(host="https://testnut.example.org")]}, "bare lowercase host"),
    ({"mints": [_entry(host="testnut.example.org.")]}, "bare lowercase host"),  # would never match
    ({"mints": [_entry(), _entry()]}, "listed twice"),
    ({"mints": [_entry(notez="typo")]}, "unknown keys"),
])
def test_a_malformed_registry_is_refused(doc, match):
    with pytest.raises(ValueError, match=match):
        M.parse_registry(doc)


# -- classify: precedence --

def test_precedence_host_rule_then_url_then_host_then_unreviewed():
    reg = _reg(
        _entry(host="mint.example.org", **{"class": "unreachable"}),
        _entry(host=None, url="https://mint.example.org/special", **{"class": "test-wording-live"}),
        # A registry row cannot rescue a private host: the rule wins.
        _entry(host="umbrel.local", **{"class": "unreachable"}),
    )
    assert M.classify("https://mint.example.org/special", reg).mint_class == "test-wording-live"
    assert M.classify("https://mint.example.org", reg).mint_class == "unreachable"
    assert M.classify("http://mint.example.org:3338/other", reg).mint_class == "unreachable"
    assert M.classify("https://umbrel.local", reg) == M.MintClass("private-host", "host-rule", "non-public name")
    assert M.classify("https://other.example.org", reg) == M.MintClass("unreviewed", "none", None)


def test_host_rows_do_not_match_subdomains_or_lookalikes():
    reg = _reg(_entry(host="testnut.cashu.space"))
    assert M.classify("https://testnut.cashu.space", reg).excluded
    assert not M.classify("https://nofee.testnut.cashu.space", reg).excluded
    assert not M.classify("https://testnut.cashu.space.evil.example.com", reg).excluded


def test_excluded_is_exactly_the_three_classes():
    assert M.EXCLUDED_CLASSES == {"private-host", "fake-backend", "test-mint-gone"}
    assert M.MintClass("unreviewed", "none", None).excluded is False


# -- the shipped registry --

@pytest.fixture(scope="module")
def shipped():
    # PyYAML is in the Dagster image (the lightning signer manifest needs it too) but not in
    # a workstation's .venv-mcp; run these in a venv that has it, or inside the image.
    pytest.importorskip("yaml")
    return M.load_registry(_REGISTRY_PATH)


@pytest.mark.parametrize("url, cls", [
    # The mints #9 names, as the archive spells them.
    ("http://127.0.0.1:3338", "private-host"),
    ("http://127.0.0.1:32787", "private-host"),
    ("http://172.17.0.1:32769", "private-host"),
    ("http://169.254.169.254", "private-host"),
    ("http://[::1]", "private-host"),
    ("http://localhost:3338", "private-host"),
    ("https://testnut.cashu.space", "fake-backend"),
    ("https://nofee.testnut.cashu.space", "fake-backend"),
    ("https://nofees.testnut.cashu.space", "fake-backend"),
    ("https://testmint.macadamia.cash", "test-mint-gone"),
    ("https://mint.routstr.com", "test-wording-live"),
    ("https://mint.lnpay.cz", "test-wording-live"),
    ("https://8333.space:3338", "test-wording-live"),
    ("https://mint.bitcointxoko.com", "unreachable"),
    # Real mints stay unreviewed and counted, incl. the one with "test" in its own text.
    ("https://mint.minibits.cash/Bitcoin", "unreviewed"),
    ("https://mint.coinos.io", "unreviewed"),
    ("https://mint.macadamia.cash", "unreviewed"),
    # Named "testnut" but unreviewed (no evidence of a fake backend): counted.
    ("https://testnut.era21.space", "unreviewed"),
    ("http://lbutlh5lfggq5r7xpiwhrajdl7sxpupgagazxl65w4c5cg72wtofasad.onion:3338", "unreviewed"),
])
def test_the_shipped_registry_classifies_the_known_mints(shipped, url, cls):
    assert M.classify(url, shipped).mint_class == cls


def test_the_shipped_registry_only_excludes_with_evidence(shipped):
    for mc in list(shipped.by_url.values()) + list(shipped.by_host.values()):
        assert mc.evidence and len(mc.evidence) > 20


def test_mutating_a_parsed_doc_does_not_change_the_registry():
    doc = {"mints": [_entry()]}
    reg = M.parse_registry(copy.deepcopy(doc))
    doc["mints"][0]["class"] = "unreachable"
    assert reg.by_host["testnut.example.org"].mint_class == "fake-backend"


def test_every_wire_fixture_mint_classifies_as_recorded(shipped):
    """The real archived events in fixtures/nutzap_wire_families.json record their mint's
    class; the PG tier asserts the same classes land in nutzap_mint_classes."""
    import json
    with open(os.path.join(_HERE, "fixtures", "nutzap_wire_families.json"), encoding="utf-8") as fh:
        fixtures = json.load(fh)["fixtures"]
    classes = set()
    for f in fixtures:
        urls = [M.normalize_mint_url(t[1]) for t in f["event"]["tags"] if len(t) >= 2 and t[0] == "u"]
        url = next((u for u in urls if u), None)
        got = M.classify(url, shipped).mint_class if url else None
        assert got == f["expected"]["mint_class"], f["family"]
        classes.add(got)
    assert {"private-host", "fake-backend", "test-mint-gone", "unreviewed"} <= classes


def test_the_sql_check_constraints_list_the_same_classes_as_python():
    """nutzap_mint_classes is CREATE TABLE IF NOT EXISTS, so its CHECKs never change on
    their own: a class added in Python only would fail every nightly INSERT (and with it
    the MAU rebuild). This pins the two lists together."""
    sql = open(os.path.join(os.path.dirname(_HERE), "schema", "init.sql"), encoding="utf-8").read()
    table = sql[sql.index("CREATE TABLE IF NOT EXISTS nutzap_mint_classes"):]
    table = table[:table.index(");")]
    quoted = lambda text: set(re.findall(r"'([a-z-]+)'", text))  # noqa: E731
    all_classes = table[table.index("mint_class IN ("):table.index("excluded      BOOLEAN")]
    excluded = table[table.index("excluded = (mint_class IN ("):]
    assert quoted(all_classes) == set(M.CLASSES)
    assert quoted(excluded) == set(M.EXCLUDED_CLASSES)
