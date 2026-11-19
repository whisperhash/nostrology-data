"""Which Cashu mints count on /nutzaps and in the paid `nutzaps.active_users` metric.

Two parts, both pure (no database, no network), so they are unit-tested directly:

- `normalize_mint_url`: the one spelling every mint URL is stored under, in both
  kind-9321 (nutzaps) `u` tags and kind-10019 (nutzap-configs) `mint` tags.
- `classify`: a mint's class, from the automatic private-host rule and then the reviewed
  registry (`data/nutzaps/mint_registry.yaml`). Only the EXCLUDED_CLASSES leave the
  numbers; every other mint, including one nobody has reviewed, is counted.

The loader materialises the result into `nutzap_mint_classes`
(`--classify-nutzap-mints`), and every reader joins that table. Rules and evidence:
docs/design/nutzaps-design.md, "Test and private mints" (nostrology-data#9).
"""

from __future__ import annotations

import datetime as _dt
import ipaddress
import re
from typing import NamedTuple
from urllib.parse import urlsplit

# Longest URL kept, in UTF-8 bytes. Mint URLs land in btree / GIN indexes whose entries
# max out near 2,700 bytes; an incompressible URL longer than that loads, then fails the
# index rebuild, and init.sql (applied before EVERY surface's load) fails on it every night
# until the row is removed by hand. The longest archived mint URL is 380 bytes.
MAX_URL_BYTES = 1024

_LONE_SURROGATE_RE = re.compile("[\ud800-\udfff]")
_DEFAULT_PORTS = {"http": 80, "https": 443}
# Path segments a browser (WHATWG URL) resolves away, including percent-encoded forms.
_DOT_SEGMENTS = {".", "..", "%2e", "%2e%2e", ".%2e", "%2e."}


def url_text_ok(s: str) -> bool:
    """False for text a URL column cannot safely hold: a NUL or a lone surrogate
    (Postgres text refuses both; psycopg2 cannot even encode a surrogate), or more than
    MAX_URL_BYTES bytes."""
    if "\x00" in s or _LONE_SURROGATE_RE.search(s):
        return False
    return len(s.encode("utf-8")) <= MAX_URL_BYTES


def normalize_mint_url(url) -> str | None:
    """The stored spelling of a Cashu mint URL, or None when it is not a usable http(s) URL.

    Folds only what cannot change which server answers:
      - surrounding whitespace and trailing slashes;
      - the case of the scheme and the host (both are case-insensitive);
      - a default port (`:443` on https, `:80` on http).
    The PATH keeps its case: `https://mint.minibits.cash/Bitcoin` answers and `/bitcoin`
    returns 404 (checked 2026-09-18), so they are different mints and stay apart. Query and
    fragment are kept as written. Not usable, so None:
      - a port that is not a number in 0..65535;
      - a backslash before the path (Python puts `a\\@b` on host b, browsers on host a);
      - a host part that does not rebuild to itself (`[v1.fe]`, `[::1]x`: urlsplit would
        quietly turn them into a different host);
      - a `.` or `..` path segment (a browser resolves it away, so the per-mint link would
        land on another page).
    """
    if not url or not isinstance(url, str):
        return None
    s = url.strip().rstrip("/")
    if not url_text_ok(s):
        return None
    scheme, sep, rest = s.partition("://")
    scheme = scheme.lower()
    if not sep or scheme not in _DEFAULT_PORTS:
        return None
    if any(c in rest for c in "\t\r\n"):
        # urlsplit silently deletes these, so the parsed URL would no longer be the text.
        return None
    try:
        parts = urlsplit(f"{scheme}://{rest}")
        host = parts.hostname  # lowercased, brackets stripped
        port = parts.port  # raises ValueError on a non-numeric or out-of-range port
    except ValueError:
        return None
    if not host:
        return None
    netloc = parts.netloc
    if "\\" in netloc or not rest.startswith(netloc):
        return None
    userinfo, at, raw_hostport = netloc.rpartition("@")
    bare = f"[{host}]" if ":" in host else host
    raw = raw_hostport.lower()
    if not (raw == bare or re.fullmatch(re.escape(bare) + r":[0-9]*", raw)):
        return None
    hostport = bare
    if port is not None and port != _DEFAULT_PORTS[scheme]:
        hostport = f"{hostport}:{port}"
    tail = rest[len(netloc):]
    path = urlsplit(f"x://h{tail}").path
    if any(seg.lower() in _DOT_SEGMENTS for seg in path.split("/")):
        return None
    return f"{scheme}://{userinfo}{at}{hostport}{tail}"


# -- The automatic rule: hosts a recipient elsewhere cannot reach --
#
# A mint on one of these hosts is someone's own machine, LAN, container or tailnet. The
# recipient of a nutzap on it cannot redeem the ecash, so it is not a payment to anyone
# else, and the URL is not even one mint: every developer's 127.0.0.1:3338 is a different
# server. .onion and plain-http PUBLIC hosts are reachable and are not caught.

# Written out rather than read from ipaddress's is_private / is_global, whose ranges have
# changed between Python releases (3.11 vs 3.12.4 vs 3.13); the image and the tests must agree.
_NON_PUBLIC_NETS = tuple(ipaddress.ip_network(n) for n in (
    "0.0.0.0/8",        # "this network", incl. 0.0.0.0
    "10.0.0.0/8",       # RFC 1918
    "100.64.0.0/10",    # carrier-grade NAT; Tailscale addresses live here
    "127.0.0.0/8",      # loopback
    "169.254.0.0/16",   # link-local, incl. the cloud metadata address
    "172.16.0.0/12",    # RFC 1918, incl. Docker's 172.17.0.1 bridge
    "192.0.0.0/24",     # IETF protocol assignments
    "192.0.2.0/24",     # documentation
    "192.168.0.0/16",   # RFC 1918
    "198.18.0.0/15",    # benchmarking
    "198.51.100.0/24",  # documentation
    "203.0.113.0/24",   # documentation
    "224.0.0.0/4",      # multicast
    "240.0.0.0/4",      # reserved, incl. 255.255.255.255
    "::/128",           # unspecified
    "::1/128",          # loopback
    "fc00::/7",         # unique local
    "fe80::/10",        # link-local
    "ff00::/8",         # multicast
    "2001:db8::/32",    # documentation
))

_NAT64 = ipaddress.ip_network("64:ff9b::/96")
_V4_COMPAT = ipaddress.ip_network("::/96")

# Names that never resolve on the public internet: RFC 6761 / 6762 / 8375 special-use
# names, ICANN's reserved .internal, and the de-facto home and office suffixes.
_NON_PUBLIC_SUFFIXES = (
    "localhost", "local", "localdomain", "internal", "lan", "home", "home.arpa", "corp",
    "test", "example", "invalid",
)
_NUMERIC_LABEL_RE = re.compile(r"(0x[0-9a-f]*|[0-9]+)")


def private_host_reason(host: str) -> str | None:
    """Why `host` (as urlsplit's .hostname returns it) cannot be reached by a recipient
    elsewhere, or None for a public host."""
    h = host.lower().rstrip(".")
    if not h:
        return "empty host"
    try:
        ip = ipaddress.ip_address(h.split("%", 1)[0])  # drop an IPv6 zone id
    except ValueError:
        ip = None
    if ip is not None:
        if ip.version == 6:
            # An IPv6 spelling of an IPv4 address is judged as that address: mapped
            # (::ffff:a.b.c.d), 6to4 (2002::/16), NAT64 (64:ff9b::/96) and the deprecated
            # IPv4-compatible form (::a.b.c.d).
            embedded = ip.ipv4_mapped or ip.sixtofour
            if embedded is None and (ip in _NAT64 or (ip in _V4_COMPAT and int(ip) > 1)):
                embedded = ipaddress.IPv4Address(int(ip) & 0xFFFFFFFF)
            if embedded is not None:
                ip = embedded
        for net in _NON_PUBLIC_NETS:
            if ip.version == net.version and ip in net:
                return f"address in {net}"
        return None
    if any(h == s or h.endswith("." + s) for s in _NON_PUBLIC_SUFFIXES):
        return "non-public name"
    if "." not in h:
        return "single-label host"
    # A browser reads a host whose last label is numeric as an IPv4 address in a short or
    # hex form (127.1, 0x7f.1). No public mint is written that way.
    if _NUMERIC_LABEL_RE.fullmatch(h.rsplit(".", 1)[1]):
        return "non-canonical IP address"
    return None


# -- The reviewed registry --

# class -> excluded from value, payments, senders, the leaderboard and the per-mint page.
# schema/init.sql writes the same lists into nutzap_mint_classes' CHECK constraints, and that
# table is CREATE TABLE IF NOT EXISTS: adding or re-scoping a class here needs a hand-run
# ALTER of both constraints in production first, or every nightly classification fails its INSERT
# (and with it the MAU rebuild). test_nutzap_mints pins the two lists together.
CLASSES = {
    "private-host": True,       # automatic: the host rule above; never written in the registry
    "fake-backend": True,       # the mint says it runs a fake Lightning backend (testnut)
    "test-mint-gone": True,     # named as a test mint, and no longer answers
    "test-wording-live": False, # calls itself test/demo, but settles real Lightning; kept
    "unreachable": False,       # does not answer today; its past payments were real; kept
    "unreviewed": False,        # the default: counted, never presumed fake
}
EXCLUDED_CLASSES = frozenset(c for c, excluded in CLASSES.items() if excluded)
REGISTRY_CLASSES = frozenset(CLASSES) - {"private-host", "unreviewed"}


class MintClass(NamedTuple):
    mint_class: str
    source: str        # 'host-rule' | 'registry' | 'none'
    evidence: str | None

    @property
    def excluded(self) -> bool:
        return CLASSES[self.mint_class]


class Registry(NamedTuple):
    by_url: dict   # normalized url -> MintClass
    by_host: dict  # lowercased host -> MintClass


def parse_registry(doc, *, source: str = "mint registry") -> Registry:
    """Validate the parsed YAML of data/nutzaps/mint_registry.yaml.

    Each entry names exactly one of `url` (one mint, written normalized) or `host` (every
    URL on that host, any scheme, port or path), plus `class`, `evidence`, `as_of` (a date)
    and `reviewer`. Raises ValueError on anything malformed, so a typo fails the refresh
    instead of silently counting a test mint.
    """
    if not isinstance(doc, dict) or not isinstance(doc.get("mints"), list):
        raise ValueError(f"{source}: expected a top-level 'mints' list")
    by_url: dict = {}
    by_host: dict = {}
    for i, e in enumerate(doc["mints"]):
        where = f"{source}: mints[{i}]"
        if not isinstance(e, dict):
            raise ValueError(f"{where} is not a mapping")
        unknown = set(e) - {"url", "host", "class", "evidence", "as_of", "reviewer", "notes"}
        if unknown:
            raise ValueError(f"{where} has unknown keys {sorted(unknown)}")
        cls = e.get("class")
        if cls not in REGISTRY_CLASSES:
            raise ValueError(f"{where}: class {cls!r} is not one of {sorted(REGISTRY_CLASSES)}")
        evidence = e.get("evidence")
        if not isinstance(evidence, str) or not evidence.strip():
            raise ValueError(f"{where}: missing 'evidence'")
        as_of = e.get("as_of")
        if isinstance(as_of, str):
            try:
                as_of = _dt.date.fromisoformat(as_of)
            except ValueError:
                as_of = None
        if not isinstance(as_of, _dt.date):
            raise ValueError(f"{where}: 'as_of' must be a YYYY-MM-DD date")
        reviewer = e.get("reviewer")
        if not isinstance(reviewer, str) or not reviewer.strip():
            raise ValueError(f"{where}: missing 'reviewer'")
        url, host = e.get("url"), e.get("host")
        if (url is None) == (host is None):
            raise ValueError(f"{where}: give exactly one of 'url' or 'host'")
        mc = MintClass(cls, "registry", evidence.strip())
        if url is not None:
            if normalize_mint_url(url) != url:
                raise ValueError(f"{where}: url {url!r} is not normalized "
                                 f"(expected {normalize_mint_url(url)!r})")
            if url in by_url:
                raise ValueError(f"{where}: url {url!r} is listed twice")
            by_url[url] = mc
        else:
            if (not isinstance(host, str) or host != host.lower().strip() or not host or "/" in host
                    or ":" in host or host.endswith(".")):
                raise ValueError(f"{where}: host {host!r} must be a bare lowercase host name")
            if host in by_host:
                raise ValueError(f"{where}: host {host!r} is listed twice")
            by_host[host] = mc
    return Registry(by_url, by_host)


def load_registry(path: str) -> Registry:
    import yaml  # lazily, like the signer manifest: the rest of the loader does not need PyYAML
    with open(path, "r", encoding="utf-8") as f:
        return parse_registry(yaml.safe_load(f), source=path)


def classify(url: str, registry: Registry) -> MintClass:
    """The class of a normalized mint URL. First match wins:
    the private-host rule, then a registry `url` row, then a registry `host` row;
    anything else is 'unreviewed' and counted."""
    host = urlsplit(url).hostname or ""
    reason = private_host_reason(host)
    if reason:
        return MintClass("private-host", "host-rule", reason)
    if url in registry.by_url:
        return registry.by_url[url]
    hit = registry.by_host.get(host.rstrip("."))
    if hit:
        return hit
    return MintClass("unreviewed", "none", None)
