"""Unit tests for pipeline/ngit - the /git metrics compute.

Two granularities, mirroring test_pipeline_routstr.py:
  (a) primitive pins - URL parsing and host classes, the GRASP shape, relay normalization,
      npub decoding, the role-tag grammar, version ordering, the ref-map diff, the bolt11
      gate: the rules docs/dataset-reference/ngit-dataset-reference.md states and the independent
      cross-check must match exactly;
  (b) methodology on hand-verifiable synthetic fixtures - every noise-ledger family with a
      positive and a near-miss, repository components and the lead, the push ledger (no
      daily cap since 2026-10-02), windows at week and month boundaries, the weekly series
      from each capture wall's week, hosting, the comment ledger,
      stars (kind-7 (reactions) and Iris Git's kind-17 (external-content-reactions)), zaps,
      caps and folds, future-dated rows, the invariants against a broken
      document, and the write gates (refusing before any write);
  (c) the roster reduction - the reduced kind-30617 (repo-announcements) and kind-30618
      (repo-states) rows build exactly the snapshot the full rows build.
"""

import copy
import datetime as dt
import itertools
import json
import os

import pytest

from dagster_code.pipeline import ngit as N

DAY = N.DAY_S
# 2026-09-25T03:15:00Z - a nightly run instant; as_of_day is 2026-09-24, a Thursday.
NOW = 1790306100
AS_OF_MIDNIGHT = 1790208000          # 2026-09-24T00:00:00Z
assert N.day_str(AS_OF_MIDNIGHT) == "2026-09-24"
assert dt.date(2026, 9, 24).weekday() == 3


def D(n, sec=43200):
    """A timestamp n days before as_of_day, at `sec` seconds past midnight UTC."""
    return AS_OF_MIDNIGHT - n * DAY + sec


_ids = iter(range(1, 10_000_000))


def nid():
    return f"{next(_ids):064x}"


def pk(i):
    return f"{i:02x}" * 32


PK1, PK2, PK3, PK4, PK5, PK6, PK7, PK8, PK9 = (pk(i) for i in range(1, 10))
SK1 = N.SECRET_KEY_1_PUBKEY
NPUB_SHAPE = "npub1" + "q" * 58            # the GRASP path shape (checksum not required)
C1, C2, C3, C4 = ("1" * 40, "2" * 40, "3" * 40, "4" * 40)
GITHUB = "https://github.com/o/r.git"


def grasp_url(host="relay.ngit.dev", repo="r"):
    return f"https://{host}/{NPUB_SHAPE}/{repo}.git"


def ann(pubkey, d, t, *, name=None, clone=(GITHUB,), web=(), relays=(), topics=(),
        maint=None, extra=(), eid=None):
    tags = [["d", d]]
    if name is not None:
        tags.append(["name", name])
    tags += [["clone", c] for c in clone]
    tags += [["web", w] for w in web]
    tags += [["relays", r] for r in relays]
    tags += [["t", x] for x in topics]
    if maint is not None:
        tags.append(["maintainers", *maint])
    tags += [list(x) for x in extra]
    return {"id": eid or nid(), "pubkey": pubkey, "kind": 30617, "created_at": t,
            "content": "", "tags": tags, "sig": "0" * 128, "d_tag": d}


def st(pubkey, d, t, refs=None, head="ref: refs/heads/main", *, eid=None, extra=()):
    refs = {"refs/heads/main": C1} if refs is None else refs
    tags = [["d", d]]
    if head is not None:
        tags.append(["HEAD", head])
    tags += [[k, v] for k, v in refs.items()]
    tags += [list(x) for x in extra]
    return {"id": eid or nid(), "pubkey": pubkey, "kind": 30618, "created_at": t,
            "content": "", "tags": tags, "sig": "0" * 128, "d_tag": d}


def comment(pubkey, t, *, K="1621", E="e" * 64, A=None, a=None, content="hi", extra=()):
    tags = []
    if K is not None:
        tags.append(["K", K])
    if E is not None:
        tags.append(["E", E])
    if A is not None:
        tags.append(["A", A])
    if a is not None:
        tags.append(["a", a])
    tags += [list(x) for x in extra]
    return {"id": nid(), "pubkey": pubkey, "kind": 1111, "created_at": t, "content": content,
            "tags": tags, "sig": "0" * 128}


def reaction(pubkey, t, *, content="+", a=None, e=None, k="30617"):
    tags = []
    if e is not None:
        tags.append(["e", e])
    if a is not None:
        tags.append(["a", a])
    if k is not None:
        tags.append(["k", k])
    return {"id": nid(), "pubkey": pubkey, "kind": 7, "created_at": t, "content": content,
            "tags": tags, "sig": "0" * 128}


def ext_reaction(pubkey, t, *, content="+", i=None, k="git-repo", eid=None, extra=()):
    """kind-17 (external-content-reactions) as Iris Git writes a star: k=git-repo, i=30617:...,
    p=<owner> (ref 6.2). `i` may be a list of values, one `i` tag each, in order."""
    tags = []
    if k is not None:
        tags.append(["k", k])
    for v in ([] if i is None else [i] if isinstance(i, str) else i):
        tags.append(["i", v])
    tags += [list(x) for x in extra]
    return {"id": eid or nid(), "pubkey": pubkey, "kind": 17, "created_at": t, "content": content,
            "tags": tags, "sig": "0" * 128}


# ref 6.3: a receipt counts only when a verified LNURL provider key signed it; the fixtures sign
# with this stand-in, which build() passes as the whole verified set.
SIGNER = "f" * 64
SIGNERS = frozenset({SIGNER})


def zap(t, *, a, bolt11, sender=PK9, signer=SIGNER, p=PK1, amount=None):
    tags = [["a", a]]
    if p is not None:
        tags.append(["p", p])
    if bolt11 is not None:
        tags.append(["bolt11", bolt11])
    if sender is not None:
        req_tags = [] if amount is None else [["amount", amount]]
        tags.append(["description", json.dumps({"kind": 9734, "pubkey": sender, "created_at": t,
                                                "tags": req_tags, "content": ""})])
    return {"id": nid(), "pubkey": signer, "kind": 9735, "created_at": t, "content": "",
            "tags": tags, "sig": "0" * 128}


def dele(author, coord, t):
    return {"author": author, "coord": coord, "created_at": t}


def build(anns=(), states=(), grasp=(), comments=(), reactions=(), zaps=(), dels=(),
          names=None, now=NOW, signers=SIGNERS, ext=()):
    return N.build_snapshot(list(anns), list(states), list(grasp), list(comments),
                            list(reactions), list(zaps), list(dels), names or {}, now,
                            zap_signers=signers, external_reaction_events=list(ext))


def row(state, pubkey, d):
    (r,) = [r for r in state["repos_rows"] if r["pubkey"] == pubkey and r["d"] == d]
    return r


def fam(state, pubkey, d):
    return row(state, pubkey, d)["family"]


def pushes_of(state, pubkey, d):
    return [r for r in state["push_rows"] if r["pubkey"] == pubkey and r["d"] == d]


def kv(lst):
    return {e["k"]: e["v"] for e in lst}


# --- (a) primitive pins ---------------------------------------------------------------------

def test_parse_url_scheme_userinfo_port_and_terminators():
    assert N.parse_url("https://npub13kk@Git.Shakespeare.DIY:443/npub1x/r.git") == (
        "https", False, "git.shakespeare.diy", "/npub1x/r.git")
    assert N.parse_url("ssh://git@git.nostrdev.com:29418/repo") == (
        "ssh", False, "git.nostrdev.com", "/repo")
    # userinfo up to the LAST @; the port is ':' + digits at the end (a bare ':' too)
    assert N.parse_url("https://a@b@host.example:/x")[2] == "host.example"
    assert N.parse_url("HTTPS://Host.Example?x=1") == ("https", False, "host.example", "?x=1")
    assert N.parse_url("https://host.example#frag")[3] == "#frag"
    assert N.parse_url("htree://npub1abc/repo")[:3] == ("htree", False, "npub1abc")
    assert N.parse_url("gnostr-cloud://host.example/r")[0] == "gnostr-cloud"
    # an empty authority is no host
    assert N.parse_url("https:///path")[2] is None


def test_parse_url_scp_like_and_scheme_less():
    assert N.parse_url("git@GitHub.com:org/repo.git") == (None, True, "github.com", None)
    assert N.parse_url("ubuntu@melvin.me:www/git/sync") == (None, True, "melvin.me", None)
    # no scheme, not scp-like: no host at all
    assert N.parse_url("github.com/org/repo") == (None, False, None, None)
    assert N.parse_url("git@host/path:x") == (None, False, None, None)   # '/' before ':'
    assert N.parse_url("a b@host:x") == (None, False, None, None)        # whitespace


def test_values_are_trimmed_of_any_whitespace_deduped_and_ordered():
    tags = [["clone", " \t https://a.example/x \u00a0", "", "https://b.example/y"],
            ["clone", "https://a.example/x", "\n"], ["t", " Rust\u2003", "NOSTR"]]
    assert N.clean_values(tags, "clone") == ["https://a.example/x", "https://b.example/y"]
    assert N.announcement_attrs({"pubkey": PK1, "tags": tags})["t"] == ["rust", "nostr"]


def test_private_hosts_and_boundaries():
    for h in ("localhost", "0.0.0.0", "box.local", "127.0.0.1", "127.255.0.9", "10.1.2.3",
              "172.16.0.1", "172.31.255.255", "192.168.1.4", "100.64.0.1", "100.127.9.9"):
        assert N.is_private_host(h), h
    for h in ("172.15.0.1", "172.32.0.1", "100.63.0.1", "100.128.0.1", "192.169.0.1",
              "11.0.0.1", "999.1.1.1", "local", "relay-2", "8.8.8.8", "0.0.0.1",
              # each octet at most 255: an in-range prefix does not make a non-address private
              "10.0.0.256", "192.168.1.300", "127.999.0.1"):
        assert not N.is_private_host(h), h
    # ref 4.2 (review 4 m2): a nip.io / sslip.io name embeds an IPv4 as the four octets right
    # before the suffix, separated by `.` or `-`, each at most 255, starting the host or
    # following a `.`
    for h in ("grasp.127.0.0.1.nip.io", "x.10-0-0-1.sslip.io", "10.0.0.1.nip.io",
              "a.b.192.168.1.4.sslip.io", "172.16.0.1.nip.io", "100.64.0.1.nip.io",
              "10.0-0.1.nip.io", "1.2.3.4.10.0.0.1.nip.io"):
        assert N.is_private_host(h), h
    for h in ("1.2.3.4.nip.io", "8-8-8-8.sslip.io", "10.0.0.1.2.nip.io",   # the LAST four octets
              "app-10-0-0-1.nip.io", "app-127-0-0-1.nip.io",             # they follow a `-`
              "x10.0.0.1.nip.io",                                        # `x10` is no octet
              "256.0.0.1.nip.io", "10.0.0.256.nip.io", "x.192-168-1-300.sslip.io",
              "127.0.0.nip.io", "nip.io", "127.0.0.1nip.io",
              "10.0.0.1.nip.io.example.com", "10.0.0.1.example.io", "172.32.0.1.sslip.io"):
        assert not N.is_private_host(h), h


def test_host_classes_and_precedence():
    gh = {"relay.ngit.dev", "github.com"}
    cls = lambda u: N.host_class(N.parse_url(u), gh)        # noqa: E731
    assert cls("htree://localhost/r") == "hashtree"          # hashtree before private
    assert cls("http://localhost:3000/r.git") == "private"
    assert cls("https://relay.ngit.dev/x/r.git") == "grasp"  # the HOST decides, not the shape
    # grasp precedes forge in the class precedence - moot since ref 4.3 keeps every forge host
    # out of the GRASP host set, but the primitive keeps the order
    assert cls("https://github.com/o/r") == "grasp"
    assert N.host_class(N.parse_url("https://github.com/o/r"), set()) == "forge"
    assert cls("git@codeberg.org:o/r.git") == "forge"
    assert cls("nostr://npub1abc/relay.example/r") == "non_network"   # a host, a non-network scheme
    assert cls("rns://abc") == "non_network"
    assert cls("not a url") == "non_network"                 # no host
    assert cls("git@git.example.org:o/r.git") == "self_hosted"  # scp-like is network
    assert cls("wss://git.example.org/r") == "self_hosted"
    assert cls("git://git.example.org/r") == "self_hosted"
    assert cls("http://relay-2:7334/r") == "self_hosted"      # dotless but not private


def test_repo_hosting_class_precedence():
    assert N.repo_hosting_class({"grasp"}) == "grasp_only"
    assert N.repo_hosting_class({"grasp", "forge"}) == "grasp_mixed"
    assert N.repo_hosting_class({"grasp", "hashtree"}) == "grasp_mixed"
    assert N.repo_hosting_class({"hashtree", "self_hosted"}) == "hashtree"
    assert N.repo_hosting_class({"hashtree", "forge"}) == "hashtree"
    assert N.repo_hosting_class({"forge"}) == "forge_only"
    assert N.repo_hosting_class({"forge", "self_hosted"}) == "other_hosts"
    assert N.repo_hosting_class({"self_hosted"}) == "other_hosts"
    assert N.repo_hosting_class(set()) == "none"


def test_repo_hosting_classes_partition_every_reachable_set():
    """ref 4.4: the six classes are mutually exclusive and collectively exhaustive. Over EVERY
    subset R of the reachable host classes (16), exactly one class's own condition holds, each
    tested as the reference states it and without the precedence, and repo_hosting_class
    returns that class. So a repository is in one class whatever its clone values, and the
    hosting list sums to `repos`. A fifth reachable host class fails here until 4.4 gives it a
    place: the precedence alone would put a repository on it alone, or on it and forges only,
    in `none`, "No public host"."""
    assert N.REACHABLE == {"grasp", "hashtree", "forge", "self_hosted"}   # ref 4.2's four
    conditions = {
        "grasp_only": lambda r: r == {"grasp"},
        "grasp_mixed": lambda r: "grasp" in r and len(r) > 1,
        "hashtree": lambda r: "hashtree" in r and "grasp" not in r,
        "forge_only": lambda r: r == {"forge"},
        "other_hosts": lambda r: "self_hosted" in r and "grasp" not in r and "hashtree" not in r,
        "none": lambda r: not r,
    }
    assert tuple(conditions) == N.REPO_CLASSES          # the document's order, the precedence
    members = sorted(N.REACHABLE)
    seen = set()
    for size in range(len(members) + 1):
        for combo in itertools.combinations(members, size):
            r = frozenset(combo)
            held = [c for c, cond in conditions.items() if cond(r)]
            assert len(held) == 1, (sorted(r), held)
            assert N.repo_hosting_class(r) == held[0], sorted(r)
            seen.add(held[0])
    assert seen == set(N.REPO_CLASSES)                  # and no class is unreachable


def test_grasp_shape():
    ok = f"https://relay.ngit.dev/{NPUB_SHAPE}/r.git"
    assert N.is_grasp_shaped(N.parse_url(ok))
    assert N.is_grasp_shaped(N.parse_url(f"wss://relay.ngit.dev/{NPUB_SHAPE}/r"))
    assert N.is_grasp_shaped(N.parse_url(f"http://h.example:7334/{NPUB_SHAPE}/r.git"))  # a port
    assert N.is_grasp_shaped(N.parse_url(f"https://u@h.example/{NPUB_SHAPE}/r.git"))    # userinfo
    # the bech32 charset excludes b, i, o and 1 after the prefix
    for bad in ("b", "i", "o", "1"):
        assert not N.is_grasp_shaped(N.parse_url(f"https://h.example/npub1{bad}{'q' * 57}/r"))
    assert not N.is_grasp_shaped(N.parse_url(f"https://h.example/npub1{'q' * 57}/r"))    # 57
    assert not N.is_grasp_shaped(N.parse_url(f"https://h.example/{NPUB_SHAPE}"))         # no slash
    assert not N.is_grasp_shaped(N.parse_url(f"ssh://h.example/{NPUB_SHAPE}/r"))         # scheme
    assert not N.is_grasp_shaped(N.parse_url(f"https://h.example/x/{NPUB_SHAPE}/r"))     # not first
    assert not N.is_grasp_shaped(N.parse_url(f"https://h.example/{'NPUB1' + 'Q' * 58}/r"))


def test_relay_normalization():
    assert N.norm_relay("WSS://Relay.Example.COM/") == "relay.example.com"
    assert N.norm_relay("ws://relay.example.com//") == "relay.example.com"
    assert N.norm_relay("wss://relay.example.com/path/") == "relay.example.com/path"
    assert N.norm_relay("https://relay.example.com") == "https://relay.example.com"  # only ws(s)
    assert N.norm_relay("wss://localhost:7777") is None                             # no dot
    assert N.norm_relay("relay") is None


def test_npub_decoding():
    # NIP-19 (bech32 entities) published test vector
    v = "npub10elfcs4fr0l0r8af98jlmgdh9c8tcxjvz9qkw038js35mp4dma8qzvjptg"
    hexpk = "7e7e9c42a91bfef19fa929e5fda1b72e0ebc1a4c1141673e2794234d86addf4e"
    assert N.npub_to_hex(v) == hexpk
    assert N.norm_pubkey(v) == hexpk
    assert N.norm_pubkey(v[:-1] + ("q" if v[-1] != "q" else "p")) is None   # checksum
    assert N.norm_pubkey(v.upper()) is None          # not an `npub1...` string
    assert N.norm_pubkey(" " + v) is None            # values are not trimmed
    assert N.norm_pubkey("nsec1" + v[5:]) is None
    assert N.norm_pubkey(hexpk.upper()) == hexpk     # 64-hex lowercased
    assert N.norm_pubkey(hexpk[:63]) is None
    assert N.norm_pubkey(None) is None
    assert N.npub_to_hex(v[:30]) is None


def test_role_tag_grammar_and_maintainer_listing():
    assert N.role_tag_active(["m", PK2])
    assert N.role_tag_active(["m", PK2, "1700000000"])
    assert not N.role_tag_active(["m", PK2, "1700000000", "1700000100"])            # ended
    assert N.role_tag_active(["m", PK2, "1700000000", "1700000100", "1700000200"])  # re-activated
    assert not N.role_tag_active(["m", PK2, "1", "2", "3", "4"])
    npub = "npub10elfcs4fr0l0r8af98jlmgdh9c8tcxjvz9qkw038js35mp4dma8qzvjptg"
    npub_hex = "7e7e9c42a91bfef19fa929e5fda1b72e0ebc1a4c1141673e2794234d86addf4e"
    # role mode: active M/m values only; the legacy tag is ignored once M, m or o exists
    tags = [["M", PK1], ["m", PK2, "1"], ["m", PK3, "1", "2"], ["m", npub, "1", "2", "3"],
            ["maintainers", PK4], ["m", "junk"]]
    listing, self_lead = N.maintainer_listing(tags, PK1)
    assert listing == {PK2, npub_hex} and self_lead
    # an `o` tag alone switches to role mode (so the legacy listing is NOT read)
    assert N.maintainer_listing([["o", "x", "y"], ["maintainers", PK4]], PK1) == (set(), False)
    # legacy mode: every value of every maintainers tag, own pubkey removed, junk dropped
    listing, self_lead = N.maintainer_listing(
        [["maintainers", PK1, PK2.upper(), "junk"], ["maintainers", PK3]], PK1)
    assert listing == {PK2, PK3} and not self_lead
    # an ENDED self-M confers nothing
    assert N.maintainer_listing([["M", PK1, "1", "2"]], PK1) == (set(), False)


def test_version_order_same_second_lowest_id_is_current():
    a = st(PK1, "r", 100, eid="b" * 64)
    b = st(PK1, "r", 100, eid="a" * 64)
    c = st(PK1, "r", 99, eid="f" * 64)
    order = N.version_sorted([b, a, c])
    assert [r["id"][0] for r in order] == ["f", "b", "a"]   # current = the lowest id


def test_ref_map_diff_peeled_repeated_head_only():
    refs, head = N.ref_map_and_head([
        ["d", "r"], ["HEAD", "ref: refs/heads/main"], ["HEAD", "ref: refs/heads/dev"],
        ["refs/heads/main", C1], ["refs/heads/main", C2],       # repeated: first wins
        ["refs/tags/v1", C3], ["refs/tags/v1^{}", C4],          # peeled: dropped
        ["refs/heads/broken"], ["refs/notes/x", C1], ["refs/heads/x^{}", C2],
    ])
    # the one-element "refs/heads/broken" tag is ignored (ref 3)
    assert refs == {"refs/heads/main": C1, "refs/tags/v1": C3}
    assert head == "ref: refs/heads/main"
    first = N.version_flags(None, (refs, head), N.STATE_WALL)
    assert first["changed"] and first["push"] and not first["head_changed"]
    assert (first["branches_added"], first["tags_added"], first["heads_moved"]) == (1, 1, 0)
    # tag order irrelevant + HEAD-only change: not changed, head_changed
    same = ({"refs/tags/v1": C3, "refs/heads/main": C1}, "ref: refs/heads/dev")
    f = N.version_flags((refs, head), same, N.STATE_WALL + 10)
    assert not f["changed"] and not f["push"] and f["head_changed"]
    assert all(f[k] == 0 for k in ("heads_moved", "branches_added", "branches_removed",
                                   "tags_added", "tags_removed", "tags_moved"))
    # a move of the default branch, a tag move, an added and a removed branch
    new = ({"refs/heads/main": C2, "refs/heads/feat": C1, "refs/tags/v1": C4},
           "ref: refs/heads/main")
    old = ({"refs/heads/main": C1, "refs/heads/gone": C3, "refs/tags/v1": C3},
           "ref: refs/heads/dev")
    f = N.version_flags(old, new, N.STATE_WALL + 20)
    assert f["changed"] and f["push"] and f["default_moved"]     # the CURRENT row's head
    assert (f["heads_moved"], f["branches_added"], f["branches_removed"]) == (1, 1, 1)
    assert (f["tags_moved"], f["tags_added"], f["tags_removed"]) == (1, 0, 0)
    assert (f["branches"], f["tags"]) == (2, 1)
    # a changed pair BEFORE the wall is not a push
    f = N.version_flags(old, new, N.STATE_WALL - 1)
    assert f["changed"] and not f["push"]


def test_bolt11_gate_through_the_shared_parser():
    zp = N.load_zap_parser()
    amt = lambda b: N.zap_amount_msats(zp, [["bolt11", b]])   # noqa: E731
    assert amt("lnbc100n1pjxyz") == 10_000
    assert amt("lnbc1u1pjxyz") == 100_000
    assert amt("lnbc210m1pjxyz") == 21_000_000_000
    assert amt("lnbc1pvjluezxyz") is None         # no amount
    assert N.zap_amount_msats(zp, [["p", PK1]]) is None
    # the LAST bolt11 tag that HAS a value is read (ref 6.3), scanned like zap_parser
    assert N.zap_amount_msats(zp, [["bolt11", "lnbc100n1a"], ["bolt11", "lnbc1u1a"]]) == 100_000
    assert N.zap_amount_msats(zp, [["bolt11", "lnbc100n1a"], ["bolt11"]]) == 10_000
    assert N.zap_amount_msats(zp, [["bolt11", "lnbc100n1a"], ["bolt11", None]]) is None
    assert N.zap_amount_msats(zp, [["bolt11", "lnbc0n1a"]]) == 0       # a 0-msat amount is kept
    # the zapper: the description's pubkey when it parses, else none
    desc = lambda v: [["description", v]]                     # noqa: E731
    assert N.zap_sender(desc(json.dumps({"pubkey": PK9, "created_at": "x"}))) == PK9
    assert N.zap_sender(desc("{not json")) is None
    assert N.zap_sender(desc(json.dumps(["pubkey", PK9]))) is None
    assert N.zap_sender(desc(json.dumps({"pubkey": ""}))) is None
    assert N.zap_sender(desc(json.dumps({"pubkey": PK9})) + [["description"]]) == PK9


def test_zap_parser_is_loaded_by_path_and_fails_loud_when_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(N, "_ZAP_PARSER", None)
    monkeypatch.setattr(N, "ZAP_PARSER_PATH", str(tmp_path / "missing" / "zap_parser.py"))
    with pytest.raises(RuntimeError, match="zap parser not found"):
        build([ann(PK1, "r", D(10))])


def test_recency_bucket_edges():
    assert [N.recency_bucket(d) for d in (-1, 0, 29, 30, 89, 90, 179, 180, 364, 365, 900)] == [
        "30d", "30d", "30d", "90d", "90d", "180d", "180d", "365d", "365d", "older", "older"]


def test_display_name_cleaning():
    assert N.display_name("  Alice\x00\n ", "alice") == "Alice"
    assert N.display_name("", "bob") == "bob"
    assert N.display_name(None, None) is None
    assert N.display_name("x" * 60, None) == "x" * 40


# --- (b) the noise ledger -------------------------------------------------------------------

def test_n1_squatter_on_clone_or_web_and_near_miss():
    s, _ = build([
        ann(PK1, "a", D(9), clone=("https://nostr.life/agent/a",)),
        ann(PK2, "b", D(9), web=("https://nostr.life/p/b",)),
        ann(PK3, "c", D(9), clone=("http://nostr.life/agent/c",)),   # http, not https
    ])
    assert (fam(s, PK1, "a"), fam(s, PK2, "b"), fam(s, PK3, "c")) == ("N1", "N1", "repo")


def test_n2_harness_and_near_miss():
    s, _ = build([
        ann(PK1, "a", D(9), clone=("git@example.com:test/a.git",)),
        ann(PK2, "b", D(9), extra=(["euc", C1],)),                    # a bare euc tag
        ann(PK3, "c", D(9), clone=("https://example.com/test/c.git",)),
        ann(PK4, "d", D(9), extra=(["r", C1, "euc"],)),               # the r-tag form is fine
    ])
    assert [fam(s, p, d) for p, d in ((PK1, "a"), (PK2, "b"), (PK3, "c"), (PK4, "d"))] == [
        "N2", "N2", "repo", "repo"]


def test_n3_template_copy_exact_only():
    s, _ = build([
        ann(PK1, "a", D(9), name="<human-readable project name>"),
        ann(PK2, "b", D(9), name="<human-readable project name> "),
    ])
    assert (fam(s, PK1, "a"), fam(s, PK2, "b")) == ("N3", "repo")


def test_n4_date_named_d_full_match_only():
    ds = ["20260511", "2026051", "20260511a", "19260511", "2026-05-11"]
    s, _ = build([ann(pk(10 + i), d, D(9)) for i, d in enumerate(ds)])
    assert [fam(s, pk(10 + i), d) for i, d in enumerate(ds)] == [
        "N4", "repo", "repo", "repo", "repo"]


def _versions(pubkey, d, gaps, t0=D(200)):
    out, t = [], t0
    for g in [0] + list(gaps):
        t += g
        out.append(ann(pubkey, d, t))
    return out


def test_n5_republish_loop_threshold_and_even_median():
    loop = _versions(PK1, "loop", [100] * 999)                   # 1,000 versions, gap 100
    short = _versions(PK2, "short", [100] * 998)                 # 999 versions
    at300 = _versions(PK3, "at300", [300] * 999)                 # median exactly 300: kept
    even_lo = _versions(PK4, "even", [298] * 500 + [301] * 500)  # 1,001 versions, median 299.5
    even_hi = _versions(PK5, "even", [299] * 500 + [301] * 500)  # median 300
    s, d = build(loop + short + at300 + even_lo + even_hi)
    assert fam(s, PK1, "loop") == "N5" and row(s, PK1, "loop")["versions"] == 1000
    assert fam(s, PK2, "short") == "repo"
    assert fam(s, PK3, "at300") == "repo"
    assert fam(s, PK4, "even") == "N5"
    assert fam(s, PK5, "even") == "repo"
    assert d["summary"]["archive_events"]["announcements"] == 1000 + 999 + 1000 + 1001 + 1001


def test_n6_deleted_tags_names_and_nip09_requests():
    s, d = build([
        ann(PK1, "a", D(9), extra=(["deleted", "true"],)),
        ann(PK2, "b", D(9), extra=(["status", "deleted"],)),
        ann(PK3, "c", D(9), name="DeLeTeD"),
        ann(PK4, "d", D(9), extra=(["deleted", "false"],)),
        ann(PK5, "e", D(9), extra=(["deleted", "true", "extra"],)),   # first two elements match
        ann(PK6, "f", D(9), extra=(["deleted"],)),
    ])
    assert [fam(s, p, x) for p, x in ((PK1, "a"), (PK2, "b"), (PK3, "c"), (PK4, "d"),
                                      (PK5, "e"), (PK6, "f"))] == [
        "N6", "N6", "N6", "repo", "N6", "repo"]


def test_n6_deletion_request_and_later_reannouncement():
    anns = [ann(PK1, "x", D(20)), ann(PK1, "x", D(10)),      # current version D(10)
            ann(PK2, "y", D(20)), ann(PK2, "y", D(10)),
            ann(PK3, "z", D(10)), ann(PK4, "w", D(10)), ann(PK5, "v", D(10)),
            ann(PK6, "u", D(10)), ann(PK7, "p", D(10))]
    dels = [
        dele(PK1, f"30617:{PK1}:x", D(10)),        # >= the current version: deleted
        dele(PK2, f"30617:{PK2}:y", D(15)),        # before the re-announcement: restarts
        dele(PK9, f"30617:{PK3}:z", D(5)),         # another author's: not NIP-09 (deletion requests) valid
        dele(PK4, f"30617:{PK4}:w", NOW + 60),     # future-dated: dropped by the horizon
        dele(PK5, f"30617:{PK5}:other", D(5)),     # a different coordinate
        # the horizon applies per TRIPLE: a future request cannot mask an earlier valid one
        dele(PK6, f"30617:{PK6}:u", D(5)), dele(PK6, f"30617:{PK6}:u", NOW + 60),
        dele(PK7, f"30617:{PK7}:p", N.PAST_FLOOR - 1),   # before the floor: dropped
        dele(PK1, f"30617:{PK1}:x", D(10)),        # a duplicate triple changes nothing
        # ref 1.2 (review 4 m1): a self-authored coord WITHOUT a second colon names no
        # repository, but its pair still counts; another author's does not; a bare "30617"
        # has no author at all
        dele(PK8, f"30617:{PK8}", D(5)),
        dele(PK9, f"30617:{PK8}", D(5)),
        dele(PK8, "30617", D(5)),
    ]
    s, d = build(anns + [ann(PK8, "q", D(10))], dels=dels)
    assert (fam(s, PK1, "x"), fam(s, PK6, "u")) == ("N6", "N6")
    assert [fam(s, p, x) for p, x in ((PK2, "y"), (PK3, "z"), (PK4, "w"), (PK5, "v"),
                                      (PK7, "p"), (PK8, "q"))] == ["repo"] * 6
    # the self-authored pairs with a surviving triple; deletions have no anomalies slot
    assert d["summary"]["archive_events"]["deletions"] == 5
    assert "deletions" not in d["anomalies"]["future_dated"]


def test_n7_n8a_n8b_n9_and_near_misses():
    s, d = build([
        ann(SK1, "a", D(9)),
        ann(PK1, "b", D(9), clone=("http://localhost:3000/b.git", "http://192.168.1.2/b.git")),
        ann(PK2, "c", D(9), clone=("http://localhost/c.git", GITHUB)),          # one public
        ann(PK3, "d", D(9), clone=("http://localhost/d.git", "not a url")),     # one lacks a host
        ann(PK4, "e", D(9), clone=()),                                          # no clone, no state
        ann(PK5, "f", D(9), clone=()),                                          # no clone, a state
        ann(PK6, "g", D(9), topics=("Personal-Fork ",)),                        # no state
        ann(PK7, "h", D(9), topics=("personal-fork",)),                         # with a state
        ann(PK8, "i", D(9), maint=(SK1,)),               # the well-known key only LISTED: kept
    ], states=[st(PK5, "f", D(8)), st(PK7, "h", D(8))])
    got = [fam(s, p, x) for p, x in ((SK1, "a"), (PK1, "b"), (PK2, "c"), (PK3, "d"),
                                     (PK4, "e"), (PK5, "f"), (PK6, "g"), (PK7, "h"), (PK8, "i"))]
    # PK3's pair (a private host and a hostless value) misses N8a but reaches nothing: N8b
    assert got == ["N7", "N8a", "repo", "N8b", "N8b", "repo", "N9", "repo", "repo"]
    assert d["summary"]["forks"] == 1                      # the personal fork with a state
    assert kv([{"k": e["k"], "v": e["v"]} for e in d["exclusions"]])["N9"] == 1


CATALOG = "62ec33a4090f0959e5c547a33f22a3253688bba121667a69e64a2dc758f8baaf"


def test_n10_excludes_the_nostr_net_catalog_by_its_key():
    """N10 (the operator's decision of 2026-10-12): every announcement signed by the
    nostr.net catalog key is excluded, a forge clone, a GRASP one and a state notwithstanding,
    as the last rule, so one with no public clone URL stays N8b and a stateless personal fork
    N9. It keys on the pubkey, never on the catalog's client tag, which another key's kept
    repository carries too; listing the key as a maintainer keeps an announcement. The
    held_keys_absent check then passes by itself."""
    assert N.NOSTR_NET_CATALOG_PUBKEY == CATALOG
    s, d = build([
        ann(CATALOG, "protocol-nips", D(9), clone=("https://github.com/nostr-protocol/nips.git",),
            extra=(("client", "nostr.net"), ("t", "protocol"))),
        ann(CATALOG, "relays-on-grasp", D(9), clone=(grasp_url(repo="g"),)),
        ann(CATALOG, "tools-pushed", D(9)),
        ann(CATALOG, "relays-nostr_relay", D(9), clone=(),
            web=("https://code.pobblelabs.org/fossil/nostr_relay/",)),
        ann(CATALOG, "tools-forked", D(9), topics=("personal-fork",)),
        ann(PK1, "Swarm-relay", D(9), extra=(("client", "nostr.net"),)),
        ann(PK2, "listed", D(9), maint=(CATALOG,)),
    ], states=[st(CATALOG, "tools-pushed", D(8))])
    got = [fam(s, p, x) for p, x in ((CATALOG, "protocol-nips"), (CATALOG, "relays-on-grasp"),
                                     (CATALOG, "tools-pushed"), (CATALOG, "relays-nostr_relay"),
                                     (CATALOG, "tools-forked"), (PK1, "Swarm-relay"),
                                     (PK2, "listed"))]
    assert got == ["N10", "N10", "N10", "N8b", "N9", "repo", "repo"]
    ledger = kv([{"k": e["k"], "v": e["v"]} for e in d["exclusions"]])
    assert (ledger["N10"], ledger["N8b"], ledger["N9"]) == (3, 1, 1)
    assert d["summary"]["repos"] == 2 and d["summary"]["maintainers"] == 2
    assert N.held_key_repos(s) == {}
    N.require_no_held_keys(s)
    assert N.check_invariants(s, d) == []


def test_ledger_first_match_wins_and_partition():
    s, d = build([
        ann(SK1, "a", D(9), clone=("https://nostr.life/x",)),                   # N1 before N7
        ann(PK1, "b", D(9), name="deleted", clone=()),                          # N6 before N8b
        ann(PK2, "20260101", D(9), name="<human-readable project name>"),      # N3 before N4
        ann(PK3, "k", D(9)),
    ])
    assert [fam(s, p, x) for p, x in ((SK1, "a"), (PK1, "b"), (PK2, "20260101"), (PK3, "k"))] == [
        "N1", "N6", "N3", "repo"]
    ann_rows = [e for e in d["exclusions"] if e["unit"] == "announcements"]
    assert [e["k"] for e in d["exclusions"]] == [
        "N0", "N1", "N2", "N3", "N4", "N5", "N6", "N7", "N8a", "N8b", "N9", "N10",
        "state_republish_loop", "orphan_states", "ephemeral", "imported", "mirrored"]
    assert [e["unit"] for e in d["exclusions"]][10:] == [
        "announcements", "announcements", "state coordinates", "state coordinates", "comments",
        "comments", "comments"]
    assert sum(e["v"] for e in ann_rows) + d["summary"]["announcements_kept"] == 4
    assert N.check_invariants(s, d) == []


# --- (b) repositories -----------------------------------------------------------------------

def test_reciprocal_components_one_way_and_excluded_members():
    s, d = build([
        ann(PK1, "r", D(30), maint=(PK2,)),
        ann(PK2, "r", D(20), maint=(PK1,)),        # reciprocal with PK1: one repository
        ann(PK3, "r", D(30), maint=(PK4,)),        # one-way: never merges
        ann(PK4, "r", D(20)),
        ann(PK5, "r", D(30), maint=(SK1,)),        # listed by an EXCLUDED coordinate
        ann(SK1, "r", D(20), maint=(PK5,)),
        ann(PK6, "other", D(30), maint=(PK1,)),    # a different d never joins
        ann(PK1, "other", D(30), maint=(PK6,)),
    ])
    sm = d["summary"]
    assert sm["announcements_kept"] == 7 and sm["repos"] == 5
    assert sm["multi_maintainer_repos"] == 2       # {PK1,PK2}:r and {PK1,PK6}:other
    assert sm["shared_listed_repos"] == 3          # + the one-way {PK3,PK4}
    assert row(s, PK2, "r")["repo_key"] == f"{PK1}:r" and row(s, PK1, "r")["is_lead"]
    assert not row(s, PK2, "r")["is_lead"]
    assert row(s, PK3, "r")["repo_key"] == f"{PK3}:r" and row(s, PK4, "r")["repo_key"] == f"{PK4}:r"
    assert row(s, PK5, "r")["repo_key"] == f"{PK5}:r" and row(s, SK1, "r")["repo_key"] is None
    assert sm["maintainers"] == 6
    assert N.check_invariants(s, d) == []


def test_role_mode_reciprocity_ended_and_reactivated_roles():
    s, d = build([
        ann(PK1, "r", D(30), extra=(["M", PK1], ["m", PK2])),
        ann(PK2, "r", D(20), extra=(["m", PK1, "1", "2"],)),        # ended: no join
        ann(PK3, "q", D(30), extra=(["M", PK3], ["m", PK4])),
        ann(PK4, "q", D(20), extra=(["m", PK3, "1", "2", "3"],)),   # re-activated: joins
    ])
    assert row(s, PK2, "r")["repo_key"] == f"{PK2}:r"
    assert row(s, PK4, "q")["repo_key"] == f"{PK3}:q"
    assert d["summary"]["repos"] == 3


def test_lead_rule_self_m_earliest_first_seen_and_pubkey_tie():
    s, _ = build([
        # unique self-M wins over the earlier member
        ann(PK1, "a", D(30), maint=None, extra=(["m", PK2],)),
        ann(PK2, "a", D(10), extra=(["M", PK2], ["m", PK1])),
        # two self-Ms: the earliest FIRST seen, although it has the higher pubkey and the later
        # current version (an OLDER version counts)
        ann(PK4, "b", D(40), extra=(["M", PK4], ["m", PK3])),
        ann(PK4, "b", D(5), extra=(["M", PK4], ["m", PK3])),
        ann(PK3, "b", D(30), extra=(["M", PK3], ["m", PK4])),
        # equal first seen: the lowest pubkey
        ann(PK6, "c", D(20), maint=(PK5,)),
        ann(PK5, "c", D(20), maint=(PK6,)),
    ])
    assert row(s, PK1, "a")["repo_key"] == f"{PK2}:a" and row(s, PK2, "a")["is_lead"]
    assert row(s, PK3, "b")["repo_key"] == f"{PK4}:b" and row(s, PK4, "b")["is_lead"]
    assert row(s, PK6, "c")["repo_key"] == f"{PK5}:c"


def test_union_order_name_euc_and_fork_families():
    s, d = build([
        ann(PK1, "r", D(30), name="Lead", maint=(PK2,), clone=(GITHUB,), topics=("rust",)),
        ann(PK2, "r", D(20), name="Other", maint=(PK1,), clone=(grasp_url(),),
            extra=(["r", C1, "euc"],)),
        ann(PK3, "f", D(25), name="Fork", extra=(["r", C1],)),     # the v1.0-1.2 bare form
        ann(PK4, "g", D(22), name="", extra=(["r", C2, "euc"],)),
    ])
    assert d["summary"]["fork_families"] == 1
    (fam_row,) = d["fork_families_top"]
    assert fam_row == {"euc": C1, "repos": 2, "names": ["Lead", "Fork"]}
    lead = [r for r in d["repos_active"] + d["repos_new"] if r["d"] == "r"][0]
    assert lead["name"] == "Lead" and lead["cls"] == "grasp_mixed"
    assert lead["hosts"] == ["github.com", "relay.ngit.dev"]           # lead first
    assert lead["host_kinds"] == ["grasp", "forge"]                     # HOST_KINDS order, not hosts'
    assert row(s, PK1, "r")["euc"] is None and row(s, PK2, "r")["euc"] == C1


# --- (b) the push ledger --------------------------------------------------------------------

def test_push_rule_republish_head_only_peeled_and_the_wall():
    W = N.STATE_WALL
    anns = [ann(PK1, "r", D(300)), ann(PK2, "n", D(300))]
    states = [
        st(PK1, "r", W - 2 * DAY, {"refs/heads/main": C1}),                  # first, pre-wall
        st(PK1, "r", W - DAY, {"refs/heads/main": C2}),                      # pre-wall changed pair
        st(PK1, "r", D(40), {"refs/heads/main": C2}),                        # identical republish
        st(PK1, "r", D(39), {"refs/heads/main": C2}, head="ref: refs/heads/dev"),   # HEAD only
        st(PK1, "r", D(38), {"refs/heads/main": C2, "refs/tags/v1^{}": C3},
           head="ref: refs/heads/dev"),                                      # peeled only
        st(PK1, "r", D(37), {"refs/heads/main": C3, "refs/tags/v1": C1}),    # a push
        st(PK2, "n", D(20), {"refs/heads/main": C1, "refs/heads/dev": C2, "refs/tags/v1": C3}),
    ]
    s, d = build(anns, states)
    rows = sorted(pushes_of(s, PK1, "r"), key=lambda r: r["created_at"])
    assert [r["changed"] for r in rows] == [True, True, False, False, False, True]
    assert [r["push"] for r in rows] == [False, False, False, False, False, True]
    assert [r["head_changed"] for r in rows] == [False, False, False, True, False, True]
    last = rows[-1]
    assert (last["heads_moved"], last["default_moved"], last["tags_added"]) == (1, True, 1)
    (first_after,) = pushes_of(s, PK2, "n")
    assert first_after["push"]
    assert (first_after["branches_added"], first_after["tags_added"]) == (2, 1)
    assert (first_after["branches"], first_after["tags"]) == (2, 1)
    sm = d["summary"]
    assert sm["pushes_total"] == 2 and sm["repos_with_state"] == 2
    assert row(s, PK1, "r")["last_state"] == D(37) and row(s, PK1, "r")["last_push"] == D(37)
    assert N.check_invariants(s, d) == []


def test_pre_wall_only_repository_has_state_but_no_push():
    W = N.STATE_WALL
    s, d = build([ann(PK1, "r", D(300))],
                 [st(PK1, "r", W - 3 * DAY, {"refs/heads/main": C1}),
                  st(PK1, "r", W - 1, {"refs/heads/main": C2})])
    r = row(s, PK1, "r")
    assert r["last_state"] == W - 1 and r["last_push"] is None and r["pushes_total"] == 0
    assert d["summary"]["pushes_total"] == 0 and d["summary"]["newest_push"] is None
    assert kv(d["recency"])["older"] == 0 and sum(kv(d["recency"]).values()) == 1


def test_same_second_versions_follow_the_nip01_tie_break():
    s, _ = build([ann(PK1, "r", D(50))], [
        st(PK1, "r", D(10), {"refs/heads/main": C1}, eid="b" * 64),
        st(PK1, "r", D(10), {"refs/heads/main": C2}, eid="a" * 64),
    ])
    rows = pushes_of(s, PK1, "r")
    by_id = {r["event_id"][0]: r for r in rows}
    # "b" is earlier in version order (first version), "a" is current and changed vs "b"
    assert by_id["b"]["branches_added"] == 1 and by_id["a"]["heads_moved"] == 1


def test_there_is_no_daily_cap_every_push_counts():
    """ref 3: the per-repository cap of 50 pushes a UTC day was dropped on 2026-10-02 - 174
    pushes in one day (the busiest real repository-day) all count, everywhere a push counts,
    and the ledger has no pushes row."""
    day_x, day_y = D(5, 0), D(4, 0)
    states = [st(PK1, "r", day_x + i, {"refs/heads/main": f"{i:040x}"}) for i in range(174)]
    states += [st(PK1, "r", day_y + i, {"refs/heads/main": f"{1000 + i:040x}"}) for i in range(51)]
    s, d = build([ann(PK1, "r", D(60))], states)
    sm = d["summary"]
    assert sm["pushes_total"] == sm["pushes_7d"] == sm["pushes_30d"] == 225
    assert sum(w["pushes"] for w in d["pushes_weekly"]) == 225
    r = row(s, PK1, "r")
    assert (r["pushes_total"], r["pushes_30d"], r["pushes_90d"]) == (225, 225, 225)
    assert d["repos_active"][0]["pushes_90d"] == 225
    assert d["maintainers_top"][0]["pushes_90d"] == 225
    assert all("counted" not in x for x in s["push_rows"]) and "counted" not in N.PUSH_COLUMNS
    assert not any(e["unit"] == "pushes" for e in d["exclusions"])
    assert "daily_cap" not in N.EXCLUSION_KEYS
    assert N.check_invariants(s, d) == []


def test_a_repositorys_pushes_count_across_member_coordinates_without_a_cap():
    t0 = D(3, 0)
    anns = [ann(PK1, "r", D(60), maint=(PK2,)), ann(PK2, "r", D(59), maint=(PK1,))]
    states = [st(PK1, "r", t0 + 2 * i, {"refs/heads/main": f"{i:040x}"}) for i in range(30)]
    states += [st(PK2, "r", t0 + 2 * i + 1, {"refs/heads/main": f"{500 + i:040x}"}) for i in range(30)]
    s, d = build(anns, states)
    assert d["summary"]["pushes_total"] == 60 and row(s, PK1, "r")["pushes_total"] == 60
    assert d["summary"]["maintainers_active_30d"] == 2
    assert N.check_invariants(s, d) == []


def test_a_hashtree_repositorys_announcement_republishes_are_its_pushes():
    """ref 3 (2026-10-12): hashtree publishes a push as a kind-30064 (hashtree-roots), never a
    kind-30618 (repo-states), and republishes the announcement with each public push. Each
    push of a hashtree-class repository's member coordinate counts (versions less than
    HTREE_PUSH_GAP_S apart are one push), except the coordinate's first and any dated before
    STATE_WALL."""
    W, GAP = N.STATE_WALL, N.HTREE_PUSH_GAP_S
    HT = ("htree://npub1abc/h",)
    anns = [
        # a survivor from before the wall, a push just before it (exactly one gap before, so the
        # version AT the wall is a push of its own), one 60 days back (in 90 d, not 30 d), two in
        # 30 d
        ann(PK1, "h", W - 30 * DAY, clone=HT), ann(PK1, "h", W - GAP, clone=HT),
        ann(PK1, "h", W, clone=HT), ann(PK1, "h", D(60), clone=HT),
        ann(PK1, "h", D(20), clone=HT), ann(PK1, "h", D(3), clone=HT),
        # first seen after the wall: its first version is its creation, not a push
        ann(PK2, "new", D(10), clone=HT), ann(PK2, "new", D(5), clone=HT),
        # announced once and never republished
        ann(PK3, "once", D(40), clone=HT),
    ]
    s, d = build(anns)
    p = pushes_of(s, PK1, "h")
    assert [r["created_at"] for r in p] == [W, D(60), D(20), D(3)]
    assert all(r["source"] == N.SOURCE_HTREE and r["push"] and r["changed"] and not r["head_changed"]
               and not r["default_moved"] and r["branches"] is None and r["tags"] is None
               and r["heads_moved"] == r["branches_added"] == r["tags_added"] == r["tags_moved"] == 0
               for r in p)
    assert [r["created_at"] for r in pushes_of(s, PK2, "new")] == [D(5)]
    assert pushes_of(s, PK3, "once") == []
    h = row(s, PK1, "h")
    assert (h["cls"], h["has_state"], h["last_state"], h["last_push"]) == ("hashtree", False, None, D(3))
    assert (h["pushes_total"], h["pushes_30d"], h["pushes_90d"]) == (4, 2, 3)
    sm = d["summary"]
    # pushes_hashtree_30d is a 30-day count: the push 60 days back is in pushes_total only
    assert (sm["pushes_total"], sm["pushes_30d"], sm["pushes_7d"], sm["pushes_hashtree_30d"]) == (5, 3, 2, 3)
    assert (sm["repos_pushed_30d"], sm["repos_pushed_90d"], sm["repos_with_state"]) == (2, 2, 0)
    assert (sm["maintainers_active_30d"], sm["newest_state"], sm["newest_push"]) == (2, None, N.day_str(D(3)))
    # the wall's own week holds the push AT the wall; none from the second before it
    assert {e["w"]: e["pushes"] for e in d["pushes_weekly"] if e["pushes"]}[
        N.week_monday(N.utc_date(W)).isoformat()] == 1
    # the lists: a hashtree repository has pushes but no state, so no branch or tag counts;
    # Anatomy reads states only and holds nothing here
    active = {r["d"]: r for r in d["repos_active"]}
    assert set(active) == {"h", "new"}
    assert (active["h"]["last_state"], active["h"]["branches"], active["h"]["tags"]) == (None, None, None)
    assert active["h"]["last_push"] == N.day_str(D(3)) and active["h"]["host_kinds"] == ["hashtree"]
    assert [(m["pubkey"], m["pushes_90d"]) for m in d["maintainers_top"][:2]] == [(PK1, 3), (PK2, 1)]
    assert sum(e["v"] for e in d["branches_per_repo"]) == 0
    assert N.check_invariants(s, d) == []


def test_only_the_hashtree_class_counts_republishes_and_its_states_count_too():
    """ref 3: today's hosting class decides (4.4). GRASP and hashtree is grasp_mixed, whose
    republishes are not pushes (a push sent to both counts once, as its state); a hashtree
    repository with states of its own counts both; N9 drops a hashtree fork with no state."""
    HT, GR = "htree://npub1abc/x", grasp_url()
    anns = [
        ann(PK1, "gm", D(30), clone=(GR, HT)), ann(PK1, "gm", D(20), clone=(GR, HT)),
        ann(PK2, "hf", D(30), clone=(HT, GITHUB)), ann(PK2, "hf", D(20), clone=(HT, GITHUB)),
        # hashtree-only once, GRASP-only now: none of its republishes counts
        ann(PK3, "was", D(30), clone=(HT,)), ann(PK3, "was", D(20), clone=(GR,)),
        ann(PK4, "both", D(30), clone=(HT,)), ann(PK4, "both", D(20), clone=(HT,)),
        ann(PK5, "fork", D(30), clone=(HT,), topics=("personal-fork",)),
        ann(PK5, "fork", D(20), clone=(HT,), topics=("personal-fork",)),
    ]
    states = [st(PK1, "gm", D(19), {"refs/heads/main": C1}),
              st(PK4, "both", D(25), {"refs/heads/main": C2})]
    s, d = build(anns, states)

    def sources(pubkey, d_):
        return sorted(r["source"] for r in pushes_of(s, pubkey, d_))

    assert (row(s, PK1, "gm")["cls"], sources(PK1, "gm")) == ("grasp_mixed", [N.SOURCE_STATE])
    assert (row(s, PK2, "hf")["cls"], sources(PK2, "hf")) == ("hashtree", [N.SOURCE_HTREE])
    assert (row(s, PK3, "was")["cls"], sources(PK3, "was")) == ("grasp_only", [])
    assert sources(PK4, "both") == [N.SOURCE_HTREE, N.SOURCE_STATE]
    assert fam(s, PK5, "fork") == "N9" and sources(PK5, "fork") == []
    # last_state is the state's alone, last_push the newest push of either source
    b = row(s, PK4, "both")
    assert (b["last_state"], b["last_push"], b["pushes_total"]) == (D(25), D(20), 2)
    assert row(s, PK2, "hf")["last_state"] is None
    assert (d["summary"]["pushes_hashtree_30d"], d["summary"]["repos_with_state"]) == (2, 2)
    assert N.check_invariants(s, d) == []


def test_each_member_coordinates_first_version_is_its_creation():
    """ref 3: the rule is per member coordinate, so in a two-member hashtree repository each
    member's own first version is left out and each republish is that member's push."""
    HT = ("htree://npub1abc/m",)
    anns = [ann(PK1, "m", D(40), maint=(PK2,), clone=HT), ann(PK1, "m", D(10), maint=(PK2,), clone=HT),
            ann(PK2, "m", D(30), maint=(PK1,), clone=HT), ann(PK2, "m", D(9), maint=(PK1,), clone=HT)]
    s, d = build(anns)
    key = row(s, PK1, "m")["repo_key"]
    assert row(s, PK2, "m")["repo_key"] == key and d["summary"]["repos"] == 1
    assert sorted((r["pubkey"], r["created_at"]) for r in s["push_rows"]) == [(PK1, D(10)), (PK2, D(9))]
    assert {r["repo_key"] for r in s["push_rows"]} == {key}
    assert d["summary"]["maintainers_active_30d"] == 2
    assert N.check_invariants(s, d) == []


def test_a_pushs_republishes_less_than_the_gap_apart_count_once():
    """ref 3: hashtree republishes the announcement once per changed ref, one ref's version
    8-107 s after the one before, so versions less than HTREE_PUSH_GAP_S apart are one push;
    counting versions would count refs while capture is live and pushes when it is late (a late
    request gets only the newest version). A push's row is its first version in version order
    (created_at ascending, then id DESCENDING)."""
    GAP = N.HTREE_PUSH_GAP_S
    assert GAP == 120  # the reference's 120 s and the page's "two minutes"
    HT = ("htree://npub1abc/c",)
    t = D(10)
    anns = [
        ann(PK1, "c", D(40), clone=HT),                      # its creation
        # a push of three refs, 100 s apart: one push at D(20), though its last ref comes 200 s
        # after its first (the gap is between a version and the one before, not the first)
        ann(PK1, "c", D(20), clone=HT), ann(PK1, "c", D(20) + 100, clone=HT),
        ann(PK1, "c", D(20) + 200, clone=HT),
        # GAP - 1 after the version before joins its push; exactly GAP after starts a new one
        ann(PK1, "c", t, clone=HT), ann(PK1, "c", t + GAP - 1, clone=HT),
        ann(PK1, "c", t + 2 * GAP - 1, clone=HT),
        # a first push of two refs is the coordinate's first push: neither version counts
        ann(PK2, "f", D(30), clone=HT), ann(PK2, "f", D(30) + 50, clone=HT), ann(PK2, "f", D(8), clone=HT),
        # two versions in one second start a push: its row is the higher id
        ann(PK3, "t", D(40), clone=HT),
        ann(PK3, "t", D(5), clone=HT, eid="0" * 64), ann(PK3, "t", D(5), clone=HT, eid="f" * 64),
    ]
    s, d = build(anns)
    assert [r["created_at"] for r in pushes_of(s, PK1, "c")] == [D(20), t, t + 2 * GAP - 1]
    assert [r["created_at"] for r in pushes_of(s, PK2, "f")] == [D(8)]
    assert [(r["event_id"], r["created_at"]) for r in pushes_of(s, PK3, "t")] == [("f" * 64, D(5))]
    assert d["summary"]["pushes_hashtree_30d"] == 5 and row(s, PK1, "c")["pushes_total"] == 3
    assert N.check_invariants(s, d) == []


def _htree_world():
    """_world() plus a republish of its hashtree repository (PK3's `s`): one hashtree push."""
    anns, states, comments = _world_rows()
    return build(anns + [ann(PK3, "s", D(9), clone=("htree://npub1abc/s",))], states, comments=comments)


def _htree_row(s):
    return next(r for r in s["push_rows"] if r["source"] == N.SOURCE_HTREE)


@pytest.mark.parametrize("mutate, expect", [
    (lambda s, d: _htree_row(s).__setitem__("branches", 3),
     "a hashtree push row carries a state's flags or counts"),
    (lambda s, d: _htree_row(s).__setitem__("head_changed", True),
     "a hashtree push row carries a state's flags or counts"),
    (lambda s, d: _htree_row(s).__setitem__("tags_added", 1),
     "a hashtree push row carries a state's flags or counts"),
    (lambda s, d: next(r for r in s["repos_rows"] if r["d"] == "s").__setitem__("cls", "other_hosts"),
     "a hashtree push row on a repository whose class is not hashtree"),
    (lambda s, d: next(r for r in s["repos_rows"] if r["d"] == "s").__setitem__("last_state", D(9)),
     "ngit_repos last_state != the newest state row of ngit_pushes"),
    # a hashtree push at its coordinate's first version, or one less than a gap after another
    (lambda s, d: _htree_row(s).__setitem__(
        "created_at", next(r for r in s["repos_rows"] if r["d"] == "s")["first_seen"]),
     "a coordinate's hashtree push rows do not fit its versions"),
    (lambda s, d: s["push_rows"].append(dict(_htree_row(s), event_id="e" * 64,
                                              created_at=_htree_row(s)["created_at"] + 60)),
     "a coordinate's hashtree push rows do not fit its versions"),
    # as many pushes as versions: the first group is never a push
    (lambda s, d: next(r for r in s["repos_rows"] if r["d"] == "s").__setitem__("versions", 1),
     "a coordinate's hashtree push rows do not fit its versions"),
])
def test_invariants_catch_a_broken_hashtree_push(mutate, expect):
    s, d = _htree_world()
    assert N.check_invariants(s, d) == [] and d["summary"]["pushes_hashtree_30d"] == 1
    s, d = copy.deepcopy(s), copy.deepcopy(d)
    mutate(s, d)
    problems = N.check_invariants(s, d)
    assert any(expect in p for p in problems), problems


def test_windows_are_complete_utc_days_ending_on_as_of_day():
    states = [st(PK1, "r", t, {"refs/heads/main": f"{i:040x}"}) for i, t in enumerate(
        (D(30, 86399), D(29, 0), D(7, 86399), D(6, 0), D(0, 86399), D(-1, 60)))]
    s, d = build([ann(PK1, "r", D(60))], states)
    sm = d["summary"]
    assert sm["pushes_total"] == 5          # the push on the run's own day waits for the next run
    assert sm["pushes_30d"] == 4          # D(29) .. as_of_day, not D(30)
    assert sm["pushes_7d"] == 2           # D(6) and as_of_day
    assert row(s, PK1, "r")["pushes_90d"] == 5
    assert sm["newest_push"] == "2026-09-24"
    assert kv(d["recency"])["30d"] == 1
    assert N.check_invariants(s, d) == []


def test_weekly_series_wall_week_sunday_monday_and_zeros():
    W = N.STATE_WALL                       # Sunday 2026-06-14 03:22:39Z
    sun = D(4, 86399)                      # Sunday 2026-09-20T23:59:59Z
    mon = D(3, 0)                          # Monday 2026-09-21T00:00:00Z
    assert N.day_str(sun) == "2026-09-20" and N.day_str(mon) == "2026-09-21"
    states = [st(PK1, "r", t, {"refs/heads/main": f"{i:040x}"})
              for i, t in enumerate((W, W + 3600, sun, mon))]
    s, d = build([ann(PK1, "r", D(200))], states)
    weekly = d["pushes_weekly"]
    assert weekly[0] == {"w": "2026-06-08", "pushes": 2, "repos": 1, "maintainers": 1}
    assert weekly[-2]["w"] == "2026-09-14" and weekly[-2]["pushes"] == 1
    assert weekly[-1]["w"] == "2026-09-21" and weekly[-1]["pushes"] == 1
    assert len(weekly) == 16 and sum(w["pushes"] for w in weekly) == 4
    assert weekly[1] == {"w": "2026-06-15", "pushes": 0, "repos": 0, "maintainers": 0}
    cov = d["coverage"]
    assert cov["partial_first_week"] == "2026-06-08" and cov["partial_week"] == "2026-09-21"
    assert cov["partial_month"] == "2026-09"
    assert N.check_invariants(s, d) == []


def test_sunday_as_of_day_and_a_push_on_the_runs_own_monday_waits():
    now = 1790565300                       # Monday 2026-09-28T03:15:00Z; as_of_day Sunday 09-27
    states = [st(PK1, "r", 1790510400 + 60, {"refs/heads/main": C1}),     # Sunday 09-27
              st(PK1, "r", 1790553600 + 600, {"refs/heads/main": C2})]    # Monday 09-28, today
    s, d = build([ann(PK1, "r", D(60))], states, now=now)
    assert d["as_of_day"] == "2026-09-27" and d["coverage"]["partial_week"] is None
    assert d["pushes_weekly"][-1]["w"] == "2026-09-21"
    # every weekly series ends with the week holding as_of_day, the Sunday's week
    assert (d["new_repos_weekly"][-1]["w"] == d["comments_weekly"][-1]["w"]
            == d["grasp_lists"]["per_week"][-1]["w"] == "2026-09-21")
    assert d["summary"]["pushes_total"] == 1                       # Monday's push waits
    assert sum(w["pushes"] for w in d["pushes_weekly"]) == 1
    assert N.check_invariants(s, d) == []


def test_month_boundary_new_repos_monthly_and_cum_repos():
    now = 1790824500                       # 2026-10-01T03:15:00Z; as_of_day 2026-09-30
    s, d = build([
        ann(PK1, "a", 1788220799),         # 2026-08-31T23:59:59Z
        ann(PK2, "b", 1788220800),         # 2026-09-01T00:00:00Z
        ann(PK3, "c", 1790820000),         # 2026-10-01T02:00:00Z - today, after as_of_day
        ann(PK4, "d", 1788220800 - 40 * DAY, clone=(grasp_url(),)),
    ], now=now)
    assert d["as_of_day"] == "2026-09-30" and d["coverage"]["partial_month"] is None
    months = {e["m"]: e for e in d["new_repos_monthly"]}
    assert months["2026-08"]["v"] == 1 and months["2026-09"]["v"] == 1
    assert months["2026-07"] == {"m": "2026-07", "v": 1, "grasp": 1, "other": 0, "s": 1}
    assert "2026-10" not in months and d["summary"]["repos"] == 3   # 10-01 waits for the next run
    assert d["cum_repos"][-1] == {"d": "2026-09-30", "v": 3}
    assert d["cum_repos"][0] == {"d": "2026-07-23", "v": 1}
    assert N.check_invariants(s, d) == []


def _ts(y, m, d, sec=0):
    """The unix timestamp `sec` seconds after midnight UTC of a date."""
    return int(dt.datetime(y, m, d, tzinfo=dt.timezone.utc).timestamp()) + sec


def by_host(**counts):
    """A by_host list as ref 7 writes it: every host kind in order, zeros included."""
    return [{"k": k, "v": counts.get(k, 0)} for k in N.HOST_KINDS]


def test_new_repos_weekly_starts_with_the_announcements_wall_week():
    """ref 7: ISO weeks from the week holding ANN_WALL (2026-04-27) to as_of_day's week,
    zero-filled; a repository or maintainer first seen before that week is in no week, and one
    first seen in it before the wall instant counts there."""
    assert N.day_str(N.ANN_WALL) == "2026-05-02"
    s, d = build([
        ann(PK1, "a", _ts(2026, 4, 26, 86399)),                # the Sunday before: no week
        ann(PK1, "b", _ts(2026, 4, 27)),                       # the wall week's Monday, pre-wall
        ann(PK2, "c", N.ANN_WALL, clone=(grasp_url(),)),       # the wall instant, GRASP-hosted
        ann(PK2, "e", _ts(2026, 9, 21)),                       # PK2's second repository: no s
        ann(PK3, "f", _ts(2026, 9, 24, 86399)),                # as_of_day's last second
    ])
    nrw = d["new_repos_weekly"]
    assert nrw[0] == {"w": "2026-04-27", "v": 2, "grasp": 1, "other": 1, "s": 1,
                      "by_host": by_host(grasp=1, forge=1)}
    assert nrw[1] == {"w": "2026-05-04", "v": 0, "grasp": 0, "other": 0, "s": 0,
                      "by_host": by_host()}
    assert nrw[-1] == {"w": "2026-09-21", "v": 2, "grasp": 0, "other": 2, "s": 1,
                       "by_host": by_host(forge=2)}
    assert len(nrw) == 22 and sum(e["v"] for e in nrw) == 4       # PK1's "a" is in no week
    assert sum(e["s"] for e in nrw) == 2                          # nor is PK1, first seen with it
    assert sum(e["v"] for e in d["new_repos_monthly"]) == d["summary"]["repos"] == 5
    assert N.check_invariants(s, d) == []


def test_new_repos_weekly_by_host_counts_a_repository_once_per_kind_of_host_it_is_on():
    """ref 7: by_host counts a week's repositories once in EVERY kind of public host they are on
    today - R of ref 4.4 - so a GRASP server and a GitHub mirror count under both, and the
    entries can sum past v; none is R empty. Every kind is present, in order, zeros included;
    grasp equals the row's grasp and none the week's class none. The counts differ per kind on
    purpose, so no two kinds can trade values unnoticed. (The week of first seen against
    today's R, and R over every member, are the next two tests.)"""
    assert N.HOST_KINDS == ("grasp", "hashtree", "forge", "self_hosted", "none")
    selfh = "https://git.example.org/x.git"
    s, d = build([
        ann(pk(40), "g_only", D(9), clone=(grasp_url(),)),
        ann(pk(41), "g_only2", D(9), clone=(grasp_url(repo="b"),)),
        ann(pk(42), "g_forge", D(9), clone=(grasp_url(), GITHUB)),                 # grasp + forge
        ann(pk(43), "g_ht", D(9), clone=(grasp_url(), "htree://npub1abc/g")),      # grasp + hashtree
        ann(pk(44), "ht", D(9), clone=("htree://npub1abc/ht",)),
        ann(pk(45), "ht_self", D(9), clone=("htree://npub1abc/x", selfh)),          # hashtree + self
        ann(pk(46), "forge", D(9)),
        ann(pk(47), "forge2", D(9)),
        ann(pk(48), "forge3", D(9)),
        ann(pk(49), "self_forge", D(9), clone=(selfh, GITHUB)),                     # self + forge
        ann(pk(50), "nonnet", D(9), clone=("nostr://npub1abc/relay.example/x",)),   # R empty
        # first seen in the next week as a forge mirror, on a GRASP server too today
        ann(pk(51), "moved", D(2), clone=(GITHUB,)),
        ann(pk(51), "moved", D(1), clone=(grasp_url(repo="m"), GITHUB)),
        ann(pk(52), "self2", D(2), clone=(selfh,)),
    ], [st(pk(50), "nonnet", D(8))])
    weeks = {e["w"]: e for e in d["new_repos_weekly"]}
    a, b = weeks["2026-09-14"], weeks["2026-09-21"]
    assert a["by_host"] == by_host(grasp=4, hashtree=3, forge=5, self_hosted=2, none=1)
    assert (a["v"], a["grasp"]) == (11, 4) and sum(c["v"] for c in a["by_host"]) == 15
    assert b["by_host"] == by_host(grasp=1, forge=1, self_hosted=1)
    assert (b["v"], b["grasp"]) == (2, 1)
    assert weeks["2026-09-07"]["by_host"] == by_host()
    for e in d["new_repos_weekly"]:
        assert [c["k"] for c in e["by_host"]] == list(N.HOST_KINDS)
        assert e["by_host"][0]["v"] == e["grasp"]
        assert sum(c["v"] for c in e["by_host"]) >= e["v"]
    # the substrate classes of week a: the bounds the invariants hold by_host to
    cls = {r["d"]: r["cls"] for r in s["repos_rows"] if r["family"] == "repo"}
    assert (cls["g_forge"], cls["g_ht"], cls["ht_self"], cls["self_forge"], cls["nonnet"]) == (
        "grasp_mixed", "grasp_mixed", "hashtree", "other_hosts", "none")
    assert N.check_invariants(s, d) == []


def test_by_host_bounds_each_catch_a_broken_week_alone():
    """The invariants hold by_host to the week's hosting classes from below and above. Each
    broken document here leaves every other bound and identity holding, so each of the six
    sides is shown to catch alone what it exists for; the terms inside a side are the next
    two tests."""
    selfh = "https://git.example.org/x.git"
    s, d = build([
        ann(pk(60), "f1", D(9)),                                              # forge_only
        ann(pk(61), "h1", D(9), clone=("htree://npub1abc/h", selfh, GITHUB)),  # hashtree class
        ann(pk(62), "o1", D(9), clone=(selfh,)),                               # other_hosts
    ])
    wk = [e for e in d["new_repos_weekly"] if e["w"] == "2026-09-14"][0]
    assert wk["by_host"] == by_host(hashtree=1, forge=2, self_hosted=2) and wk["v"] == 3
    assert N.check_invariants(s, d) == []
    for kind, v in (("forge", 0), ("forge", 4), ("hashtree", 0), ("hashtree", 2),
                    ("self_hosted", 0), ("self_hosted", 3)):
        broken = copy.deepcopy(d)
        week = [e for e in broken["new_repos_weekly"] if e["w"] == "2026-09-14"][0]
        next(c for c in week["by_host"] if c["k"] == kind)["v"] = v
        assert N.check_invariants(s, broken) == [
            "a new_repos_weekly week's by_host is outside what its hosting classes allow"], (kind, v)


def _week(d, w):
    return next(e for e in d["new_repos_weekly"] if e["w"] == w)


def test_by_host_counts_todays_kinds_in_the_week_first_seen():
    """A repository first seen as a forge mirror and on a GRASP server too by its current
    version counts under both, in the week of its FIRST version, and in no later week."""
    s, d = build([ann(pk(70), "mv", D(9), clone=(GITHUB,)),
                  ann(pk(70), "mv", D(1), clone=(grasp_url(), GITHUB))])
    assert _week(d, "2026-09-14")["by_host"] == by_host(grasp=1, forge=1)
    assert _week(d, "2026-09-21")["by_host"] == by_host()
    assert N.check_invariants(s, d) == []


def test_by_host_takes_R_over_every_member_of_a_repository():
    """ref 2.3 / 4.4: R is over all the repository's clone values - co-maintainers who list
    each other are one repository, the lead on a forge and the other on GRASP: both kinds."""
    s, d = build([ann(pk(71), "u", D(9), clone=(GITHUB,), maint=(pk(72),)),
                  ann(pk(72), "u", D(8), clone=(grasp_url(),), maint=(pk(71),))])
    assert d["summary"]["repos"] == 1
    assert _week(d, "2026-09-14")["by_host"] == by_host(grasp=1, forge=1)
    assert N.check_invariants(s, d) == []


def test_by_host_bounds_hold_at_their_limits_and_count_out_the_classes_they_name():
    """Every upper bound met exactly is no failure (tightening any term would fail this real
    shape of week), and the forge upper bound counts out grasp_only and none."""
    selfh, ht = "https://git.example.org/x.git", "htree://npub1abc/x"
    nonnet = "nostr://npub1abc/relay.example/x"
    s, d = build([
        ann(pk(73), "gm", D(9), clone=(grasp_url(), ht, GITHUB, selfh)),   # grasp_mixed, every kind
        ann(pk(74), "ht", D(9), clone=(ht, GITHUB, selfh)),                # hashtree class, + forge + self
        ann(pk(75), "oh", D(9), clone=(selfh, GITHUB)),                    # other_hosts with a forge
        ann(pk(76), "fo", D(9)),                                           # forge_only
        ann(pk(77), "go", D(9), clone=(grasp_url(repo="g"),)),             # grasp_only
        ann(pk(78), "no", D(9), clone=(nonnet,)),                          # none, kept by its state
    ], [st(pk(78), "no", D(8))])
    w = _week(d, "2026-09-14")
    assert w["by_host"] == by_host(grasp=2, hashtree=2, forge=4, self_hosted=3, none=1) and w["v"] == 6
    assert N.check_invariants(s, d) == []
    s, d = build([ann(pk(80), "fo", D(9)), ann(pk(81), "go", D(9), clone=(grasp_url(),)),
                  ann(pk(82), "no", D(9), clone=(nonnet,))], [st(pk(82), "no", D(8))])
    assert _week(d, "2026-09-14")["by_host"] == by_host(grasp=1, forge=1, none=1)
    # forge above v - grasp_only - none = 1 (at or below v = 3); hashtree above its upper bound,
    # hashtree + grasp_mixed = 0, while within grasp_only: a grasp_only repository has no hashtree
    for kind, v in (("forge", 2), ("forge", 3), ("hashtree", 1)):
        broken = copy.deepcopy(d)
        next(c for c in _week(broken, "2026-09-14")["by_host"] if c["k"] == kind)["v"] = v
        assert N.check_invariants(s, broken) == [
            "a new_repos_weekly week's by_host is outside what its hosting classes allow"], (kind, v)


def test_comments_weekly_starts_with_the_comments_wall_week_distinct_per_week():
    """ref 6.1: ISO weeks from the week holding COMMENT_WALL (2026-06-15) to as_of_day's week;
    a comment before that week is in no week, one in it before the wall instant counts there;
    authors and threads are distinct within the week, and a comment without E is no thread."""
    assert N.day_str(N.COMMENT_WALL) == "2026-06-18"
    coord = f"30617:{PK1}:r"
    cs = [
        comment(PK2, _ts(2026, 6, 14, 86399)),                    # the Sunday before: no week
        comment(PK2, _ts(2026, 6, 15), K="1617", E="e1" * 32),    # the wall week's Monday
        comment(PK2, N.COMMENT_WALL, K="1617", E="e1" * 32),      # same author, same thread
        comment(PK3, _ts(2026, 6, 21, 86399), K="1618", E=None),  # its Sunday: no thread
        comment(PK4, _ts(2026, 9, 21), K="30617"),
        comment(PK4, _ts(2026, 9, 24, 86399), K="1", A=coord),    # as_of_day's last second
    ]
    s, d = build([ann(PK1, "r", D(300))], comments=cs)
    roots = ("issues", "prs", "patches", "repo", "other")
    cw = d["comments_weekly"]
    assert cw[0] == {"w": "2026-06-15", "issues": 0, "prs": 1, "patches": 2, "repo": 0,
                     "other": 0, "authors": 2, "threads": 1}
    assert cw[1] == {"w": "2026-06-22", **{k: 0 for k in roots}, "authors": 0, "threads": 0}
    assert cw[-1] == {"w": "2026-09-21", "issues": 0, "prs": 0, "patches": 0, "repo": 1,
                      "other": 1, "authors": 1, "threads": 1}
    assert len(cw) == 15 and sum(e[k] for e in cw for k in roots) == 5
    assert d["summary"]["comments_total"] == 6 and d["comments_monthly"][0]["m"] == "2026-06"
    assert N.check_invariants(s, d) == []


def test_grasp_lists_per_week_starts_with_the_states_wall_week():
    """ref 5.2: users by the ISO week of their earliest archived list version, from the week
    holding STATE_WALL (2026-06-08); a user first seen before that week is in no week, so the
    series sums to grasp_list_users minus those users."""
    g = lambda p, t, vals: {"id": nid(), "pubkey": p, "kind": 10317, "created_at": t,   # noqa: E731
                            "content": "", "tags": [["g", v] for v in vals], "sig": "0" * 128}
    lists = [
        g(PK1, _ts(2026, 6, 7, 86399), ["wss://relay.ngit.dev"]),       # the Sunday before
        g(PK1, D(5), ["wss://relay.ngit.dev", "wss://gitnostr.com"]),   # a later version
        g(PK2, _ts(2026, 6, 8), ["wss://relay.ngit.dev"]),              # the wall week, pre-wall
        g(PK3, _ts(2026, 9, 24, 86399), ["wss://gitnostr.com"]),        # as_of_day's last second
        # an empty first list (nothing valid) still dates the user: PK4 is first seen in the
        # week of 2026-06-15, though its first VALID list is in the week of 2026-09-14
        g(PK4, _ts(2026, 6, 16), ["wss://localhost"]),
        g(PK4, D(5), ["wss://relay.ngit.dev"]),
    ]
    s, d = build([ann(PK1, "r", D(9), clone=(grasp_url(),))], grasp=lists)
    gl = d["grasp_lists"]
    assert gl["per_week"][0] == {"w": "2026-06-08", "v": 1}
    assert gl["per_week"][1] == {"w": "2026-06-15", "v": 1}           # PK4, by its empty list
    assert {e["w"]: e["v"] for e in gl["per_week"]}["2026-09-14"] == 0
    assert gl["per_week"][-1] == {"w": "2026-09-21", "v": 1} and len(gl["per_week"]) == 16
    assert gl["users"] == 4 and sum(e["v"] for e in gl["per_week"]) == 3
    assert sum(e["v"] for e in gl["per_month"]) == 4
    assert N.check_invariants(s, d) == []


def test_a_monday_as_of_day_ends_every_weekly_series_with_its_own_week():
    """A run on a Tuesday: as_of_day is the Monday 2026-09-28, so every weekly series ends with
    that one-day week, flagged partial; rows of the run's own Tuesday wait."""
    now = _ts(2026, 9, 29, 3 * 3600 + 900)              # Tuesday 2026-09-29T03:15:00Z
    g = {"id": nid(), "pubkey": PK2, "kind": 10317, "created_at": _ts(2026, 9, 28, 600),
         "content": "", "tags": [["g", "wss://relay.ngit.dev"]], "sig": "0" * 128}
    s, d = build(
        [ann(PK1, "r", D(60)), ann(PK3, "monday", _ts(2026, 9, 28, 43200)),
         ann(PK4, "tuesday", _ts(2026, 9, 29, 600))],
        [st(PK1, "r", _ts(2026, 9, 28, 60), {"refs/heads/main": C1}),
         st(PK1, "r", _ts(2026, 9, 29, 60), {"refs/heads/main": C2})],
        grasp=[g],
        comments=[comment(PK5, _ts(2026, 9, 28, 3600)), comment(PK5, _ts(2026, 9, 29, 60))],
        now=now)
    assert d["as_of_day"] == "2026-09-28" and d["coverage"]["partial_week"] == "2026-09-28"
    for series in (d["pushes_weekly"], d["new_repos_weekly"], d["comments_weekly"],
                   d["grasp_lists"]["per_week"]):
        assert series[-1]["w"] == "2026-09-28"
    assert d["pushes_weekly"][-1]["pushes"] == 1                    # Tuesday's push waits
    assert d["new_repos_weekly"][-1]["v"] == 1                      # Tuesday's repository too
    assert d["comments_weekly"][-1]["issues"] == 1
    assert d["grasp_lists"]["per_week"][-1]["v"] == 1
    assert N.check_invariants(s, d) == []


def test_push_concentration_warns_past_half_of_a_weeks_pushes_from_100_on():
    """The non-blocking push_concentration_sane check: the busiest repository's share of the
    push rows in the 7 days to as_of_day, failing above half once there are 100 of them."""
    def run(a_pushes, b_pushes, *, a_day=2):
        states = [st(PK1, "a", D(a_day, i), {"refs/heads/main": f"{i:040x}"}) for i in range(a_pushes)]
        states += [st(PK2, "b", D(3, i), {"refs/heads/main": f"{5000 + i:040x}"}) for i in range(b_pushes)]
        s, d = build([ann(PK1, "a", D(60)), ann(PK2, "b", D(60))], states)
        return N.push_concentration(s, d)
    # 51 of 100: more than half
    c = run(51, 49)
    assert (c["top_repo"], c["top_pushes"], c["pushes_7d"], c["passed"]) == (f"{PK1}:a", 51, 100, False)
    assert c["share"] == 0.51
    # exactly half passes; so does any share below the floor of 100 pushes
    assert run(50, 50)["passed"] and run(99, 0)["passed"] and run(98, 1)["passed"]
    # a repository's pushes older than the 7 days do not count: 7 d = as_of_day and 6 before
    old = run(60, 40, a_day=7)
    assert (old["top_repo"], old["pushes_7d"], old["passed"]) == (f"{PK2}:b", 40, True)
    assert not run(60, 40, a_day=6)["passed"]
    # no pushes at all: nothing to warn about
    s, d = build([ann(PK1, "a", D(60))])
    assert N.push_concentration(s, d) == {"top_repo": None, "top_pushes": 0, "pushes_7d": 0,
                                          "share": 0.0, "passed": True}


def test_state_republish_loop_is_dropped_from_the_ledger():
    t0 = D(100)
    loop = [st(PK1, "r", t0 + 60 * i, {"refs/heads/main": f"{i:040x}"}) for i in range(1000)]
    s, d = build([ann(PK1, "r", D(120))], loop + [st(PK9, "orphan", D(3))])
    loops = [e for e in d["exclusions"] if e["k"] == "state_republish_loop"][0]
    orphans = [e for e in d["exclusions"] if e["k"] == "orphan_states"][0]
    assert (loops["v"], orphans["v"]) == (1, 1)
    assert s["push_rows"] == [] and row(s, PK1, "r")["has_state"]
    assert d["summary"]["repos_with_state"] == 0 and d["no_state"]["total"] == 1
    assert N.check_invariants(s, d) == []


# --- (b) hosting ----------------------------------------------------------------------------

def test_every_hosting_class_the_denylist_and_private_grasp():
    npub_path = f"{NPUB_SHAPE}/x.git"
    anns = [
        ann(pk(20), "g_only", D(9), clone=(grasp_url(),)),
        ann(pk(21), "g_mixed", D(9), clone=(grasp_url(), GITHUB)),
        ann(pk(22), "ht", D(9), clone=("htree://npub1abc/ht",)),
        ann(pk(23), "ht_self", D(9), clone=("htree://npub1abc/x", "https://git.example.org/x.git")),
        ann(pk(24), "forge", D(9), clone=(GITHUB, "git@codeberg.org:o/r.git")),
        ann(pk(25), "other", D(9), clone=("https://git.example.org/x.git",)),
        ann(pk(26), "nonnet", D(9), clone=("nostr://npub1abc/relay.example/x",)),
        ann(pk(27), "priv_forge", D(9), clone=("http://localhost/x.git", GITHUB)),
        ann(pk(28), "webui", D(9), clone=(f"https://gitworkshop.dev/{npub_path}",)),
        ann(pk(29), "dotless", D(9), clone=(f"http://relay-2:7334/{npub_path}",)),
        ann(pk(30), "port", D(9), clone=(f"http://grasp.example.net:7334/{npub_path}",)),
        ann(pk(31), "local_grasp", D(9), clone=(f"http://localhost:8081/{npub_path}", GITHUB)),
        ann(pk(32), "priv_only", D(9), clone=(f"http://127.0.0.1/{npub_path}",)),
        # a GRASP-shaped URL on a DOTTED private host never makes it a GRASP server
        ann(pk(33), "lan_grasp", D(9), clone=(f"http://192.168.1.10/{npub_path}", GITHUB)),
        # a hex-pubkey path on a GRASP host: class grasp although not GRASP-shaped
        ann(pk(34), "hexpath", D(9), clone=(f"https://relay.ngit.dev/{PK1}/x.git",
                                            "git@grasp.example.net:x.git")),
    ]
    # a repository with nothing reachable stays on the roster (class none) only with a state
    s, d = build(anns, [st(pk(26), "nonnet", D(8))])
    cls = {r["d"]: r["cls"] for r in s["repos_rows"] if r["family"] == "repo"}
    assert cls == {"g_only": "grasp_only", "g_mixed": "grasp_mixed", "ht": "hashtree",
                   "ht_self": "hashtree", "forge": "forge_only", "other": "other_hosts",
                   "nonnet": "none", "priv_forge": "forge_only", "webui": "other_hosts",
                   "dotless": "other_hosts", "port": "grasp_only", "local_grasp": "forge_only",
                   "lan_grasp": "forge_only", "hexpath": "grasp_only"}
    assert fam(s, pk(32), "priv_only") == "N8a"
    sm = d["summary"]
    assert sm["grasp_servers"] == 2 and sm["grasp_repos"] == 4
    assert sm["grasp_share"] == 4 / 14
    # every GRASP-hosted repository counts its GRASP-set hosts over ALL clone values
    assert kv(d["grasp_per_repo"]) == {"1": 3, "2": 1, "3": 0, "4+": 0}   # hexpath uses two
    # repos per server counts GRASP-SHAPED values only: the hex path does not add one
    assert {g["host"]: g["repos"] for g in d["grasp_servers"]} == {
        "relay.ngit.dev": 2, "grasp.example.net": 1}
    assert kv(d["hosting"]) == {"grasp_only": 3, "grasp_mixed": 1, "hashtree": 2,
                                "forge_only": 4, "other_hosts": 3, "none": 1}
    # the substrate's grasp flag is the hosting class (ref 4.4), not the URL shape
    assert row(s, pk(20), "g_only")["grasp"] and row(s, pk(34), "hexpath")["grasp"]
    assert not row(s, pk(28), "webui")["grasp"] and not row(s, pk(24), "forge")["grasp"]
    hosts = {h["host"]: h["cls"] for h in d["clone_hosts"]}
    assert hosts["gitworkshop.dev"] == "self_hosted" and hosts["localhost"] == "private"
    assert hosts["npub1abc"] == "hashtree"
    assert N.check_invariants(s, d) == []


def test_a_forge_host_serving_grasp_shaped_urls_never_joins_the_grasp_host_set():
    """ref 4.3 (review 4 M1): ONE announcement with a GRASP-shaped path on github.com once
    turned every GitHub-hosted repository into a GRASP-hosted one."""
    s, d = build([ann(PK1, "a", D(9), clone=(f"https://github.com/{NPUB_SHAPE}/a.git",)),
                  ann(PK2, "b", D(9), clone=(GITHUB,))])
    assert row(s, PK1, "a")["cls"] == row(s, PK2, "b")["cls"] == "forge_only"
    assert d["summary"]["grasp_servers"] == 0 and d["summary"]["grasp_repos"] == 0
    assert N.check_invariants(s, d) == []


def test_listed_relays_and_a_loopback_nip_io_name_never_join_the_grasp_host_set():
    """ref 4.3 (review 4 m2): general-purpose relays serve no git, and a nip.io name that
    embeds 127.0.0.1 is private (4.2); a real candidate beside them still joins."""
    relays = ("relay.damus.io", "nos.lol", "relay.primal.net", "relay.nostr.band",
              "relay.nostrcheck.me")
    anns = [ann(pk(80 + i), f"r{i}", D(9), clone=(f"wss://{h}/{NPUB_SHAPE}/r{i}",))
            for i, h in enumerate(relays)]
    anns += [ann(PK1, "lo", D(9), clone=(f"https://grasp.127.0.0.1.nip.io/{NPUB_SHAPE}/lo.git",
                                          GITHUB)),
             ann(PK2, "ok", D(9), clone=(grasp_url("grasp.example.net"),))]
    s, d = build(anns)
    assert [g["host"] for g in d["grasp_servers"]] == ["grasp.example.net"]
    assert d["summary"]["grasp_servers"] == 1
    assert {row(s, pk(80 + i), f"r{i}")["cls"] for i in range(len(relays))} == {"other_hosts"}
    assert row(s, PK1, "lo")["cls"] == "forge_only" and row(s, PK2, "ok")["cls"] == "grasp_only"
    assert {h["host"]: h["cls"] for h in d["clone_hosts"]}["grasp.127.0.0.1.nip.io"] == "private"
    assert N.check_invariants(s, d) == []
    # the admission rule itself, one clause at a time
    for u in (f"https://github.com/{NPUB_SHAPE}/x", f"wss://nos.lol/{NPUB_SHAPE}/x",
              f"https://gitworkshop.dev/{NPUB_SHAPE}/x", f"http://relay-2:7334/{NPUB_SHAPE}/x",
              f"http://10.0.0.1/{NPUB_SHAPE}/x", f"https://grasp.10-0-0-1.sslip.io/{NPUB_SHAPE}/x",
              "https://grasp.example.net/x/r.git", f"ssh://grasp.example.net/{NPUB_SHAPE}/x"):
        assert N.grasp_set_host(N.parse_url(u)) is None, u
    assert N.grasp_set_host(N.parse_url(grasp_url("grasp.example.net"))) == "grasp.example.net"


def test_hosts_shown_not_to_be_grasp_servers_never_join_the_grasp_host_set():
    """ref 4.3's third list: four hosts with GRASP-shaped clone URLs that are not GRASP servers,
    found by the probe of 2026-10-12 (#17 (/git: 5 of the 63 hosts in the GRASP host set are not
    GRASP servers and 14 are dead)); the probe's fifth, git.gittr.space, is a forge since the
    same day (the next test). Each reads self_hosted, so a repository on one
    alone is other_hosts and one also on a real GRASP server is grasp_mixed; the match is on the
    exact host, so gittr's relay, relay.gittr.space, still joins."""
    shown = ("git.jb55.com", "relay.vanderwarker.family", "gitrepublic.imwald.eu",
             "denimroad.feeds.relay.tools")
    assert N.GRASP_NOT_SERVER_DENYLIST == frozenset(shown)
    anns = [ann(pk(90 + i), f"n{i}", D(9), clone=(grasp_url(h),)) for i, h in enumerate(shown)]
    anns += [ann(pk(95 + i), f"m{i}", D(9), clone=(grasp_url(), grasp_url(h)))
             for i, h in enumerate(shown)]
    anns += [ann(PK1, "gittr", D(9), clone=(grasp_url("relay.gittr.space"),))]
    s, d = build(anns)
    assert {row(s, pk(90 + i), f"n{i}")["cls"] for i in range(len(shown))} == {"other_hosts"}
    assert {row(s, pk(95 + i), f"m{i}")["cls"] for i in range(len(shown))} == {"grasp_mixed"}
    assert row(s, PK1, "gittr")["cls"] == "grasp_only"
    assert sorted(g["host"] for g in d["grasp_servers"]) == ["relay.gittr.space", "relay.ngit.dev"]
    assert d["summary"]["grasp_servers"] == 2 and d["summary"]["grasp_repos"] == 5
    hosts = {h["host"]: h["cls"] for h in d["clone_hosts"]}
    assert {hosts[h] for h in shown} == {"self_hosted"}
    # every grasp_mixed repository counts ONE GRASP-set host: the listed host is not one
    assert kv(d["grasp_per_repo"]) == {"1": 5, "2": 0, "3": 0, "4+": 0}
    assert N.check_invariants(s, d) == []
    # the admission rule itself, under every GRASP scheme and in any letter case; the same
    # shapes on a host on no list do join, so the denials cannot pass vacuously
    assert N.GRASP_SCHEMES == frozenset({"http", "https", "ws", "wss"})
    for scheme in sorted(N.GRASP_SCHEMES):
        for h in shown + ("grasp.example.org",):
            for u in (f"{scheme}://{h}/{NPUB_SHAPE}/x.git", f"{scheme}://{h.upper()}:8080/{NPUB_SHAPE}/x"):
                want = "grasp.example.org" if h == "grasp.example.org" else None
                assert N.grasp_set_host(N.parse_url(u)) == want, u


def test_gittrs_git_server_is_a_forge():
    """ref 4.2, the operator's decision of 2026-10-12 on #17 (/git: 5 of the 63 hosts in the
    GRASP host set are not GRASP servers and 14 are dead): git.gittr.space, gittr's hosted git
    server, is a forge in its HTTPS form, GRASP-shaped or not, and in its scp-like SSH form, and
    a non-network value on it is a forge by the same precedence as on github.com (ref 4.2). So
    it never joins the GRASP host set; a repository on it alone or with
    another forge is forge_only, with a self-hosted server other_hosts, and with gittr's GRASP
    relay grasp_mixed; Clone hosts and by_host count it as a forge."""
    gittr = "git.gittr.space"
    assert gittr in N.FORGE_HOSTS and gittr not in N.GRASP_NOT_SERVER_DENYLIST
    https, scp = grasp_url(gittr), f"git@{gittr}:{NPUB_SHAPE}/x.git"
    for u in (https, scp, f"https://{gittr}/{'a' * 64}/x.git",
              f"https://{gittr.upper()}/{NPUB_SHAPE}/x.git", f"nostr://{gittr}/{NPUB_SHAPE}/x"):
        assert N.host_class(N.parse_url(u), frozenset()) == "forge", u
        assert N.grasp_set_host(N.parse_url(u)) is None, u
    repos = {"alone": (https,), "ssh": (scp,), "with_github": (https, GITHUB),
             "with_self": (https, "https://git.example.org/x.git"),
             "with_relay": (https, grasp_url("relay.gittr.space"))}
    s, d = build([ann(pk(60 + i), r, D(9), clone=c) for i, (r, c) in enumerate(repos.items())])
    assert {r: row(s, pk(60 + i), r)["cls"] for i, r in enumerate(repos)} == {
        "alone": "forge_only", "ssh": "forge_only", "with_github": "forge_only",
        "with_self": "other_hosts", "with_relay": "grasp_mixed"}
    assert [g["host"] for g in d["grasp_servers"]] == ["relay.gittr.space"]
    assert {h["host"]: h["cls"] for h in d["clone_hosts"]}[gittr] == "forge"
    week = {e["w"]: e for e in d["new_repos_weekly"]}["2026-09-14"]
    assert week["by_host"] == by_host(grasp=1, forge=5, self_hosted=1)
    assert N.check_invariants(s, d) == []


def test_relays_web_hosts_topics_and_tools():
    anns = [
        ann(PK1, "a", D(9), relays=("wss://Relay.ngit.dev/", "ws://nos.lol", "wss://localhost"),
            # ref 4.5 (review 4 m4): hosts that pass the private-host test are nobody's web host
            web=("https://gitworkshop.dev/x", "nostr://x.example/y", "http://127.0.0.1:3000/git/x",
                 "https://grasp.127.0.0.1.nip.io/x", "http://box.local/x"),
            topics=("Nostr", "shakespeare", "2026", "12345678", "123", ""),
            extra=(["client", "shakespeare.diy"],)),
        ann(PK2, "b", D(9), relays=("wss://relay.ngit.dev",), topics=("nostr",),
            extra=(["client", "NostrHub"],)),
        ann(PK3, "c", D(9), topics=("shakespeare",)),
        ann(PK4, "d", D(9), clone=("htree://npub1abc/d",)),
        ann(PK5, "e", D(9), extra=(["alt", "git repository: e"],)),
        ann(PK6, "f", D(9), extra=(["refs/heads/main", C1],)),
        ann(PK7, "g", D(9)),
    ]
    s, d = build(anns)
    assert d["relays"] == [{"relay": "relay.ngit.dev", "repos": 2}, {"relay": "nos.lol", "repos": 1}]
    assert kv(d["relays_per_repo"]) == {"0": 5, "1": 1, "2-3": 1, "4-5": 0, "6+": 0}
    assert d["web_hosts"] == [{"host": "gitworkshop.dev", "repos": 1}]
    assert {t["t"]: t["repos"] for t in d["topics"]} == {"nostr": 2, "123": 1}
    assert kv(d["tools"]) == {"Shakespeare": 2, "NostrHub": 1, "hashtree": 1,
                              "ngit / GitWorkshop (inferred)": 1,
                              "git-sync mirror (refs in announcement)": 1, "unknown": 1}
    assert row(s, PK2, "b")["tool"] == "NostrHub"   # `contains` on the LOWERCASED client
    assert N.check_invariants(s, d) == []


def test_tools_fold_all_but_the_five_most_frequent_clients():
    anns, i = [], 40
    # "C1" and "c1" are one label: other clients are labelled by their lowercased value
    for client, n in (("C1", 4), ("c1", 3), ("c2", 6), ("c3", 5), ("c4", 4), ("c5", 3),
                      ("c6", 2), ("c7", 1)):
        for j in range(n):
            anns.append(ann(pk(i), f"{client}-{j}", D(9), extra=(["client", client],)))
        i += 1
    s, d = build(anns)
    assert kv(d["tools"]) == {"c1": 7, "c2": 6, "c3": 5, "c4": 4, "c5": 3, "other client": 3}
    assert row(s, pk(47), "c7-0")["tool"] == "other client"
    assert row(s, pk(40), "C1-0")["tool"] == "c1"


def test_anatomy_from_the_newest_state():
    heads = {"main": "ref: refs/heads/main", "master": "ref: refs/heads/master",
             "other": "ref: refs/heads/trunk", "detached": C1, "none": None}
    anns, states = [], []
    for i, (k, h) in enumerate(heads.items()):
        anns.append(ann(pk(50 + i), k, D(30)))
        states.append(st(pk(50 + i), k, D(20), {"refs/heads/old": C1}, head="ref: refs/heads/old"))
        refs = {f"refs/heads/b{j}": C1 for j in range(i * 3)}
        refs.update({f"refs/tags/t{j}": C2 for j in range(i * 6)})
        states.append(st(pk(50 + i), k, D(10), refs, head=h))
    s, d = build(anns, states)
    assert kv(d["default_branch"]) == {"main": 1, "master": 1, "other": 1, "detached": 1, "none": 1}
    # branches 0 / 3 / 6 / 9 / 12, tags 0 / 6 / 12 / 18 / 24
    assert kv(d["branches_per_repo"]) == {"0": 1, "1": 0, "2-5": 1, "6-20": 3, "21+": 0}
    assert kv(d["tags_per_repo"]) == {"0": 1, "1-5": 0, "6-20": 3, "21+": 1}
    assert N.check_invariants(s, d) == []


# --- (b) maintainers ------------------------------------------------------------------------

def test_grasp_lists_validity_current_version_and_servers():
    g = lambda p, t, vals: {"id": nid(), "pubkey": p, "kind": 10317, "created_at": t,   # noqa: E731
                            "content": "", "tags": [["g", v] for v in vals], "sig": "0" * 128}
    lists = [
        g(PK1, D(9), ["wss://relay.ngit.dev/", "wss://gitnostr.com", "WSS://relay.ngit.dev"]),
        # only tag[1] of a `g` tag is a value: the second element is not a list entry
        {"id": nid(), "pubkey": PK4, "kind": 10317, "created_at": D(9), "content": "",
         "tags": [["g", "wss://relay.example.com", "wss://relay.ngit.dev"]], "sig": ""},
        g(PK2, D(9), ["wss://relay.example.com", "wss://localhost"]),     # nothing valid
        g(PK3, D(40), ["wss://gitnostr.com"]),                            # older version
        g(PK3, D(8), ["wss://cdn.hzrd149.com", "wss://relay.ngit.dev/x"]),  # current
    ]
    s, d = build([ann(PK1, "r", D(9), clone=(grasp_url(),))], grasp=lists)
    gl = d["grasp_lists"]
    assert gl["users"] == d["summary"]["grasp_list_users"] == 2
    assert kv(gl["length"]) == {"1": 0, "2": 2, "3": 0, "4+": 0}
    assert {e["host"]: e["v"] for e in gl["first_choice"]} == {"relay.ngit.dev": 1, "cdn.hzrd149.com": 1}
    assert {e["m"]: e["v"] for e in gl["per_month"]} == {"2026-08": 1, "2026-09": 1}
    (srv,) = d["grasp_servers"]
    assert srv == {"host": "relay.ngit.dev", "repos": 1, "users": 2, "first_choice": 1}
    assert N.check_invariants(s, d) == []


def test_maintainers_top_concentration_names_and_caps():
    anns = []
    for i in range(25):
        for j in range(i + 1):
            anns.append(ann(pk(60 + i), f"r{j}", D(9)))
    names = {pk(84): "  Big\x07Maintainer with a very long display name indeed  ", pk(83): ""}
    s, d = build(anns, names=names)
    top = d["maintainers_top"]
    assert len(top) == 20 and top[0]["pubkey"] == pk(84) and top[0]["repos"] == 25
    assert top[0]["name"] == "BigMaintainer with a very long display n"
    assert top[1]["name"] is None
    total = sum(range(1, 26))
    assert d["summary"]["concentration"]["top1_share"] == 25 / total
    assert d["summary"]["concentration"]["top10_share"] == sum(range(16, 26)) / total
    assert N.kept_maintainers(anns, [], [], NOW)[:2] == [pk(84), pk(83)]
    assert N.check_invariants(s, d) == []


def test_maintainers_top_is_cut_by_pushes_in_90_days_then_repositories():
    # ref 5.1: the list is the most active maintainers. A key announcing 25 repositories it
    # never pushes to ranks below a one-repository key that pushed; pushes older than the 90 d
    # window do not count; ties go to more repositories (PK5 with three before PK3 with one,
    # against pubkey order), then to the pubkey (PK6 before PK7).
    def pushes(pubkey, d, days):
        return [st(pubkey, d, D(n), {"refs/heads/main": f"{n:040x}"}) for n in days]

    anns = [ann(PK1, "busy", D(120))]
    anns += [ann(PK2, f"wide{i}", D(120)) for i in range(25)]
    anns += [ann(PK5, x, D(120)) for x in ("e", "e2", "e3")]
    anns += [ann(PK4, "old", D(120)), ann(PK3, "a", D(120)), ann(PK6, "f", D(120)),
             ann(PK7, "g", D(120))]
    anns += [ann(pk(60 + i), "idle", D(120)) for i in range(20)]
    states = (pushes(PK1, "busy", range(10, 15)) + pushes(PK3, "a", (20, 21))
              + pushes(PK5, "e", (30, 31)) + pushes(PK6, "f", (40,)) + pushes(PK7, "g", (41,))
              + pushes(PK4, "old", range(95, 100)))       # after the state wall, outside 90 d
    s, d = build(anns, states)
    top = d["maintainers_top"]
    assert len(top) == 20
    assert [(r["pubkey"], r["pushes_90d"], r["repos"]) for r in top[:7]] == [
        (PK1, 5, 1), (PK5, 2, 3), (PK3, 2, 1), (PK6, 1, 1), (PK7, 1, 1), (PK2, 0, 25), (PK4, 0, 1)]
    assert [r["pubkey"] for r in top[7:]] == [pk(60 + i) for i in range(13)]
    assert row(s, PK4, "old")["pushes_total"] == 5 and top[6]["last_push"] is not None
    assert N.check_invariants(s, d) == []


def test_maintainers_top_ranks_by_all_pushes_a_burst_day_included():
    # ref 5.1: pushes_90d is every push in 90 d (no daily cap since 2026-10-02). PK1 pushes 60
    # times in one day, PK2 55 times over two days: PK1 leads (under the old cap of 50 a UTC
    # day PK2 did).
    t1 = D(5, 3600)
    burst = [st(PK1, "burst", t1 + 60 * i, {"refs/heads/main": f"{i:040x}"}) for i in range(60)]
    spread = [st(PK2, "spread", D(6, 3600) + 60 * i, {"refs/heads/main": f"{100 + i:040x}"})
              for i in range(30)]
    spread += [st(PK2, "spread", D(7, 3600) + 60 * i, {"refs/heads/main": f"{200 + i:040x}"})
               for i in range(25)]
    s, d = build([ann(PK1, "burst", D(60)), ann(PK2, "spread", D(60))], burst + spread)
    top = d["maintainers_top"]
    assert [(r["pubkey"], r["pushes_90d"]) for r in top] == [(PK1, 60), (PK2, 55)]
    assert N.check_invariants(s, d) == []


def test_repo_rows_cap_hosts_and_lists():
    clones = tuple(f"https://h{i}.example.org/x.git" for i in range(8))
    anns = [ann(pk(100 + i), f"r{i}", D(9), clone=clones, topics=tuple(f"topic{j}" for j in range(30)))
            for i in range(30)]
    states = [st(pk(100 + i), f"r{i}", D(2)) for i in range(30)]
    s, d = build(anns, states)
    assert len(d["repos_active"]) == 25 and all(len(r["hosts"]) == 5 for r in d["repos_active"])
    assert len(d["topics"]) == 25 and len(d["clone_hosts"]) == 8
    assert all(len(r["hosts"]) == 8 for r in s["repos_rows"])
    assert len(d["repos_new"]) == 25
    assert N.check_invariants(s, d) == []


# --- (b) collaboration ----------------------------------------------------------------------

def test_comment_ledger_mirror_boundaries_roots_and_attribution():
    mirror = [comment(PK7, D(3), content=("**@alice** said" if i < 46 else "own")) for i in range(50)]
    at90 = [comment(PK8, D(3), content=("**@bob** said" if i < 45 else "own")) for i in range(50)]
    few = [comment(PK9, D(3), content="**@carol** said") for _ in range(49)]
    coord = f"30617:{PK1}:r"
    others = [
        comment(PK2, D(3), K="1621", E="e1" * 32, A=coord),
        comment(PK2, D(3), K="1618", E="e1" * 32, a=coord),
        comment(PK3, D(40), K="1619", E="e2" * 32),
        comment(PK3, D(3), K="1617", E=None),
        comment(PK4, D(3), K="30617", A=f"30617:{PK5}:gone"),     # not a member coordinate
        comment(PK4, D(3), K="1", A=coord),                        # admitted by its A tag: other
        comment(PK4, D(3), K="1621", extra=(["expiration", "1"],)),
        comment(PK4, D(3), K="1621", extra=(["imported", "github"],)),
        comment(PK7, D(3), K="1621", extra=(["expiration", "1"],)),  # ephemeral wins over mirror
        {"id": nid(), "pubkey": PK4, "kind": 1111, "created_at": D(3), "content": "",
         "tags": [["K", "1"], ["K", "1621"]], "sig": ""},          # the FIRST K is not a git kind
        comment(PK4, NOW + 5, K="1621"),                           # future-dated
    ]
    s, d = build([ann(PK1, "r", D(9))], comments=mirror + at90 + few + others)
    sm = d["summary"]
    ledger = {e["k"]: e["v"] for e in d["exclusions"] if e["unit"] == "comments"}
    assert ledger == {"ephemeral": 2, "imported": 1, "mirrored": 50}
    assert sm["archive_events"]["comments"] == 50 + 50 + 49 + 9     # admitted, after the horizon
    assert d["anomalies"]["future_dated"]["comments"] == 1
    assert sm["comments_total"] == 50 + 49 + 6
    last = d["comments_monthly"][-1]
    assert last["m"] == "2026-09"
    assert (last["issues"], last["prs"], last["patches"], last["repo"], last["other"]) == (
        50 + 49 + 1, 1, 1, 1, 1)
    assert sm["comments_30d"] == 104
    assert sm["comments_90d"] == 105          # and the D(40) one; none of the 53 in the ledger
    assert sm["comment_participants"] == 5
    assert sm["comment_threads"] == 3          # e1, e2 and the fixtures' default E
    assert sm["comments_attributable_share"] == 3 / 105
    assert d["repos_discussed"] == [{"pubkey": PK1, "d": "r", "name": "r", "cls": "forge_only", "comments": 3, "authors": 2}]
    assert N.check_invariants(s, d) == []


def test_comment_windows_are_complete_utc_days_ending_on_as_of_day():
    """ref 1.5 / 6.1: comments_30d and comments_90d count by the UTC date of created_at, as_of_day
    included; a comment older than 90 d stays in comments_total only."""
    stamps = (D(120), D(90, 86399), D(89, 0), D(30, 86399), D(29, 0), D(0, 86399), D(-1, 60))
    s, d = build([ann(PK1, "r", D(9))], comments=[comment(PK2, t) for t in stamps])
    sm = d["summary"]
    assert sm["comments_total"] == 6      # the comment on the run's own day waits for the next run
    assert sm["comments_90d"] == 4        # D(89) .. as_of_day, not D(90)
    assert sm["comments_30d"] == 2        # D(29) and as_of_day, not D(30)
    assert N.check_invariants(s, d) == []


def test_stars_dedupe_e_mapping_minus_and_malformed():
    old = ann(PK1, "r", D(30), maint=(PK2,))
    anns = [old, ann(PK1, "r", D(20), maint=(PK2,)), ann(PK2, "r", D(20), maint=(PK1,)),
            ann(PK3, "x", D(20), clone=("https://nostr.life/x",))]
    reactions = [
        reaction(PK9, D(5), a=f"30617:{PK1}:r"),
        reaction(PK9, D(5), content="", a=f"30617:{PK2}:r"),       # same reactor, same repository
        reaction(PK8, D(5), e=old["id"]),                          # mapped through an OLD version
        reaction(PK7, D(5), content="-", a=f"30617:{PK1}:r"),      # not a star
        reaction(PK6, D(5), a=f"30617:{PK1}", e=old["id"]),        # malformed a: no e fallback
        reaction(PK5, D(5), a=f"30617:{PK3}:x"),                   # an excluded coordinate
        reaction(PK4, D(5), content="+", e="0" * 64, k=None),      # out of scope entirely
    ]
    s, d = build(anns, reactions=reactions)
    sm = d["summary"]
    assert (sm["stars_total"], sm["stars_repos"]) == (2, 1)
    assert sm["archive_events"]["reactions"] == 6
    assert d["repos_starred"] == [{"pubkey": PK1, "d": "r", "name": "r", "cls": "forge_only", "stars": 2}]
    assert row(s, PK1, "r")["name"] == ""        # the display fallback is document-only


def test_external_reaction_scope_needs_k_git_repo_and_a_30617_i():
    """ref 6.2: the kind-17 (external-content-reactions) input is a `k` tag valued git-repo AND
    an `i` value starting 30617: - either alone is some other external reaction."""
    i = ["i", f"30617:{PK1}:r"]
    assert N.external_reaction_in_scope([["k", "git-repo"], i])
    assert N.external_reaction_in_scope([i, ["p", PK1], ["k", "web"], ["k", "git-repo"]])  # any k tag
    assert not N.external_reaction_in_scope([i])                                  # no k
    assert not N.external_reaction_in_scope([["k", "git-repo"]])                  # no i
    assert not N.external_reaction_in_scope([["k", "web"], i])                    # another k
    assert not N.external_reaction_in_scope([["k", "git-repo"], ["i", "podcast:guid:x"]])
    assert not N.external_reaction_in_scope([["k", "git-repo"], ["i", f" 30617:{PK1}:r"]])
    assert not N.external_reaction_in_scope([["k", "Git-Repo"], i])               # exact value
    assert not N.external_reaction_in_scope([["k"], ["i"], ["k", 7], ["i", None]])
    assert not N.external_reaction_in_scope([])
    # the target is the first i value starting 30617:, past any other i (ref 6.2)
    tags = [["i", "podcast:guid:x"], ["i", f"30617:{PK2}:a:b"], ["i", f"30617:{PK1}:r"]]
    assert N.repo_coordinate(tags, names=("i",)) == (PK2, "a:b")
    assert N.repo_coordinate([["i", f"30617:{PK1}"]], names=("i",)) is None


def test_iris_stars_are_each_keys_current_reaction_per_coordinate():
    """ref 6.2: an Iris Git star is the key's CURRENT kind-17 (external-content-reactions) per
    coordinate in version order (ref 1.4), content trimmed; a later "-" unstars, a later "+"
    stars again, a same-second tie falls to the lowest id; any member coordinate maps to its
    repository, and the first 30617 `i` value is the target."""
    anns = [ann(PK1, "r", D(30), maint=(PK2,)), ann(PK2, "r", D(30), maint=(PK1,)),
            ann(PK3, "x", D(30), clone=("https://nostr.life/x",)),          # N1: excluded
            ann(PK4, "t", D(30))]
    r1, r2, x, t = f"30617:{PK1}:r", f"30617:{PK2}:r", f"30617:{PK3}:x", f"30617:{PK4}:t"
    a, b, c, e, f, g, h, j, m, q = (pk(40 + n) for n in range(10))
    ext = [
        ext_reaction(a, D(9), i=r1), ext_reaction(a, D(8), content="-", i=r1),
        ext_reaction(a, D(7), i=r1),                                   # + - +: a star
        ext_reaction(b, D(9), i=r1), ext_reaction(b, D(8), content="-", i=r1),   # + -: none
        ext_reaction(c, D(9), content=" +\n", i=r1),                  # trimmed: a star
        ext_reaction(e, D(9), content="", i=r1),                       # empty: a star
        ext_reaction(f, D(9), content="x", i=r1),                      # neither: none
        ext_reaction(g, D(9), i=r2),                                   # the other member: a star
        # same second: the lowest id is the current reaction, in either order of content
        # (j's tie is on its own repository, so a reversed tie-break shows in the counts)
        ext_reaction(h, D(9), content="+", i=r1, eid="1" * 64),
        ext_reaction(h, D(9), content="-", i=r1, eid="2" * 64),        # h: a star
        ext_reaction(j, D(9), content="-", i=t, eid="3" * 64),
        ext_reaction(j, D(9), content="+", i=t, eid="4" * 64),         # j: none, t unstarred
        # per coordinate, not per repository: m's "-" on r2 leaves its star on r1
        ext_reaction(m, D(9), i=r1), ext_reaction(m, D(5), content="-", i=r2),
        ext_reaction(q, D(9), i=[x, r1]),                              # first 30617 i: excluded
        ext_reaction(q, D(9), i=f"30617:{PK9}:nope"),                  # no such coordinate
        ext_reaction(q, D(9), i=f"30617:{PK1}"),                       # no second colon: dropped
        ext_reaction(q, D(9), i=r1, k=None),                           # out of scope (no k)
        ext_reaction(q, D(9), i=r1, k="web"),                          # out of scope (other k)
    ]
    s, d = build(anns, ext=ext)
    sm = d["summary"]
    starred = {a, c, e, g, h, m}
    assert (sm["stars_total"], sm["stars_iris"], sm["stars_repos"]) == (len(starred), len(starred), 1)
    assert d["repos_starred"] == [{"pubkey": PK1, "d": "r", "name": "r", "cls": "forge_only",
                                   "stars": len(starred)}]
    assert sm["archive_events"]["external_reactions"] == len(ext) - 2      # the two out of scope
    assert sm["archive_events"]["reactions"] == 0
    assert N.check_invariants(s, d) == []


def test_iris_and_kind7_stars_are_one_pair_per_key_and_repository():
    """ref 6.2: stars_total is the union of the two kinds' (pubkey, repository) pairs; a
    kind-17 (external-content-reactions) "-" withdraws only that kind's star; stars_iris
    counts that kind's pairs whether or not kind-7 (reactions) also has them."""
    anns = [ann(PK1, "r", D(30)), ann(PK2, "o", D(30))]
    r, o = f"30617:{PK1}:r", f"30617:{PK2}:o"
    reactions = [reaction(PK9, D(9), a=r), reaction(PK8, D(9), a=r), reaction(PK6, D(9), a=o)]
    ext = [ext_reaction(PK9, D(8), i=r),                                 # both kinds: one pair
           ext_reaction(PK8, D(8), i=r), ext_reaction(PK8, D(7), content="-", i=r),
           ext_reaction(PK7, D(8), i=r),                                 # Iris only
           ext_reaction(PK6, D(8), content="-", i=o)]                    # cannot unstar kind-7 (reactions)
    s, d = build(anns, reactions=reactions, ext=ext)
    sm = d["summary"]
    assert (sm["stars_total"], sm["stars_iris"], sm["stars_repos"]) == (4, 2, 2)
    assert [(x["d"], x["stars"]) for x in d["repos_starred"]] == [("r", 3), ("o", 1)]
    assert N.check_invariants(s, d) == []
    # without the kind-17 (external-content-reactions) input the stars are kind-7 (reactions)
    # ones alone, as before 2026-10-11
    s0, d0 = build(anns, reactions=reactions)
    assert (d0["summary"]["stars_total"], d0["summary"]["stars_iris"]) == (3, 0)
    assert d0["summary"]["archive_events"]["external_reactions"] == 0


def test_iris_stars_wait_for_the_horizon_and_count_their_anomalies():
    """ref 1.2 on the kind-17 (external-content-reactions) input: a "-" on the run's own
    partial day waits for the next run, so the star before it stands; rows after the run
    instant and before 2023 are dropped and counted in the external_reactions slots."""
    as_of_end = N.as_of_bounds(NOW)[1]
    anns = [ann(PK1, "r", D(30))]
    r = f"30617:{PK1}:r"
    ext = [ext_reaction(PK9, D(9), i=r),
           ext_reaction(PK9, as_of_end + 1, content="-", i=r),           # today: not yet
           ext_reaction(PK8, NOW + 10 * DAY, i=r),                        # future-dated
           ext_reaction(PK7, N.PAST_FLOOR - 1, i=r),                      # past-dated
           ext_reaction(PK6, as_of_end, i=r)]                             # as_of_day's last second
    s, d = build(anns, ext=ext)
    assert (d["summary"]["stars_total"], d["summary"]["stars_iris"]) == (2, 2)
    assert d["summary"]["archive_events"]["external_reactions"] == 2
    assert d["anomalies"]["future_dated"]["external_reactions"] == 1
    assert d["anomalies"]["past_dated"]["external_reactions"] == 1
    assert d["anomalies"]["future_dated"]["reactions"] == 0
    assert N.check_invariants(s, d) == []
    # the next day's run reads the "-": PK9's star is withdrawn
    s1, d1 = build(anns, ext=ext, now=NOW + DAY)
    assert (d1["summary"]["stars_total"], d1["summary"]["stars_iris"]) == (1, 1)


def test_zaps_drop_cap_zappers_and_membership():
    coord = f"30617:{PK1}:r"
    zaps = [
        zap(D(5), a=coord, bolt11="lnbc100n1pjxyz", sender=PK9),         # 10,000
        zap(D(5), a=coord, bolt11="lnbc150m1pjxyz", sender=PK8),         # 15e9 -> capped 10e9
        zap(D(5), a=coord, bolt11="lnbc210m1pjxyz", sender=PK8),         # exactly 0.21 BTC: capped
        zap(D(5), a=coord, bolt11="lnbc300m1pjxyz", sender=PK7),         # above 0.21 BTC: dropped
        zap(D(5), a=coord, bolt11="lnbc1pvjluezxyz", sender=PK7),        # no amount: dropped
        zap(D(5), a=coord, bolt11=None, sender=PK7),                     # no bolt11: dropped
        zap(D(5), a=coord, bolt11="lnbc1u1pjxyz", sender=None),          # counted, no zapper
        zap(D(5), a=f"30617:{PK5}:nope", bolt11="lnbc1u1pjxyz"),         # not a member
    ]
    s, d = build([ann(PK1, "r", D(9))], zaps=zaps)
    sm = d["summary"]
    assert sm["zap_receipts"] == 4
    assert sm["zap_msats"] == 10_000 + 10_000_000_000 + 10_000_000_000 + 100_000
    assert sm["zappers"] == 2 and sm["zaps_repos"] == 1
    assert sm["archive_events"]["zaps"] == 8
    assert d["repos_zapped"] == [{"pubkey": PK1, "d": "r", "name": "r", "cls": "forge_only", "receipts": 4,
                                  "msats": sm["zap_msats"]}]
    assert N.check_invariants(s, d) == []


def test_repository_rows_carry_their_kinds_of_host():
    """Since 2026-10-11 each repository row (active, recent pushes, new) carries `host_kinds`:
    ref 4.4's R in the HOST_KINDS order, the kinds of public host its clone URLs are on over
    every member's, or ["none"] when R is empty. The page's Hosting column names them, where the
    class alone cannot say which other hosts a grasp_mixed or other_hosts repository is on. A
    private or non-network value adds no kind, and the kinds give the row's class exactly."""
    selfhost = "https://git.example.org/{}.git"
    s, d = build([
        ann(PK1, "g", D(9), clone=(grasp_url(repo="g"),)),                        # GRASP only
        ann(PK2, "gf", D(9), clone=(GITHUB, grasp_url(repo="gf"))),                # GRASP + a forge
        ann(PK3, "gh", D(9), clone=(grasp_url(repo="gh"),), maint=(PK4,)),         # GRASP ...
        ann(PK4, "gh", D(9), clone=(f"htree://{NPUB_SHAPE}/gh",), maint=(PK3,)),   # ... + hashtree
        ann(PK5, "h", D(9), clone=(f"htree://{NPUB_SHAPE}/h", "http://localhost/h.git")),
        ann(PK6, "f", D(9)),                                                       # GitHub only
        ann(PK7, "o", D(9), clone=(selfhost.format("o"),)),                        # self-hosted
        ann(PK8, "of", D(9), clone=(selfhost.format("of"), GITHUB)),               # + a forge mirror
        ann(PK9, "n", D(9), clone=("todo",)),                                      # nothing public,
    ], [st(PK9, "n", D(8))])                                                       # kept by its state
    want = {"g": (["grasp"], "grasp_only"), "gf": (["grasp", "forge"], "grasp_mixed"),
            "gh": (["grasp", "hashtree"], "grasp_mixed"), "h": (["hashtree"], "hashtree"),
            "f": (["forge"], "forge_only"), "o": (["self_hosted"], "other_hosts"),
            "of": (["forge", "self_hosted"], "other_hosts"), "n": (["none"], "none")}
    got = {r["d"]: (r["host_kinds"], r["cls"]) for r in d["repos_new"]}
    assert got == want
    # the kinds give the class, by the one rule (ref 4.4)
    assert all(N.repo_hosting_class(frozenset(k) - {"none"}) == c for k, c in got.values())
    # and the class is the substrate's, read from the lead row
    assert all(r["cls"] == row(s, r["pubkey"], r["d"])["cls"] for r in d["repos_new"])
    assert N.check_invariants(s, d) == []


def test_leaderboard_rows_carry_their_repository_hosting_class():
    """Since 2026-10-09 every leaderboard row (discussed, starred, zapped) carries its
    repository's hosting class (ref 4.4) as `cls`, as the repository rows do: the page links a
    hashtree-class repository to git.iris.to. It is the REPOSITORY's class, over every member's
    clone values: a hashtree member beside a GRASP one makes the repository grasp_mixed."""
    ht = f"htree://{NPUB_SHAPE}/h"
    anns = [ann(PK1, "h", D(9), clone=(ht,)),                                  # hashtree
            ann(PK2, "g", D(9), clone=(grasp_url(repo="g"),), maint=(PK3,)),   # GRASP ...
            ann(PK3, "g", D(9), clone=(f"htree://{NPUB_SHAPE}/g",), maint=(PK2,)),  # + hashtree
            ann(PK4, "f", D(9)),                                               # GitHub only
            ann(PK5, "o", D(9), clone=("https://git.example.org/o.git",))]     # self-hosted
    coord = {"h": f"30617:{PK1}:h", "g": f"30617:{PK3}:g", "f": f"30617:{PK4}:f",
             "o": f"30617:{PK5}:o"}
    owner = {"h": PK1, "g": PK3, "f": PK4, "o": PK5}
    rank = ("h", "g", "f", "o")                     # 4, 3, 2 and 1 stars, zaps, authors
    reactions = [reaction(pk(60 + i), D(5), a=coord[x]) for j, x in enumerate(rank) for i in range(4 - j)]
    zaps = [zap(D(5), a=coord[x], p=owner[x], bolt11="lnbc1u1x", sender=pk(60 + i))
            for j, x in enumerate(rank) for i in range(4 - j)]
    comments = [comment(pk(60 + i), D(3), A=coord[x]) for j, x in enumerate(rank) for i in range(4 - j)]
    s, d = build(anns, reactions=reactions, zaps=zaps, comments=comments)
    want = {"h": "hashtree", "g": "grasp_mixed", "f": "forge_only", "o": "other_hosts"}
    for name in ("repos_discussed", "repos_starred", "repos_zapped"):
        assert [(r["d"], r["cls"]) for r in d[name]] == [(x, want[x]) for x in rank], name
        # the substrate's class of the repository, read from its lead row
        assert all(r["cls"] == row(s, r["pubkey"], r["d"])["cls"] for r in d[name]), name
    assert d["repos_starred"][1]["pubkey"] == PK2       # the lead of g, by the lowest pubkey
    assert list(d["repos_discussed"][0]) == ["pubkey", "d", "name", "cls", "comments", "authors"]
    assert list(d["repos_starred"][0]) == ["pubkey", "d", "name", "cls", "stars"]
    assert list(d["repos_zapped"][0]) == ["pubkey", "d", "name", "cls", "receipts", "msats"]
    assert N.check_invariants(s, d) == []
    # the invariant holds every list to its repository's class: a wrong one, a missing one
    for name in ("repos_discussed", "repos_starred", "repos_zapped"):
        for broken in ({**d[name][0], "cls": "forge_only"}, {k: v for k, v in d[name][0].items() if k != "cls"}):
            bad = dict(d, **{name: [broken] + d[name][1:]})
            assert [p for p in N.check_invariants(s, bad) if "hosting class" in p] == [
                f"{name} carries a hosting class that is not its repository's"], (name, broken)


def test_stars_and_zaps_list_their_top_20():
    """repos_starred and repos_zapped list the top 20 since 2026-10-18 (the operator's ask; the
    top 10 before, ref 6.2 and 6.3), cut AFTER the ranking, while stars_repos and zaps_repos
    still count every repository. Of 25 repositories r00 ranks first and r24 last. On stars,
    r19 and r20 tie at the cut and the key keeps r19. Zaps rank by receipts since a second ask
    that day (by msats before): r24 holds the most sats in its one receipt and ranks last; r19
    and r20 tie at the cut on six receipts and r20's sats keep it; r17 and r18 tie on both and
    the key orders them."""
    ds = [f"r{i:02d}" for i in range(25)]
    coord = {x: f"30617:{PK1}:{x}" for x in ds}
    stars = {x: 25 - i for i, x in enumerate(ds)}
    stars["r20"] = stars["r19"]                                    # 6 each
    paid = {x: [100_000] * (25 - i) for i, x in enumerate(ds)}     # 100 sats a receipt
    paid["r18"] = [100_000] * 8                                    # r17's eight receipts and sats
    paid["r20"] = [100_000] * 5 + [600_000]                        # r19's six receipts, more sats
    paid["r24"] = [50_000_000]                                     # the most sats, in one receipt
    bolt11 = {100_000: "lnbc1u1x", 600_000: "lnbc6u1x", 50_000_000: "lnbc500u1x"}
    # emitted from the last-ranked repository up, so a list cut before its ranking keeps the
    # wrong repositories
    reactions = [reaction(pk(100 + j), D(5), a=coord[x]) for x in reversed(ds) for j in range(stars[x])]
    zaps = [zap(D(5), a=coord[x], bolt11=bolt11[m], sender=pk(100 + j))
            for x in reversed(ds) for j, m in enumerate(paid[x])]
    anns = [ann(PK1, x, D(9), clone=(f"https://github.com/o/{x}.git",)) for x in ds]
    s, d = build(anns, reactions=reactions, zaps=zaps)
    assert (d["summary"]["stars_repos"], d["summary"]["zaps_repos"]) == (25, 25)
    assert [(r["d"], r["stars"]) for r in d["repos_starred"]] == [(x, stars[x]) for x in ds[:20]]
    assert [(r["d"], r["receipts"], r["msats"]) for r in d["repos_zapped"]] == [
        (x, len(paid[x]), sum(paid[x])) for x in ds[:19] + ["r20"]]
    assert N.check_invariants(s, d) == []
    # and a 21st row is a broken document
    for name in ("repos_starred", "repos_zapped"):
        bad = dict(d, **{name: d[name] + d[name][:1]})
        assert f"{name} exceeds its cap of 20" in N.check_invariants(s, bad), name
    # so is a list short of its top 20, or out of rank order: zaps by receipts, then msats.
    # Each is also not the top 20 of ngit_repo_stats, the table the page lists from - and so
    # is a full tie (r17 and r18, on both figures) out of the key's order, which the figures
    # alone cannot see
    z = d["repos_zapped"]
    off = lambda name: f"{name} is not the top 20 of ngit_repo_stats"
    for name, bad, want in (
            ("repos_starred", d["repos_starred"][:19], [off("repos_starred"), "repos_starred lists 19 rows, not the top 20"]),
            ("repos_zapped", z[1:], [off("repos_zapped"), "repos_zapped lists 19 rows, not the top 20"]),
            ("repos_starred", d["repos_starred"][::-1], [off("repos_starred"), "repos_starred is out of rank order"]),
            ("repos_zapped", [z[1], z[0]] + z[2:], [off("repos_zapped"), "repos_zapped is out of rank order"]),
            # r18 one msat above r17, on the same eight receipts
            ("repos_zapped", z[:18] + [dict(z[18], msats=z[17]["msats"] + 1)] + z[19:],
             [off("repos_zapped"), "repos_zapped is out of rank order"]),
            ("repos_zapped", z[:17] + [z[18], z[17]] + z[19:], [off("repos_zapped")])):
        assert N.check_invariants(s, dict(d, **{name: bad})) == want, (name, want)
    # ngit_repo_stats (ref 9.2): one row per repository, on its lead coordinate, with its
    # figures; the document's lists are its top 20, which the invariants above held
    rows = {r["d"]: r for r in s["stats_rows"]}
    assert sorted(rows) == ds and len(s["stats_rows"]) == 25
    assert all(r["repo_key"] == f"{PK1}:{x}" and (r["pubkey"], r["name"], r["cls"]) == (PK1, x, "forge_only")
               for x, r in rows.items())
    assert [(x, r["stars"], r["zap_receipts"], r["zap_msats"]) for x, r in sorted(rows.items())] == [
        (x, stars[x], len(paid[x]), sum(paid[x])) for x in ds]
    # and each breakage of the table is named: r24 is outside both top 20s, so only the
    # table's own checks fire
    r24 = rows["r24"]
    for bad, want in (
            ([r for r in s["stats_rows"] if r is not r24],
             ["ngit_repo_stats is not one row per kept repository",
              "ngit_repo_stats stars do not add up to stars_total and stars_repos",
              "ngit_repo_stats zaps do not add up to zap_receipts, zap_msats and zaps_repos"]),
            ([dict(r, stars=r["stars"] + 1) if r is r24 else r for r in s["stats_rows"]],
             ["ngit_repo_stats stars do not add up to stars_total and stars_repos"]),
            ([dict(r, cls="grasp_only") if r is r24 else r for r in s["stats_rows"]],
             ["an ngit_repo_stats row is not on its repository's lead coordinate and class"]),
            ([dict(r, zap_msats=-1) if r is r24 else r for r in s["stats_rows"]],
             ["an ngit_repo_stats figure is negative",
              "ngit_repo_stats zaps do not add up to zap_receipts, zap_msats and zaps_repos"]),
            ([dict(r, name="r24 renamed") if r is r24 else r for r in s["stats_rows"]],
             ["an ngit_repo_stats name is not its repository's display name"])):
        assert N.check_invariants(dict(s, stats_rows=bad), d) == want, want


def test_ngit_repo_stats_names_are_the_display_names():
    """ngit_repo_stats carries each repository's display name (ref 2.3, 9.2): the lead's
    `name` tag, control and format characters removed, clipped to 80 - else its d - the name
    the page and the coming MCP domain show, held by the invariants for every row."""
    anns = [ann(PK1, "named", D(9), name="Named\u202e repo"), ann(PK2, "nameless", D(9)),
            ann(PK3, "blank", D(9), name="  "), ann(PK4, "long", D(9), name="x" * 100)]
    s, d = build(anns)
    names = {r["d"]: r["name"] for r in s["stats_rows"]}
    # whitespace is text (only Cc and Cf go), so "  " stays, as on the document's rows; the
    # page's label decides how a name with nothing visible reads
    assert names == {"named": "Named repo", "nameless": "nameless", "blank": "  ", "long": "x" * 80}
    assert N.check_invariants(s, d) == []
    # the fallback is checked too: a nameless repository's row must carry its d
    bad = [dict(r, name="") if r["d"] == "nameless" else r for r in s["stats_rows"]]
    assert N.check_invariants(dict(s, stats_rows=bad), d) == ["an ngit_repo_stats name is not its repository's display name"]


# --- (b) horizon, substrate, invariants -----------------------------------------------------

def test_horizon_as_of_end_now_and_the_past_floor():
    as_of_end = AS_OF_MIDNIGHT + DAY - 1                  # 2026-09-24T23:59:59Z
    assert N.as_of_bounds(NOW) == (dt.date(2026, 9, 24), as_of_end)
    s, d = build(
        [ann(PK1, "r", D(9)), ann(PK2, "end", as_of_end), ann(PK3, "today", as_of_end + 1),
         ann(PK4, "now", NOW), ann(PK5, "future", NOW + 1),
         ann(PK6, "floor", N.PAST_FLOOR), ann(PK7, "old", N.PAST_FLOOR - 1)],
        [st(PK1, "r", NOW + 100), st(PK1, "r", D(2)), st(PK1, "r", 1000)],
        comments=[comment(PK8, 1000), comment(PK8, NOW + 5)],
        reactions=[reaction(PK9, NOW + 1, a=f"30617:{PK1}:r")],
        zaps=[zap(NOW + 1, a=f"30617:{PK1}:r", bolt11="lnbc1u1x")],
    )
    fd, pd = d["anomalies"]["future_dated"], d["anomalies"]["past_dated"]
    # after as_of_end but not after now_ts: dropped silently (it waits for the next run)
    assert (fd["announcements"], fd["states"], fd["comments"], fd["reactions"], fd["zaps"]) == (
        1, 1, 1, 1, 1)
    assert (pd["announcements"], pd["states"], pd["comments"], pd["reactions"]) == (1, 1, 1, 0)
    assert d["summary"]["announcements_total"] == 3        # r, end and floor
    assert d["summary"]["archive_events"]["announcements"] == 3
    assert d["summary"]["archive_events"]["states"] == 1
    assert row(s, PK1, "r")["last_state"] == D(2)
    assert d["summary"]["first_announcement"] == "2023-01-01"
    assert d["cum_repos"][-1] == {"d": "2026-09-24", "v": 3}
    assert N.check_invariants(s, d) == []


def test_n0_oversized_d_is_counted_but_never_written_and_d_is_never_clipped():
    d200 = "x" * 200
    s, d = build([ann(PK1, "ab", D(9)), ann(PK2, d200, D(9)), ann(PK2, d200 + "y", D(9)),
                  ann(PK3, "y" * 201, D(9), name="<human-readable project name>")])
    assert d["exclusions"][0]["k"] == "N0" and d["exclusions"][0]["v"] == 2   # N0 first
    assert d["summary"]["announcements_total"] == 4 and d["summary"]["announcements_kept"] == 2
    assert sorted(r["d"] for r in s["repos_rows"]) == ["ab", d200]           # no N0 row
    assert row(s, PK2, d200)["repo_key"] == f"{PK2}:{d200}"                   # never clipped
    assert N.check_invariants(s, d) == []


def test_n0_control_characters_in_d_and_d_is_stored_as_is():
    # U+0000..U+001F and U+007F make a malformed identifier; U+0080 and non-ASCII do not
    bad = ["a\x00", "a\tb", "20260511\n", "x\x7f", "\x1fy"]
    good = ["a\x80b", "caf\u00e9", "a b", "a:b/c"]
    anns = [ann(pk(70 + i), d, D(9)) for i, d in enumerate(bad + good)]
    anns.append(ann(PK1, "a", D(9)))                      # the same key's clean "a" stays kept
    s, d = build(anns)
    assert d["exclusions"][0] == {"k": "N0", "unit": "announcements", "v": len(bad),
                                  "why": d["exclusions"][0]["why"]}
    assert sorted(r["d"] for r in s["repos_rows"]) == sorted(good + ["a"])   # stored as is
    assert row(s, pk(70 + len(bad)), "a\x80b")["repo_key"] == f"{pk(70 + len(bad))}:a\x80b"
    assert N.check_invariants(s, d) == []
    assert N.strip_nul({"k\x00": ["a\x00b"]}) == {"k": ["ab"]}


def _world_rows():
    """The rows of _world(): (announcements, states, comments)."""
    anns = [
        ann(PK1, "r", D(200), maint=(PK2,), clone=(grasp_url(),), topics=("nostr",)),
        ann(PK2, "r", D(150), maint=(PK1,)),
        ann(PK3, "s", D(100), clone=("htree://npub1abc/s",)),
        ann(PK4, "t", D(50), clone=("https://git.example.org/t.git",)),
        ann(PK5, "u", D(5)),
        ann(SK1, "v", D(5)),
    ]
    states = [st(PK1, "r", D(60), {"refs/heads/main": C1}),
              st(PK2, "r", D(10), {"refs/heads/main": C2}),
              st(PK4, "t", D(3), {"refs/heads/main": C3}),
              st(PK4, "t", D(2), {"refs/heads/main": C4, "refs/tags/v1": C1})]
    comments = [comment(PK6, D(4), A=f"30617:{PK1}:r"), comment(PK7, D(3), K="1618")]
    return anns, states, comments


def _world():
    """A small mixed roster exercising every section at once."""
    anns, states, comments = _world_rows()
    return build(anns, states, comments=comments)


def _set_host_kind(d, kind, value):
    """Sets one host kind's count in the first week that has the kind, to value(week, v)."""
    week = [e for e in d["new_repos_weekly"] if any(c["k"] == kind and c["v"] for c in e["by_host"])][0]
    c = next(c for c in week["by_host"] if c["k"] == kind)
    c["v"] = value(week, c["v"])


def _doc_row(d, name, repo):
    """The row of the document's list `name` for the repository `repo` (its d)."""
    return next(r for r in d[name] if r["d"] == repo)


KINDS_MALFORMED = ("carries host_kinds that are empty, unknown, repeated, out of the HOST_KINDS "
                   "order, or none beside a kind")
KINDS_WRONG_CLASS = "carries host_kinds that do not give its hosting class"


def test_the_world_fixture_is_consistent():
    s, d = _world()
    assert N.check_invariants(s, d) == []
    assert d["summary"]["repos"] == 4 and d["summary"]["pushes_total"] == 4


@pytest.mark.parametrize("mutate, expect", [
    (lambda s, d: d["hosting"][0].__setitem__("v", d["hosting"][0]["v"] + 1),
     "hosting classes do not sum to repos"),
    (lambda s, d: d["pushes_weekly"][-1].__setitem__("pushes", d["pushes_weekly"][-1]["pushes"] + 1),
     "pushes_weekly does not sum to pushes_total"),
    (lambda s, d: d["new_repos_monthly"][-1].__setitem__("grasp", d["new_repos_monthly"][-1]["grasp"] + 1),
     "v != grasp + other"),
    (lambda s, d: d["topics"].extend({"t": f"x{i}", "repos": 1} for i in range(30)),
     "topics exceeds its cap"),
    (lambda s, d: d["repos_active"].append(dict(d["repos_active"][0], pubkey="0" * 64)),
     "not on the roster"),
    (lambda s, d: d["repos_active"][0].__setitem__("cls", "none"),
     "repos_active carries a hosting class that is not its repository's"),
    (lambda s, d: d["repos_discussed"][0].__setitem__("cls", "hashtree"),
     "repos_discussed carries a hosting class that is not its repository's"),
    (lambda s, d: d["repos_recent_pushes"][0].__setitem__("cls", "not-a-class"),
     "repos_recent_pushes carries a hosting class that is not its repository's"),
    (lambda s, d: d["repos_new"][0].pop("cls"),
     "repos_new carries a hosting class that is not its repository's"),
    # host_kinds: each malformed list sits on a row whose class it would still give, so only
    # the clause named catches it (t other_hosts, r grasp_mixed, u forge_only, s hashtree)
    (lambda s, d: _doc_row(d, "repos_active", "t").__setitem__("host_kinds", ["hashtree"]),
     "repos_active " + KINDS_WRONG_CLASS),
    (lambda s, d: _doc_row(d, "repos_active", "r").__setitem__("host_kinds", ["forge", "grasp"]),
     "repos_active " + KINDS_MALFORMED),                                  # out of order
    (lambda s, d: _doc_row(d, "repos_new", "u").__setitem__("host_kinds", ["forge", "none"]),
     "repos_new " + KINDS_MALFORMED),                                     # none beside a kind
    (lambda s, d: _doc_row(d, "repos_new", "s").__setitem__("host_kinds", ["hashtree", "hashtree"]),
     "repos_new " + KINDS_MALFORMED),                                     # repeated
    (lambda s, d: _doc_row(d, "repos_recent_pushes", "t").__setitem__("host_kinds", []),
     "repos_recent_pushes " + KINDS_MALFORMED),                           # empty, not ["none"]
    (lambda s, d: _doc_row(d, "repos_recent_pushes", "r").__setitem__("host_kinds", 3),
     "repos_recent_pushes " + KINDS_MALFORMED),                           # not a list
    (lambda s, d: _doc_row(d, "repos_new", "u").pop("host_kinds"),
     "repos_new " + KINDS_MALFORMED),                                     # absent
    (lambda s, d: _doc_row(d, "repos_active", "t").__setitem__("host_kinds", ["self_hosted", "relay"]),
     "repos_active " + KINDS_MALFORMED),                                  # unknown
    (lambda s, d: d["recency"][0].__setitem__("v", d["recency"][0]["v"] + 1),
     "recency buckets + no_state.total != repos"),
    (lambda s, d: d["exclusions"][0].__setitem__("v", d["exclusions"][0]["v"] + 1),
     "ledger rows (announcement units) + kept != announcements_total"),
    (lambda s, d: [r for r in s["repos_rows"] if r["is_lead"]][0].__setitem__(
        "pushes_total", 99), "!= its push rows in ngit_pushes"),
    (lambda s, d: d["cum_repos"].append({"d": "2027-01-01", "v": 99}),
     "cum_repos does not end at as_of_day"),
    (lambda s, d: d["summary"].__setitem__("repos_pushed_30d", 99),
     "repos_pushed_30d <= repos_pushed_90d <= repos does not hold"),
    (lambda s, d: d["summary"].__setitem__("pushes_hashtree_30d", 1),
     "pushes_hashtree_30d != hashtree push rows of ngit_pushes in 30 d"),
    (lambda s, d: s["push_rows"][0].__setitem__("source", "roots"),
     "a ngit_pushes row has an unknown source"),
    (lambda s, d: s["push_rows"][0].__setitem__("branches", None),
     "a state row without its branch and tag counts"),
    (lambda s, d: d["summary"].__setitem__("comments_30d", 99),
     "comments_30d <= comments_90d <= comments_total"),
    (lambda s, d: d["summary"].__setitem__("comments_90d", 99),
     "comments_30d <= comments_90d <= comments_total"),
    (lambda s, d: s["push_rows"][0].__setitem__("created_at", N.PAST_FLOOR - 1),
     "a substrate timestamp lies outside the horizon"),
    (lambda s, d: s["repos_rows"][0].__setitem__("last_seen", N.as_of_bounds(NOW)[1] + 1),
     "a substrate timestamp lies outside the horizon"),
    (lambda s, d: s["push_rows"].extend(dict(s["push_rows"][-1], event_id=f"{i:064x}")
                                        for i in range(51)),
     "push rows of ngit_pushes != pushes_total"),
    (lambda s, d: d["exclusions"].append({"k": "daily_cap", "unit": "pushes", "v": 0, "why": ""}),
     "the ledger holds a pushes row"),
    (lambda s, d: d["summary"].update(stars_iris=d["summary"]["stars_total"] + 1),
     "stars_iris > stars_total"),
    (lambda s, d: d["summary"].update(stars_iris=1, stars_total=1, stars_repos=1)
     or d["summary"]["archive_events"].update(external_reactions=0),
     "stars_iris without a kind-17 (external-content-reactions) row read"),
    (lambda s, d: d["new_repos_weekly"][-1].__setitem__("grasp", d["new_repos_weekly"][-1]["grasp"] + 1),
     "a new_repos_weekly week has v != grasp + other"),
    (lambda s, d: d["new_repos_weekly"].pop(0),
     "new_repos_weekly does not span"),
    (lambda s, d: d["new_repos_weekly"][-1].__setitem__("s", d["new_repos_weekly"][-1]["s"] + 1),
     "new_repos_weekly != the substrate's first-seen weeks"),
    (lambda s, d: [e for e in d["new_repos_weekly"] if e["v"]][0].update(
        v=0, other=0, grasp=0), "new_repos_weekly != the substrate's first-seen weeks"),
    (lambda s, d: d["new_repos_weekly"][-1].pop("by_host"),
     "by_host is not every host kind in order"),
    (lambda s, d: d["new_repos_weekly"][-1]["by_host"].reverse(),
     "by_host is not every host kind in order"),
    (lambda s, d: d["new_repos_weekly"][-1]["by_host"].pop(),
     "by_host is not every host kind in order"),
    (lambda s, d: d["new_repos_weekly"][-1]["by_host"][0].__setitem__("v", 1),
     "by_host grasp != grasp, or none != the substrate's class none"),
    (lambda s, d: d["new_repos_weekly"][-1]["by_host"][4].__setitem__("v", 1),
     "by_host grasp != grasp, or none != the substrate's class none"),
    # a kind below what the week's classes require, or above what they allow
    (lambda s, d: _set_host_kind(d, "hashtree", lambda week, v: v - 1),
     "by_host is outside what its hosting classes allow"),
    (lambda s, d: _set_host_kind(d, "forge", lambda week, v: v - 1),
     "by_host is outside what its hosting classes allow"),
    (lambda s, d: _set_host_kind(d, "self_hosted", lambda week, v: week["v"] + 1),
     "by_host is outside what its hosting classes allow"),
    (lambda s, d: d["comments_weekly"].pop(),
     "comments_weekly does not span"),
    (lambda s, d: [e for e in d["comments_weekly"] if e["authors"]][0].__setitem__("authors", 9),
     "!= the counted comments"),
    (lambda s, d: d["grasp_lists"]["per_week"].pop(0),
     "grasp_lists.per_week does not span"),
    (lambda s, d: d["grasp_lists"]["per_week"][0].__setitem__("v", 1),
     "grasp_lists.per_week != the users' first-seen weeks"),
    (lambda s, d: s["repos_rows"].append(dict(s["repos_rows"][-1])),
     "ngit_repos (pubkey, d) is not unique"),
])
def test_invariants_catch_a_broken_document(mutate, expect):
    s, d = _world()
    s, d = copy.deepcopy(s), copy.deepcopy(d)
    mutate(s, d)
    problems = N.check_invariants(s, d)
    assert any(expect in p for p in problems), problems


def test_held_keys_hold_the_nostr_net_catalog():
    """The nostr.net catalog key stays held after the operator's decision of 2026-10-12: N10 excludes its
    announcements, and the hold guards that rule. To count them instead, drop the key from
    HELD_KEYS and N10 together."""
    assert N.HELD_KEYS == frozenset({"62ec33a4090f0959e5c547a33f22a3253688bba121667a69e64a2dc758f8baaf"})


def test_held_keys_block_on_any_kept_coordinate_of_theirs(monkeypatch):
    """The blocking held_keys_absent check counts a held key's KEPT announcement coordinates,
    lead or not: one its announcement excludes (no public clone URL, N8b) does not count, nor
    does another key's; a held coordinate that joins another key's repository (mutual
    maintainers, same d) does, since it adds a maintainer and can move the class.
    require_no_held_keys raises with the key and the count."""
    held = pk(77)
    monkeypatch.setattr(N, "HELD_KEYS", frozenset({held}))
    s, d = build([ann(PK1, "mine", D(9))])
    assert N.held_key_repos(s) == {}
    N.require_no_held_keys(s)                  # nothing held: no raise
    s, d = build([ann(held, "clients-nostore", D(9)),
                  ann(held, "tools-twotone", D(9), clone=("https://codeberg.org/o/t.git",)),
                  ann(held, "web-only", D(9), clone=(), web=("https://git.sr.ht/~o/w",)),
                  ann(PK1, "mine", D(9))])
    assert row(s, held, "web-only")["family"] == "N8b"
    assert N.held_key_repos(s) == {held: 2}
    assert N.check_invariants(s, d) == []      # a hold, not a broken document
    with pytest.raises(RuntimeError, match=f"held keys .*{held[:8]} 2"):
        N.require_no_held_keys(s)
    # a member coordinate of another key's repository, announced later, so not its lead
    s, d = build([ann(PK1, "proj", D(9), clone=(grasp_url(),), maint=(held,)),
                  ann(held, "proj", D(5), clone=("https://github.com/real/proj.git",), maint=(PK1,))])
    member = row(s, held, "proj")
    assert member["family"] == "repo" and not member["is_lead"] and member["repo_key"].startswith(PK1)
    assert N.held_key_repos(s) == {held: 1}


def test_both_write_gates_run_before_write_pg():
    """ngit_snapshot raises on a broken document (check_invariants) and on a held key
    (require_no_held_keys) BEFORE write_pg, so a failure writes nothing and the served row
    stays. Read from the source: the asset needs dagster and Aurora to run."""
    src = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "assets.py")).read()
    body = src[src.index("def ngit_snapshot("):]
    body = body[:body.index("\n@asset")] if "\n@asset" in body else body
    write = body.index("_ngit.write_pg(")
    assert body.index("_ngit.check_invariants(") < write
    assert body.index("_ngit.require_no_held_keys(state)") < write


def test_roster_growth_check():
    base = [ann(pk(200 + i), f"q{i}", D(i + 1, 3600)) for i in range(30)]   # 1 a day
    flood = [ann(pk(40), f"flood{i}", D(0, 60 + i)) for i in range(21)]
    s, d = build(base + flood[:20])
    g = N.roster_growth(s, d)
    assert (g["first_seen_on_as_of_day"], g["median_30d"], g["limit"], g["passed"]) == (20, 1.0, 20.0, True)
    s, d = build(base + flood)
    assert not N.roster_growth(s, d)["passed"]
    busy = [ann(pk(60 + j), f"b{i}", D(i + 1, j)) for i in range(30) for j in range(10)]
    s, d = build(busy + flood)
    g = N.roster_growth(s, d)
    assert (g["median_30d"], g["limit"], g["passed"]) == (10.0, 50.0, True)


# --- (c) the write gates --------------------------------------------------------------------

class _GateCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, args=None):
        if not sql.lstrip().upper().startswith("SELECT"):
            raise AssertionError(f"a write before the anti-shrink gate: {sql[:40]}")
        self.conn.reads += 1

    def fetchone(self):
        return None if self.conn.prev is None else (self.conn.prev,)


class _GateConn:
    """Serves the gate's read of the newest summary; any other statement is a write."""

    def __init__(self, prev):
        self.prev, self.reads = prev, 0

    def cursor(self):
        return _GateCursor(self)

    def commit(self):
        raise AssertionError("commit before the anti-shrink gate")


class _RecordingCursor:
    """Enough of a psycopg2 cursor for the gate read plus execute_values."""

    class _Conn:
        encoding = "UTF8"

    connection = _Conn()

    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, args=None):
        self.conn.log.append((sql if isinstance(sql, str) else sql.decode(), args))

    def fetchone(self):
        return None if self.conn.prev is None else (self.conn.prev,)

    def mogrify(self, template, args):
        return repr(tuple(args)).encode()


class _RecordingConn:
    def __init__(self, prev=None):
        self.prev, self.log, self.committed = prev, [], False

    def cursor(self):
        return _RecordingCursor(self)

    def commit(self):
        self.committed = True


def test_write_pg_refuses_on_the_relative_floor_before_any_write():
    s, d = _world()
    prev = {"announcements_kept": d["summary"]["announcements_kept"] * 2, "pushes_total": 0}
    conn = _GateConn(prev)
    with pytest.raises(RuntimeError, match="anti-shrink: announcements_kept"):
        N.write_pg(conn, s, d, run_id="t", override_until=None)
    assert conn.reads == 1
    prev = {"announcements_kept": 0, "pushes_total": d["summary"]["pushes_total"] * 2}
    with pytest.raises(RuntimeError, match="anti-shrink: pushes_total"):
        N.write_pg(_GateConn(prev), s, d, run_id="t", override_until=None)


def _gate_doc(kept, pushes, **ae):
    events = {f: 100 for f in N.ARCHIVE_EVENT_FIELDS}
    events.update(ae)
    return {"summary": {"announcements_kept": kept, "pushes_total": pushes, "archive_events": events},
            "generated_utc": "2026-09-25 03:15:00Z"}


def test_ngit_repo_stats_ddl_is_the_columns_written():
    """schema/init.sql's ngit_repo_stats holds exactly STATS_COLUMNS, in order (ref 9.2), so
    a column the pipeline writes cannot go missing from a fresh database's table."""
    schema = open(os.path.join(os.path.dirname(N.__file__), "..", "..", "schema", "init.sql")).read()
    block = schema.split("CREATE TABLE IF NOT EXISTS ngit_repo_stats (")[1].split(");")[0]
    cols = [ln.split()[0] for ln in block.splitlines() if ln.strip() and not ln.strip().startswith("--")]
    assert tuple(cols) == N.STATS_COLUMNS


def test_relative_floor_boundary_is_ninety_percent():
    prev = _gate_doc(10, 100)["summary"]
    assert N.shrink_gate(prev, _gate_doc(9, 90), override_until=None) == ([], False)
    problems, bypassed = N.shrink_gate(prev, _gate_doc(8, 90), override_until=None)
    assert len(problems) == 1 and not bypassed


def test_write_pg_refuses_on_the_absolute_first_run_floor_before_any_write():
    s, d = _world()
    conn = _GateConn(None)
    with pytest.raises(RuntimeError, match="first-run floor") as err:
        N.write_pg(conn, s, d, run_id="t", override_until=None)
    assert conn.reads == 1
    # _world is below BOTH floors, and both are named
    assert "announcements_kept 5 < first-run floor 3500" in str(err.value)
    assert "pushes_total 4 < first-run floor 12000" in str(err.value)


def test_write_pg_passes_with_the_override_date_and_writes_one_transaction():
    s, d = _world()
    logged = []
    # an expired override does not bypass
    with pytest.raises(RuntimeError, match="anti-shrink"):
        N.write_pg(_GateConn(None), s, d, run_id="t", override_until="2026-09-24")
    conn = _RecordingConn(None)
    stats = N.write_pg(conn, s, d, run_id="t", override_until="2026-09-25", log=logged.append)
    assert stats["shrink_bypassed"] and any("BYPASSED" in m for m in logged)
    statements = [sql for sql, _ in conn.log]
    assert statements[0].startswith("SELECT data->'summary' FROM ngit_snapshot")
    assert [x for x in statements if x.startswith("TRUNCATE")] == ["TRUNCATE ngit_repos, ngit_pushes"]
    # the daily cap's column is dropped under the TRUNCATE's lock, before the inserts - and
    # never by schema/init.sql, which every job applies (a queued ALTER there would hold the
    # locks of the file's earlier statements on other surfaces' tables)
    alter = statements.index("ALTER TABLE ngit_pushes DROP COLUMN IF EXISTS counted")
    assert statements.index("TRUNCATE ngit_repos, ngit_pushes") < alter < min(
        i for i, x in enumerate(statements) if x.startswith("INSERT"))
    # ref 3 (2026-10-12): the ledger's source column, and no branch or tag count on a hashtree
    # push, under the same lock and before the inserts. The default is 'state', what the
    # pipeline before this change writes without naming the column, so a rollback still inserts
    source = statements.index("ALTER TABLE ngit_pushes ADD COLUMN IF NOT EXISTS source TEXT NOT NULL "
                              "DEFAULT 'state' CHECK (source IN ('state', 'htree_announcement'))")
    nulls = statements.index("ALTER TABLE ngit_pushes ALTER COLUMN branches DROP NOT NULL, "
                             "ALTER COLUMN tags DROP NOT NULL")
    assert alter < source < nulls < min(i for i, x in enumerate(statements) if x.startswith("INSERT"))
    # this code names the column on every row, so the default only ever serves a rollback
    assert "source" in [x for x in statements if x.startswith("INSERT INTO ngit_pushes")][0]
    schema = open(os.path.join(os.path.dirname(N.__file__), "..", "..", "schema", "init.sql")).read()
    assert "DROP COLUMN" not in schema.split("CREATE TABLE IF NOT EXISTS ngit_pushes")[1].split("CREATE TABLE")[0]
    inserts = [x for x in statements if x.startswith("INSERT")]
    assert [x.split()[2] for x in inserts] == ["ngit_repos", "ngit_pushes", "ngit_repo_stats", "ngit_snapshot"]
    # ngit_repo_stats (ref 9.2): emptied by DELETE, never in the TRUNCATE (a page read must not
    # wait on its lock), inside the same transaction, every row carrying the snapshot's run id
    delete = statements.index("DELETE FROM ngit_repo_stats")
    stats_insert = next(i for i, x in enumerate(statements) if x.startswith("INSERT INTO ngit_repo_stats"))
    assert statements.index("TRUNCATE ngit_repos, ngit_pushes") < delete < stats_insert
    assert all(f"('{r['repo_key']}', 't', '{r['pubkey']}'" in statements[stats_insert] for r in s["stats_rows"])
    assert stats["stats_rows"] == len(s["stats_rows"]) > 0 and conn.committed
    assert conn.committed
    assert stats["repo_rows"] == 6 and stats["push_rows"] == 4
    assert "pubkey, d, family, repo_key, is_lead" in inserts[0]
    assert inserts[1].startswith("INSERT INTO ngit_pushes (event_id, pubkey, d, repo_key, created_at, ")
    assert "branches, tags, source) VALUES" in inserts[1]


def test_write_pg_writes_a_hashtree_push_with_its_source_and_no_ref_counts():
    s, d = _htree_world()
    conn = _RecordingConn(None)
    stats = N.write_pg(conn, s, d, run_id="t", override_until="2026-09-25")
    assert conn.committed and stats["push_rows"] == 5
    # what execute_values sent: each row mogrified in PUSH_COLUMNS order (the recording cursor
    # renders a row as its Python tuple), so a hashtree push ends in NULL branches and tags and
    # its source, and every state row ends in its counts and 'state'
    (insert,) = [sql for sql, _ in conn.log if sql.startswith("INSERT INTO ngit_pushes")]
    assert insert.count("None, None, 'htree_announcement')") == 1
    assert insert.count("'state')") == 4 and "'htree_announcement'" in insert


def test_write_pg_passes_the_gate_on_steady_state():
    s, d = _world()
    prev = {"announcements_kept": d["summary"]["announcements_kept"],
            "pushes_total": d["summary"]["pushes_total"]}
    conn = _RecordingConn(json.dumps(prev))                    # a JSONB read back as text
    stats = N.write_pg(conn, s, d, run_id="t", override_until=None)
    assert conn.committed and not stats["shrink_bypassed"]


def test_document_contract_field_names():
    _s, d = _world()
    assert list(d) == [
        "generated_utc", "as_of_day", "coverage", "anomalies", "summary", "exclusions",
        "new_repos_monthly", "new_repos_weekly", "cum_repos", "pushes_weekly", "recency",
        "no_state", "hosting", "clone_hosts", "grasp_servers", "grasp_per_repo", "relays",
        "relays_per_repo", "web_hosts", "default_branch", "branches_per_repo", "tags_per_repo",
        "topics", "tools", "repos_active", "repos_recent_pushes", "repos_new",
        "fork_families_top", "maintainers_top", "grasp_lists", "comments_monthly",
        "comments_weekly", "repos_discussed", "repos_starred", "repos_zapped"]
    assert list(d["grasp_lists"]) == ["users", "per_month", "per_week", "length", "first_choice"]
    assert list(d["new_repos_weekly"][0]) == ["w", "v", "grasp", "other", "s", "by_host"]
    assert list(d["new_repos_weekly"][0]["by_host"][0]) == ["k", "v"]
    assert list(d["comments_weekly"][0]) == ["w", "issues", "prs", "patches", "repo", "other",
                                             "authors", "threads"]
    assert list(d["grasp_lists"]["per_week"][0]) == ["w", "v"]
    assert set(d["summary"]) == {
        "archive_events", "announcements_total", "announcements_kept", "repos", "forks",
        "repos_with_state", "repos_pushed_30d", "repos_pushed_90d", "pushes_7d", "pushes_30d",
        "pushes_total", "pushes_hashtree_30d", "maintainers", "maintainers_active_30d",
        "multi_maintainer_repos",
        "shared_listed_repos", "fork_families", "grasp_repos", "grasp_share", "grasp_servers",
        "grasp_list_users", "comments_total", "comments_30d", "comments_90d", "comment_threads",
        "comment_participants", "comments_attributable_share", "stars_total", "stars_repos",
        "stars_iris", "zap_receipts", "zap_msats", "zappers", "zaps_repos", "first_announcement",
        "newest_state", "newest_push", "concentration"}
    assert set(d["repos_active"][0]) == {"pubkey", "d", "name", "lead_name", "pushes_30d",
                                         "pushes_90d", "last_push", "last_state", "branches",
                                         "tags", "cls", "host_kinds", "hosts"}
    assert d["generated_utc"] == "2026-09-25 03:15:00Z" and d["as_of_day"] == "2026-09-24"
    assert set(d["coverage"]) == {"announcements_since", "states_since", "comments_since",
                                  "reactions_since", "external_reactions_since", "zaps_since",
                                  "deletions_since", "ngit_relay_since", "partial_first_week",
                                  "partial_week", "partial_month"}
    # the day relay.ngit.dev joined the capture (ref 0.2) - equal to states_since only by chance
    assert d["coverage"]["ngit_relay_since"] == "2026-06-14"
    slots = {"announcements", "states", "grasp_lists", "comments", "reactions",
             "external_reactions", "zaps"}
    assert set(d["anomalies"]) == {"future_dated", "past_dated"}
    assert set(d["anomalies"]["future_dated"]) == set(d["anomalies"]["past_dated"]) == slots
    assert d["coverage"]["announcements_since"] == "2026-05-02"
    assert d["coverage"]["states_since"] == "2026-06-14"
    assert d["coverage"]["comments_since"] == "2026-06-18"
    # the earliest ingest partition of each social kind (s3 listing, 2026-09-25)
    assert d["coverage"]["reactions_since"] == "2026-06-19"
    assert d["coverage"]["zaps_since"] == "2026-06-14"
    assert d["coverage"]["deletions_since"] == "2026-07-01"
    # kind-17 (external-content-reactions): captured since 2026-10-11 (nozzl's onboarding)
    assert d["coverage"]["external_reactions_since"] == "2026-10-11"
    assert list(d["summary"]["archive_events"]) == list(N.ARCHIVE_EVENT_FIELDS)


def test_parse_ndjson_gz_reads_every_part(tmp_path):
    import gzip
    for i, rows in enumerate(([{"id": "a"}, {"id": "b"}], [{"id": "c"}])):
        with gzip.open(tmp_path / f"p{i}.gz", "wt") as fh:
            fh.write("\n".join(json.dumps(r) for r in rows) + "\n\n")
    assert [r["id"] for r in N.parse_ndjson_gz(sorted(tmp_path.glob("*.gz")))] == ["a", "b", "c"]


# --- review 2 -------------------------------------------------------------------------------

def test_hostile_zap_receipts_cannot_crash_the_build():
    """A bolt11 that overflows the shared parser's float multiplier and a description nested
    past the recursion limit: the first is no readable amount (dropped), the second does not
    parse (the receipt still counts, with no zapper)."""
    coord = f"30617:{PK1}:r"
    overflow = zap(D(5), a=coord, bolt11="lnbc" + "9" * 320 + "p1x", sender=PK9)
    deep = zap(D(5), a=coord, bolt11="lnbc1u1x", sender=None)
    deep["tags"].append(["description", "[" * 12000])
    zp = N.load_zap_parser()
    with pytest.raises(OverflowError):                  # the shared parser itself still raises
        zp._parse_bolt11_amount("lnbc" + "9" * 320 + "p1x")
    assert N.zap_amount_msats(zp, overflow["tags"]) is None
    assert N.zap_sender(deep["tags"]) is None
    s, d = build([ann(PK1, "r", D(9))], zaps=[overflow, deep])
    sm = d["summary"]
    assert (sm["zap_receipts"], sm["zap_msats"], sm["zappers"]) == (1, 100_000, 0)
    assert N.check_invariants(s, d) == []


def test_the_finiteness_gate_rejects_anything_but_a_non_negative_int():
    """The shared parser returns an int or None today; the gate is what keeps a future change
    (a float, a bool, a negative) from reaching the integer msats arithmetic."""
    class _Parser:
        def __init__(self, value):
            self.value = value

        def _parse_bolt11_amount(self, _b):
            return self.value

    tags = [["bolt11", "lnbc1u1x"]]
    for bad in (float("inf"), float("nan"), 12.5, -5, True, "100"):
        assert N.zap_amount_msats(_Parser(bad), tags) is None, bad
    assert N.zap_amount_msats(_Parser(0), tags) == 0
    assert N.zap_amount_msats(_Parser(10 ** 30), tags) == 10 ** 30   # the spam filter drops it


def test_zap_input_bounds_are_inclusive():
    zp = N.load_zap_parser()
    ok = "lnbc1u1" + "x" * (N.BOLT11_MAX_LEN - 7)
    assert len(ok) == N.BOLT11_MAX_LEN and N.zap_amount_msats(zp, [["bolt11", ok]]) == 100_000
    assert N.zap_amount_msats(zp, [["bolt11", ok + "x"]]) is None
    pad = N.DESCRIPTION_MAX_LEN - len(json.dumps({"pubkey": PK9, "pad": ""}))
    fits = json.dumps({"pubkey": PK9, "pad": "x" * pad})
    assert len(fits) == N.DESCRIPTION_MAX_LEN and N.zap_sender([["description", fits]]) == PK9
    too_long = json.dumps({"pubkey": PK9, "pad": "x" * (pad + 1)})
    assert N.zap_sender([["description", too_long]]) is None


def test_the_last_valued_description_wins():
    first, last = json.dumps({"pubkey": PK8}), json.dumps({"pubkey": PK9})
    assert N.zap_sender([["description", first], ["description", last]]) == PK9
    assert N.zap_sender([["description", first], ["description", last], ["description"]]) == PK9


def test_comment_coordinate_reads_A_before_a():
    """The first `A` value starting 30617: decides even when it names no repository and an
    `a` tag names a member coordinate (ref 6.1)."""
    member, other = f"30617:{PK1}:r", f"30617:{PK5}:gone"
    s, d = build([ann(PK1, "r", D(9))], comments=[
        comment(PK2, D(3), K="1621", A=other, a=member),     # A decides: NOT attributable
        comment(PK3, D(3), K="1621", a=member),              # only `a`: attributable
    ])
    assert d["summary"]["comments_total"] == 2
    assert d["summary"]["comments_attributable_share"] == 1 / 2
    assert d["repos_discussed"] == [{"pubkey": PK1, "d": "r", "name": "r", "cls": "forge_only", "comments": 1,
                                     "authors": 1}]


def test_mirror_keys_are_counted_over_the_whole_admitted_input():
    """49 re-posts plus one ephemeral comment make 50 in the INPUT: a mirror key, so its 49
    counted-looking comments are mirrored (counting after the ephemeral rule would find 49)."""
    posts = [comment(PK7, D(3), content="**@alice** said") for _ in range(49)]
    posts.append(comment(PK7, D(3), content="**@alice** said", extra=(["expiration", "1"],)))
    s, d = build([ann(PK1, "r", D(9))], comments=posts)
    ledger = {e["k"]: e["v"] for e in d["exclusions"] if e["unit"] == "comments"}
    assert ledger == {"ephemeral": 1, "imported": 0, "mirrored": 49}
    assert d["summary"]["comments_total"] == 0


def test_fork_family_through_a_non_lead_member_euc():
    """A multi-member repository whose LEAD has no euc takes a co-maintainer's (ref 2.3); a
    second repository sharing it forms a family, and the invariants must agree."""
    s, d = build([
        ann(PK1, "r", D(30), maint=(PK2,), name="Lead"),                        # the lead: no euc
        ann(PK2, "r", D(20), maint=(PK1,), extra=(["r", C1, "euc"],)),         # member's euc
        ann(PK3, "f", D(25), name="Fork", extra=(["r", C1, "euc"],)),
    ])
    assert row(s, PK1, "r")["is_lead"] and row(s, PK1, "r")["euc"] is None
    assert d["summary"]["fork_families"] == 1
    assert d["fork_families_top"] == [{"euc": C1, "repos": 2, "names": ["Lead", "Fork"]}]
    assert N.check_invariants(s, d) == []
    d["fork_families_top"][0]["repos"] = 3
    assert any("fork_families_top names a family" in p for p in N.check_invariants(s, d))
    d["fork_families_top"][0]["repos"] = 2
    d["summary"]["fork_families"] = 2
    assert any("fork_families != the euc values" in p for p in N.check_invariants(s, d))


def test_archive_events_are_gated_against_the_served_row():
    """An empty deletions extract would resurrect deleted repositories through every other
    gate; each archive_events count is gated at 90% of the served row (ref: design 9 step 5)."""
    prev = _gate_doc(10, 100, deletions=40, reactions=10)["summary"]
    problems, bypassed = N.shrink_gate(prev, _gate_doc(10, 100, deletions=0, reactions=10),
                                       override_until=None)
    assert problems == ["archive_events.deletions 0 < 90% of the served row's 40"] and not bypassed
    problems, _ = N.shrink_gate(prev, _gate_doc(10, 100, deletions=36, reactions=8),
                                override_until=None)
    assert problems == ["archive_events.reactions 8 < 90% of the served row's 10"]
    # an empty kind-17 (external-content-reactions) extract cannot drop Iris Git's stars
    problems, _ = N.shrink_gate(_gate_doc(10, 100, external_reactions=31)["summary"],
                                _gate_doc(10, 100, external_reactions=0), override_until=None)
    assert problems == ["archive_events.external_reactions 0 < 90% of the served row's 31"]
    # a served row written before the kind was read gates nothing on it (its first run)
    prev_ae = {k: v for k, v in _gate_doc(10, 100)["summary"]["archive_events"].items()
               if k != "external_reactions"}
    assert N.shrink_gate({"announcements_kept": 10, "pushes_total": 100, "archive_events": prev_ae},
                         _gate_doc(10, 100, external_reactions=0), override_until=None) == ([], False)
    # an older served row without archive_events gates nothing on them
    assert N.shrink_gate({"announcements_kept": 10, "pushes_total": 100},
                         _gate_doc(10, 100, zaps=0), override_until=None) == ([], False)
    # the first run has no floor for them (only kept coordinates and pushes)
    first = _gate_doc(4000, 20000, deletions=0, reactions=0, zaps=0)
    assert N.shrink_gate(None, first, override_until=None) == ([], False)


def test_write_pg_refuses_an_archive_events_shrink_before_any_write():
    s, d = _world()
    prev = dict(d["summary"], archive_events=dict(d["summary"]["archive_events"], comments=100))
    with pytest.raises(RuntimeError, match="archive_events.comments"):
        N.write_pg(_GateConn(prev), s, d, run_id="t", override_until=None)


def test_shrink_override_is_parsed_strictly():
    logged = []
    prev = _gate_doc(10, 100)["summary"]
    shrunk = _gate_doc(5, 100)
    for bad in ("2026-9-30", "20260930", "2026-02-30", "soon", 20260930):
        problems, bypassed = N.shrink_gate(prev, shrunk, override_until=bad, log=logged.append)
        assert problems and not bypassed, bad
    assert len(logged) == 5 and all("stays on" in m for m in logged)
    assert N.shrink_gate(prev, shrunk, override_until="2026-09-25")[1]           # inclusive
    assert not N.shrink_gate(prev, shrunk, override_until="2026-09-24")[1]
    assert N.parse_override(None) is None and N.parse_override("") is None


def test_star_e_lookup_uses_versions_that_survive_preprocessing():
    ancient = ann(PK1, "r", N.PAST_FLOOR - 10)          # dropped as past-dated
    current = ann(PK1, "r", D(9))
    s, d = build([ancient, current], reactions=[
        reaction(PK8, D(5), e=ancient["id"]),            # names a dropped version: no target
        reaction(PK9, D(5), e=current["id"]),
    ])
    assert d["summary"]["stars_total"] == 1 and d["anomalies"]["past_dated"]["announcements"] == 1


def test_tool_labels_longer_than_40_keep_their_tail():
    # ref 4.5: the long client values are nsite hostnames whose recognisable part is the tail;
    # the document clips, the substrate column holds the full value
    host = "47h1rs70oqaspur8bichfzalg0wnassb8d7tslfacl7hwyaxhkmarlowe.nsite.lol"
    assert len(host) > 40
    anns = [ann(pk(i), f"r{i}", D(9), extra=(["client", host],)) for i in range(3)]
    s, d = build(anns)
    assert [e["k"] for e in d["tools"]] == ["..." + host[-37:]]
    assert len(d["tools"][0]["k"]) == 40 and d["tools"][0]["k"].endswith("marlowe.nsite.lol")
    assert row(s, pk(0), "r0")["tool"] == host
    assert N._clip_tail("short", 40) == "short" and N._clip_tail(None, 40) is None
    assert N.check_invariants(s, d) == []


def test_shrink_override_is_not_left_armed():
    """A far-future SHRINK_OVERRIDE_UNTIL bypasses every anti-shrink gate until that date, and a
    stale one is clutter: None, or a date at most 14 days ahead and at most 30 days past."""
    import datetime as _dt
    if N.SHRINK_OVERRIDE_UNTIL is None:
        return
    until = N.parse_override(N.SHRINK_OVERRIDE_UNTIL)
    assert until is not None, "SHRINK_OVERRIDE_UNTIL is set but is not a YYYY-MM-DD date"
    today = _dt.date.today()
    assert until <= today + _dt.timedelta(days=14), "SHRINK_OVERRIDE_UNTIL is more than 14 days ahead"
    assert until >= today - _dt.timedelta(days=30), "SHRINK_OVERRIDE_UNTIL expired over 30 days ago: set it back to None"


# --- review 4 -------------------------------------------------------------------------------

def test_zap_receipts_count_only_from_a_listed_signer_to_a_member_for_the_stated_amount():
    """ref 6.3 (review 4 M3): one receipt signed by a throwaway key once moved the Zaps headline
    600x. A receipt counts when a verified LNURL provider key signed it, its first `p` pays a
    member of the repository it names, and the embedded request's `amount`, when it states
    one, is the bolt11 amount exactly."""
    coord = f"30617:{PK1}:r"
    anns = [ann(PK1, "r", D(9), maint=(PK2,)), ann(PK2, "r", D(9), maint=(PK1,)), ann(PK3, "q", D(9))]
    first_p_other = zap(D(5), a=coord, bolt11="lnbc9u1x")
    first_p_other["tags"].insert(1, ["p", PK3])            # a member as the SECOND p does not help
    no_pubkey = zap(D(5), a=coord, bolt11="lnbc6u1x")
    no_pubkey["tags"][-1] = ["description", json.dumps({"kind": 9734, "tags": [["amount", "1"]]})]
    zaps = [
        zap(D(5), a=coord, bolt11="lnbc1u1x", amount="100000"),         # counts: 100,000
        zap(D(5), a=coord, bolt11="lnbc2u1x", p=PK2, sender=PK8,        # a NON-LEAD member at the
            amount="200000"),                                           # exact amount: counts
        zap(D(5), a=coord, bolt11="lnbc3u1x", signer="e" * 64),         # an unlisted signer: dropped
        zap(D(5), a=coord, bolt11="lnbc4u1x", p=PK3),                   # pays a non-member: dropped
        zap(D(5), a=coord, bolt11="lnbc5u1x", p=None),                  # pays nobody: dropped
        zap(D(5), a=coord, bolt11="lnbc4u1x", amount="400001"),         # 400,000 msats: dropped
        zap(D(5), a=coord, bolt11="lnbc7u1x", amount="0700000"),        # decimal TEXT: dropped
        zap(D(5), a=coord, bolt11="lnbc7u1x", amount=700000),           # not text: dropped
        zap(D(5), a=coord, bolt11="lnbc8u1x", amount=""),               # an empty amount: counts
        zap(D(5), a=coord, bolt11="lnbc150m1x", amount="15000000000"),  # the bolt11 amount, not the cap
        first_p_other,
        no_pubkey,                        # a request without a pubkey never blocks: counts
    ]
    s, d = build(anns, zaps=zaps)
    sm = d["summary"]
    assert sm["zap_receipts"] == 5
    assert sm["zap_msats"] == 100_000 + 200_000 + 800_000 + 10_000_000_000 + 600_000
    assert sm["zappers"] == 2 and sm["archive_events"]["zaps"] == len(zaps)   # the input count
    assert d["repos_zapped"] == [{"pubkey": PK1, "d": "r", "name": "r", "cls": "forge_only", "receipts": 5,
                                  "msats": sm["zap_msats"]}]
    # nothing counts without the signer
    assert build(anns, zaps=zaps, signers=frozenset({"e" * 64}))[1]["summary"]["zap_receipts"] == 1
    assert N.check_invariants(s, d) == []


def test_the_embedded_request_and_its_amount():
    """ref 6.3: the request is the zapper parse of the LAST valued description (a JSON object
    with a non-empty string pubkey); its amount is the value of its FIRST `amount` tag, as
    published, checked only when non-empty."""
    req = lambda obj: [["description", json.dumps(obj)]]           # noqa: E731
    assert N.zap_request(req({"pubkey": PK9})) == {"pubkey": PK9}
    assert N.zap_request(req({"n": 1})) is None and N.zap_request(req({"pubkey": ""})) is None
    assert N.zap_request([["description", "[1]"]]) is None and N.zap_request([]) is None
    assert N.zap_request(req({"pubkey": PK8}) + req({"pubkey": PK9}) + [["description"]]) == {
        "pubkey": PK9}
    amt = lambda rtags: N.zap_request_amount(req({"pubkey": PK9, "tags": rtags}))   # noqa: E731
    assert amt([["amount", "21000"]]) == "21000"
    assert amt([["amount", "1"], ["amount", "2"]]) == "1"         # the FIRST amount tag
    assert amt([["amount"], ["amount", "2"]]) is None             # ... has no value: no check
    assert amt([["amount", ""], ["amount", "2"]]) is None         # ... is empty: no check
    assert amt([["amount", None]]) is None
    assert amt([["amount", 21000]]) == 21000                      # as published: never equal text
    assert amt([["relays", "wss://x"]]) is None and amt("amount") is None
    assert N.zap_request_amount(req({"tags": [["amount", "1"]]})) is None   # no pubkey: no parse
    assert N.zap_request_amount(req(["amount", "1"])) is None
    assert N.zap_request_amount([["description", "{bad"]]) is None


def test_provider_signers_come_from_the_lightning_loader_parser(tmp_path):
    """ref 6.3: every key of every provider's `signers` list, read by the /lightning loader's
    own parser (lowercased, validated); an empty list or a malformed file fails loud."""
    pytest.importorskip("yaml")      # PyYAML is in the Dagster image, not in the workstation test venv
    p = tmp_path / "signers.yaml"
    p.write_text("providers:\n- domain: a.example\n  signers:\n  - " + "AB" * 32 + "\n  - "
                 + "cd" * 32 + "\n- domain: b.example\n  signers: ['" + "ef" * 32 + "']\n")
    assert N.load_provider_signers(str(p)) == frozenset({"ab" * 32, "cd" * 32, "ef" * 32})
    for bad in ("providers: []\n", "other: 1\n",
                "providers:\n- domain: x\n  signers: ['" + "ab" * 31 + "']\n",
                "providers:\n- domain: x\n  signers: []\n",
                "providers:\n- domain: x\n  signers: ['" + "ab" * 32 + "']\n"
                "- domain: y\n  signers: ['" + "AB" * 32 + "']\n"):
        p.write_text(bad)
        with pytest.raises(ValueError):
            N.load_provider_signers(str(p))
    with pytest.raises(OSError):
        N.load_provider_signers(str(tmp_path / "missing.yaml"))


def test_the_shipped_provider_signer_file_loads():
    pytest.importorskip("yaml")      # PyYAML is in the Dagster image, not in the workstation test venv
    keys = N.load_provider_signers()
    assert keys and all(N._HEX64_RE.fullmatch(k) and k == k.lower() for k in keys)


def test_n8b_is_no_reachable_clone_value_and_no_state():
    """ref 2.2 N8b (review 4 m3): the hostless `todo`, `testing`, `.` reach nothing, like a
    private host; reachability is judged with the GRASP host set irrelevant."""
    s, d = build([
        ann(PK1, "junk", D(9), clone=("todo", "testing", ".")),                 # N8b
        ann(PK2, "junk", D(9), clone=("todo", "testing", ".")),                 # with a state: kept
        ann(PK3, "priv", D(9), clone=("http://localhost/x.git",)),              # N8a comes first
        ann(PK4, "mixed", D(9), clone=("todo", "http://10.0.0.1/x.git")),       # misses N8a: N8b
        ann(PK5, "nostr", D(9), clone=("gnostr-cloud://git.example.org/r",)),   # non-network: N8b
        ann(PK6, "ok", D(9), clone=("todo", "https://git.example.org/x.git")),  # one reachable
        ann(PK7, "ht", D(9), clone=("htree://npub1abc/x",)),                    # hashtree reaches
        ann(PK8, "scp", D(9), clone=("git@git.example.org:x.git",)),            # scp-like reaches
        ann(PK9, "g", D(9), clone=(grasp_url(),)),                              # relay.ngit.dev joins
        # a non-network scheme on a GRASP host: `grasp` for the hosting class, but not
        # reachable for N8b, which never reads the set (N8b without a state, kept with one)
        ann(pk(10), "ng", D(9), clone=("nostr://relay.ngit.dev/x",)),
        ann(pk(11), "ng", D(9), clone=("nostr://relay.ngit.dev/y",)),
    ], [st(PK2, "junk", D(8)), st(pk(11), "ng", D(8))])
    got = {(r["pubkey"], r["d"]): r["family"] for r in s["repos_rows"]}
    assert [got[(p, x)] for p, x in ((PK1, "junk"), (PK2, "junk"), (PK3, "priv"), (PK4, "mixed"),
                                     (PK5, "nostr"), (PK6, "ok"), (PK7, "ht"), (PK8, "scp"),
                                     (PK9, "g"), (pk(10), "ng"), (pk(11), "ng"))] == [
        "N8b", "repo", "N8a", "N8b", "N8b", "repo", "repo", "repo", "repo", "N8b", "repo"]
    assert row(s, PK2, "junk")["cls"] == "none" and row(s, pk(11), "ng")["cls"] == "grasp_only"
    assert [N.url_reachable(N.parse_url(u)) for u in ("todo", "http://localhost/x", "htree://a/b",
                                                        "nostr://relay.ngit.dev/x", GITHUB)] == [
        False, False, True, False, True]
    assert "no public clone URL" in [e["why"] for e in d["exclusions"] if e["k"] == "N8b"][0]
    assert N.check_invariants(s, d) == []


def test_names_lose_control_and_format_characters():
    """ref 5.1 / 9 (web review 4): a bidi override or a zero-width character in one
    third-party name must not reverse or hide its neighbours on the page."""
    evil = "ab\u202ecd\u200bef\x00\tg\ufeff"
    assert N.clean_text(evil) == "abcdefg"
    assert N.clean_text("Plain name - ok, caf\u00e9") == "Plain name - ok, caf\u00e9"
    assert N.display_name("\u202eAlice\u200f", None) == "Alice"
    assert N.display_name("\u2066\u200b ", "bob") == "bob"          # nothing left: the name
    s, d = build([ann(PK1, "r", D(9), name=evil), ann(PK2, "p", D(9), name="Plain"),
                  ann(PK3, "\u202ex", D(8), name="\u200b")],
                 names={PK1: "\u202eEve\u200b"})
    assert row(s, PK1, "r")["name"] == "abcdefg" and row(s, PK2, "p")["name"] == "Plain"
    assert row(s, PK3, "\u202ex")["name"] == ""                      # d is stored as is
    new = {r["d"]: (r["name"], r["lead_name"]) for r in d["repos_new"]}
    assert new == {"r": ("abcdefg", "Eve"), "p": ("Plain", None), "\u202ex": ("x", None)}
    assert N.check_invariants(s, d) == []


def test_an_empty_client_is_no_client():
    """ref 4.5 (review 4 n5): a client value that is empty after trimming falls through to the
    no-client branches instead of labelling a tool ""."""
    s, d = build([ann(PK1, "a", D(9), extra=(["client", ""], ["alt", "git repository: a"])),
                  ann(PK2, "b", D(9), extra=(["client", " \t "],), topics=("shakespeare",)),
                  ann(PK3, "c", D(9), extra=(["client", " Tool "],))])
    assert [row(s, p, x)["tool"] for p, x in ((PK1, "a"), (PK2, "b"), (PK3, "c"))] == [
        "ngit / GitWorkshop (inferred)", "Shakespeare", " tool "]
    assert "" not in kv(d["tools"])


def test_a_port_is_removed_only_at_the_very_end_of_the_authority():
    """ref 4.1 (review 4 n1): a value is trimmed (str.strip) but an INTERIOR newline stays,
    and `:port` is removed only when it ends the authority - `$` also matches before a
    trailing newline, which once gave the host `h.example\\n`."""
    u = f"https://h.example:80\n/{NPUB_SHAPE}/x.git"
    assert N.parse_url(u) == ("https", False, "h.example:80\n", f"/{NPUB_SHAPE}/x.git")
    assert N.parse_url("https://h.example:80\n")[2] == "h.example:80\n"
    assert N.parse_url("https://h.example:80")[2] == "h.example"
    (v,) = N.clean_values([["clone", " https://h.example:80\n "]], "clone")    # trimmed first
    assert N.parse_url(v)[2] == "h.example"


def _flood_world():
    """_world() plus what the roster reduction must survive: an N5 flooder coordinate of 1,000
    versions with an orphan state coordinate of 1,000 versions; a coordinate whose NEWEST row
    lies on the run's own partial day (picking the newest before the horizon keeps the wrong
    tags), and others with future-dated and pre-floor rows; two versions in one second (the
    lowest id is current); an id duplicated with different tags (the first occurrence wins); a
    coordinate kept only because it has a state (N8b without it); a state outside the horizon
    at a member coordinate; an orphan state of an excluded coordinate; a deletion request; a
    star through an OLD version's id."""
    anns, states, comments = _world_rows()
    t0 = D(40)
    flood = [ann(PK7, "bot", t0 + 60 * i, name=f"v{i}", clone=(f"https://git.example.org/{i}.git",))
             for i in range(1000)]
    orphan = [st(PK7, "bot", t0 + 60 * i, {"refs/heads/main": f"{i:040x}"}) for i in range(1000)]
    old = ann(PK1, "r", D(250), maint=(PK2,), clone=(grasp_url(),), name="old")
    dup = ann(PK8, "dup", D(10), name="first")
    anns = anns + flood + [
        old,
        ann(PK8, "late", D(20), name="kept version"),
        ann(PK8, "late", AS_OF_MIDNIGHT + DAY + 60, name="partial day", clone=()),
        ann(PK8, "fut", D(15)), ann(PK8, "fut", NOW + 3600, clone=()),
        ann(PK8, "past", N.PAST_FLOOR - 5, clone=()), ann(PK8, "past", D(12)),
        ann(PK8, "only_future", NOW + 99),
        ann(PK8, "tie", D(11), name="high id", eid="b" * 64),
        ann(PK8, "tie", D(11), name="low id", eid="a" * 64),
        dup, dict(dup, tags=[["d", "dup"], ["name", "second"], ["clone", "todo"]]),
        ann(PK9, "stateful", D(10), clone=()),
        # ref 3's hashtree pushes read every version's id and created_at: the reduction keeps both
        ann(PK3, "s", D(30), clone=("htree://npub1abc/s",)),
        ann(PK3, "s", D(6), clone=("htree://npub1abc/s",)),
    ]
    states = states + orphan + [
        st(PK9, "stateful", D(9), {"refs/heads/main": C1}),
        st(PK9, "stateful", D(8), {"refs/heads/main": C2, "refs/tags/v1": C3}),
        st(PK1, "r", NOW + 3600, {"refs/heads/main": C4}),
        st(SK1, "v", D(4), {"refs/heads/main": C4}),
        st(PK5, "u", D(2), {"refs/heads/main": C1}),
    ]
    reactions = [reaction(PK6, D(3), e=old["id"]), reaction(PK9, D(3), a=f"30617:{PK8}:late")]
    dels = [dele(PK5, f"30617:{PK5}:u", D(1)), dele(PK8, f"30617:{PK8}:fut", D(20))]
    return anns, states, comments, reactions, dels


def test_the_roster_reduction_changes_nothing():
    """docs/design/ngit-design.md section 9 MEMORY (review 4 M2): build_snapshot over the reduced
    roster rows equals build_snapshot over the full rows, while the flooder keeps the tags
    of ONE version and its orphan state coordinate none."""
    anns, states, comments, reactions, dels = _flood_world()
    full = build(anns, states, comments=comments, reactions=reactions, dels=dels)
    red_anns, red_states, stats = N.reduce_roster(anns, states, dels, NOW)
    reduced = build(red_anns, red_states, comments=comments, reactions=reactions, dels=dels)
    assert reduced == full
    assert N.kept_maintainers(red_anns, red_states, dels, NOW) == N.kept_maintainers(anns, states, dels, NOW)
    s, d = full
    assert fam(s, PK7, "bot") == "N5" and row(s, PK7, "bot")["versions"] == 1000
    assert fam(s, PK8, "late") == "repo" and row(s, PK8, "late")["name"] == "kept version"
    assert row(s, PK8, "dup")["name"] == "first" and fam(s, PK9, "stateful") == "repo"
    assert row(s, PK8, "tie")["name"] == "low id"
    assert fam(s, PK5, "u") == "N6" and d["summary"]["stars_total"] == 2
    assert len(pushes_of(s, PK9, "stateful")) == 2
    # what the reduction kept: every row, one tagged version per coordinate with a version in
    # the horizon, tagged states only at member coordinates inside the horizon
    assert (stats["announce_rows"], stats["state_rows"]) == (len(anns) - 1, len(states))
    in_horizon = {(r["pubkey"], N.coord_d(r["tags"])) for r in anns
                  if N.PAST_FLOOR <= r["created_at"] <= N.as_of_bounds(NOW)[1]}
    assert stats["announce_tagged"] == len(in_horizon)
    assert stats["state_tagged"] == sum(1 for r in s["push_rows"] if r["source"] == N.SOURCE_STATE)
    assert [r["created_at"] for r in pushes_of(s, PK3, "s")] == [D(30), D(6)]
    assert sum(1 for r in red_anns if r["pubkey"] == PK7 and r["tags"] != [["d", "bot"]]) == 1
    assert all(r["tags"] == [["d", "bot"]] for r in red_states if r["pubkey"] == PK7)
    assert all(set(r) == {"id", "pubkey", "created_at", "tags"} for r in red_anns + red_states)


def test_load_roster_streams_the_parts(tmp_path):
    import gzip
    anns, states, _comments, _reactions, dels = _flood_world()
    for label, rows in (("a", anns), ("s", states)):
        for i in range(2):
            with gzip.open(tmp_path / f"{label}{i}.gz", "wt") as fh:
                fh.write("\n".join(json.dumps(r) for r in rows[i::2]) + "\n")
    got = N.load_roster(sorted(tmp_path.glob("a*.gz")), sorted(tmp_path.glob("s*.gz")), dels, NOW)
    # the parts are read in path order, so the rows arrive as rows[0::2] + rows[1::2]
    want = N.reduce_roster(anns[0::2] + anns[1::2], states[0::2] + states[1::2], dels, NOW)
    assert got == want
    assert isinstance(N.iter_ndjson_gz([]), type(x for x in ()))


def test_the_coordinate_reads_the_first_d_tag():
    """ref 1.3 (review 4 #4)."""
    assert N.coord_d([["d", "x"], ["d", "y"]]) == "x"
    assert N.coord_d([["d"], ["d", "y"]]) == ""          # the first d tag has no value
    assert N.coord_d([["name", "n"]]) == ""
    s, _ = build([ann(PK1, "x", D(9), extra=(["d", "y"],))],
                 [st(PK1, "x", D(8), extra=(["d", "y"],))])
    assert [r["d"] for r in s["repos_rows"]] == ["x"] and row(s, PK1, "x")["has_state"]


def test_euc_skips_an_r_tag_that_meets_only_one_condition():
    """ref 2.1 (review 4 #5): the first pass wants a third element `euc` AND 40 lowercase hex;
    the second pass wants EXACTLY two elements."""
    euc = lambda tags: N.announcement_attrs({"pubkey": PK1, "tags": tags})["euc"]   # noqa: E731
    assert euc([["r", C1, "x"], ["r", C2]]) == C2
    assert euc([["r", "ab" * 20, "euc"], ["r", "AB" * 20, "euc"]]) == "ab" * 20
    assert euc([["r", "AB" * 20, "euc"], ["r", C3]]) == C3
    assert euc([["r", C1, "x"]]) is None
    assert euc([["r", C2], ["r", C1, "euc"]]) == C1                   # the euc marker first


def test_state_republish_loop_median_gap_boundary():
    """ref 3 (review 4 #6, #7): 1,000 versions with a median gap BELOW 120 s is a loop, 120 s
    is not (the old fixture's 60 s gap pinned neither side)."""
    t0 = D(100)
    at119 = [st(PK1, "r", t0 + 119 * i, {"refs/heads/main": f"{i:040x}"}) for i in range(1000)]
    at120 = [st(PK2, "q", t0 + 120 * i, {"refs/heads/main": f"{i:040x}"}) for i in range(1000)]
    s, d = build([ann(PK1, "r", D(120)), ann(PK2, "q", D(120))], at119 + at120)
    assert [e["v"] for e in d["exclusions"] if e["k"] == "state_republish_loop"] == [1]
    assert pushes_of(s, PK1, "r") == [] and len(pushes_of(s, PK2, "q")) == 1000
    assert N.check_invariants(s, d) == []


def test_repos_new_holds_repositories_first_seen_on_or_after_the_wall():
    """ref 5.1 (review 4 #15)."""
    s, d = build([ann(PK1, "pre", N.ANN_WALL - 1), ann(PK2, "at", N.ANN_WALL),
                  ann(PK3, "post", D(9))])
    assert [r["d"] for r in d["repos_new"]] == ["post", "at"]
    assert d["summary"]["repos"] == 3


def test_each_first_run_floor_on_its_own():
    """docs/design/ngit-design.md section 9 step 5 (review 4 #16, #17)."""
    assert N.shrink_gate(None, _gate_doc(3500, 12000), override_until=None) == ([], False)
    assert N.shrink_gate(None, _gate_doc(3499, 12000), override_until=None)[0] == [
        "announcements_kept 3499 < first-run floor 3500"]
    assert N.shrink_gate(None, _gate_doc(3500, 11999), override_until=None)[0] == [
        "pushes_total 11999 < first-run floor 12000"]


def test_a_duplicated_id_counts_once():
    """ref 1.1 (review 4 #19): the extracts dedup ids; the build keeps the first occurrence
    again, for every kind."""
    a = ann(PK1, "r", D(9))
    s0 = st(PK1, "r", D(8))
    c = comment(PK2, D(3))
    r = reaction(PK3, D(5), a=f"30617:{PK1}:r")
    x = ext_reaction(PK5, D(5), i=f"30617:{PK1}:r")
    z = zap(D(5), a=f"30617:{PK1}:r", bolt11="lnbc1u1x")
    g = {"id": nid(), "pubkey": PK4, "kind": 10317, "created_at": D(4), "content": "",
         "tags": [["g", "wss://grasp.example.net"]], "sig": ""}
    s, d = build([a, dict(a, tags=a["tags"] + [["name", "later copy"]])], [s0, dict(s0)],
                 grasp=[g, dict(g)], comments=[c, dict(c)], reactions=[r, dict(r)], zaps=[z, dict(z)],
                 ext=[x, dict(x, content="-")])            # the second copy of the id is dropped
    ae = d["summary"]["archive_events"]
    assert [ae[k] for k in ("announcements", "states", "grasp_lists", "comments", "reactions",
                            "external_reactions", "zaps")] == [1] * 7
    assert (d["summary"]["stars_total"], d["summary"]["stars_iris"]) == (2, 1)
    assert row(s, PK1, "r")["versions"] == 1 and row(s, PK1, "r")["name"] == ""
    assert (d["summary"]["zap_receipts"], d["summary"]["comments_total"]) == (1, 1)


def test_weekly_and_30d_maintainers_read_every_push():
    """ref 3 (review 4 #24, #25): weekly `pushes`, `maintainers` and `maintainers_active_30d`
    read every `push` row - a member pushing after the repository's 50th push of the day
    counts (under the old daily cap its pushes did not)."""
    t0 = D(3, 0)
    anns = [ann(PK1, "r", D(60), maint=(PK2,)), ann(PK2, "r", D(59), maint=(PK1,))]
    states = [st(PK1, "r", t0 + i, {"refs/heads/main": f"{i:040x}"}) for i in range(50)]
    states += [st(PK2, "r", t0 + 100 + i, {"refs/heads/main": f"{500 + i:040x}"}) for i in range(3)]
    s, d = build(anns, states)
    assert len(pushes_of(s, PK2, "r")) == 3
    assert d["summary"]["pushes_total"] == 53 and d["summary"]["maintainers_active_30d"] == 2
    assert d["pushes_weekly"][-1] == {"w": "2026-09-21", "pushes": 53, "repos": 1, "maintainers": 2}
    assert N.check_invariants(s, d) == []


def test_recency_reads_the_newest_state_not_the_newest_push():
    """ref 3 (review 4 #32): a push 100 days ago and an identical republish 5 days ago - the
    repository was last seen 5 days ago."""
    s, d = build([ann(PK1, "r", D(200))], [st(PK1, "r", D(100), {"refs/heads/main": C1}),
                                           st(PK1, "r", D(5), {"refs/heads/main": C1})])
    r = row(s, PK1, "r")
    assert (r["last_push"], r["last_state"]) == (D(100), D(5))
    assert kv(d["recency"]) == {"30d": 1, "90d": 0, "180d": 0, "365d": 0, "older": 0}


def test_the_newest_state_is_the_lowest_id_of_the_newest_second():
    """ref 3 (review 4 #33): branches, tags and head come from the maximum (created_at, -id)."""
    s, d = build([ann(PK1, "r", D(50))], [
        st(PK1, "r", D(10), {"refs/heads/main": C1}, eid="b" * 64),
        st(PK1, "r", D(10), {"refs/heads/main": C2, "refs/heads/dev": C1, "refs/heads/x": C3},
           eid="a" * 64),
    ])
    assert d["repos_active"][0]["branches"] == 3
    assert kv(d["branches_per_repo"]) == {"0": 0, "1": 0, "2-5": 1, "6-20": 0, "21+": 0}


def test_name_is_the_first_name_tag_even_without_a_value():
    """ref 2.1 (review 4 #38): a later name tag is not consulted."""
    assert N.announcement_attrs({"pubkey": PK1, "tags": [["name"], ["name", "X"]]})["name"] == ""
    s, d = build([ann(PK1, "r", D(9), extra=(["name"], ["name", "X"]))])
    assert row(s, PK1, "r")["name"] == "" and d["repos_new"][0]["name"] == "r"   # d fallback


def test_grasp_list_values_strip_only_the_ws_schemes():
    """ref 5.2 (review 4 #41): an `https://` value keeps its scheme, so its host `https:` has
    no dot and the value is not valid."""
    assert N.grasp_list_value("https://relay.ngit.dev") is None
    assert N.grasp_list_value("wss://Relay.ngit.dev/") == ("relay.ngit.dev", "relay.ngit.dev")
    assert N.grasp_list_value("relay.ngit.dev/x") == ("relay.ngit.dev/x", "relay.ngit.dev")
    g = {"id": nid(), "pubkey": PK4, "kind": 10317, "created_at": D(4), "content": "",
         "tags": [["g", "https://relay.ngit.dev"]], "sig": ""}
    assert build([ann(PK1, "r", D(9))], grasp=[g])[1]["summary"]["grasp_list_users"] == 0


def test_init_sql_creates_the_columns_the_pipeline_writes():
    """schema/init.sql creates ngit_repos and ngit_pushes on a fresh database, and write_pg alters
    a live one under its TRUNCATE lock (section 9): both must give the columns the inserts name
    (REPO_COLUMNS, PUSH_COLUMNS), and init.sql the same `source` default and CHECK as write_pg's
    ALTER (2026-10-12), so a fresh database and a live one hold the same table."""
    schema = open(os.path.join(os.path.dirname(N.__file__), "..", "..", "schema", "init.sql")).read()

    def columns(table):
        block = schema.split(f"CREATE TABLE IF NOT EXISTS {table} (")[1].split("\n);")[0]
        return tuple(line.split()[0] for line in block.splitlines()
                     if line.strip() and not line.strip().startswith(("--", "CHECK", "PRIMARY KEY")))

    assert columns("ngit_repos") == N.REPO_COLUMNS
    assert columns("ngit_pushes") == N.PUSH_COLUMNS
    pushes = schema.split("CREATE TABLE IF NOT EXISTS ngit_pushes (")[1].split("\n);")[0]
    assert "source           TEXT     NOT NULL DEFAULT 'state'" in pushes
    assert "CHECK (source IN ('state', 'htree_announcement'))" in pushes
    assert "branches         INTEGER," in pushes and "tags             INTEGER," in pushes
