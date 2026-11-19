#!/usr/bin/env python3
"""Check the declared lineage (dagster_code/pipeline/lineage.py) against the consumers' own code.

The Discord failure post says who a failed run leaves stale by walking the lineage graph, so a
consumer whose declared reads drift from what it really queries would be left out of an alert,
or named in one it should not be. This re-resolves the reads from source and reports every
difference:

  web pages     the route files under nostrology-web src/routes/(app)/<dir>/, through the data
                functions they call and the SQL constants and helpers those use
  landing       the data function each landing panel reads (src/lib/server/landing/sources.ts)
  mcp           the tables named in the MCP modules (nostrology_mcp/metric.py, reference.py),
                compared for all MCP domains together: a table is not tied to one domain in code

It also reports a route that reads serving tables but belongs to no consumer (unless listed in
lineage.NOT_CONSUMERS) and a read of a table no Dagster asset writes. Exit 0 when consistent.

    python3 scripts/check_lineage.py [--web ../nostrology-web]

Pure standard library: reads source text, runs nothing from either repo.
"""

import argparse
import glob
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from dagster_code.pipeline import lineage  # noqa: E402  (pure module, no dagster)


def strip_ts_comments(s: str) -> str:
    s = re.sub(r"/\*.*?\*/", " ", s, flags=re.S)
    return re.sub(r"(?m)(^|[^:'\"`\\])//[^\n]*", r"\1", s)


def strip_py_comments(s: str) -> str:
    return re.sub(r"(?m)#[^\n]*", "", s)


def mentions(text: str, tables) -> set:
    return {t for t in tables if re.search(r"\b%s\b" % re.escape(t), text)}


_DECL = re.compile(r"(?m)^(?:export\s+)?(?:default\s+)?(?:async\s+)?(?:function\*?|const|let|class)\s+([A-Za-z0-9_$]+)")
_IMPORT = re.compile(r"import\s+(?!type\s)(?:([A-Za-z0-9_$]+)\s*,?\s*)?(?:\{([^}]*)\})?\s*from\s*['\"]([^'\"]+)['\"]")
_REEXPORT = re.compile(r"export\s*\{([^}]*)\}\s*from\s*['\"]([^'\"]+)['\"]|export\s*\*\s*from\s*['\"]([^'\"]+)['\"]")


def _word(name: str, text: str) -> bool:
    return re.search(r"(?<![\w$])%s(?![\w$])" % re.escape(name), text) is not None


def _resolve(spec: str, from_file: str, web_root: str):
    """'$lib/x' or './x' -> the .ts file it names, or None for anything else (packages)."""
    if spec.startswith("$lib/"):
        base = os.path.join(web_root, "src", "lib", spec[5:])
    elif spec.startswith("."):
        base = os.path.normpath(os.path.join(os.path.dirname(from_file), spec))
    else:
        return None
    base = re.sub(r"\.js$", "", base)
    for cand in (base + ".ts", os.path.join(base, "index.ts")):
        if os.path.isfile(cand):
            return os.path.normpath(cand)
    return None


def _names(group: str) -> list:
    """'a, b as c, type d' -> [(local, exported)] for the value imports in an import list."""
    out = []
    for part in (group or "").split(","):
        part = part.strip()
        if not part or part.startswith("type "):
            continue
        exported, _, local = part.partition(" as ")
        out.append(((local or exported).strip(), exported.strip()))
    return out


class WebIndex:
    """Every top-level declaration (functions, arrow helpers such as notExcludedMint, SQL
    constants) in nostrology-web src/lib, scoped by file. A declaration's block runs to the next
    one, comments stripped; it reads the tables it names, plus the reads of every same-file
    declaration and every IMPORTED name it mentions, so SQL assembled from helpers resolves while
    a local variable that happens to share a name with another file's export does not."""

    def __init__(self, web_root: str, tables):
        self.web_root, self.tables = web_root, tables
        self.reads = {}       # (file, name) -> tables
        self.imports = {}     # file -> {local name: (file, exported name)}
        lib = [os.path.normpath(f) for f in glob.glob(os.path.join(web_root, "src", "lib", "**", "*.ts"),
                                                      recursive=True) if ".test." not in f and "/test" not in f]
        blocks = {}
        for f in lib:
            t = strip_ts_comments(open(f, encoding="utf-8").read())
            self.imports[f] = self._imports_of(f, t)
            starts = [(m.start(), m.group(1)) for m in _DECL.finditer(t)]
            for i, (pos, name) in enumerate(starts):
                blocks[(f, name)] = t[pos: starts[i + 1][0] if i + 1 < len(starts) else len(t)]
        by_file = {}
        for (f, name) in blocks:
            by_file.setdefault(f, set()).add(name)
        refs = {}
        for (f, name), body in blocks.items():
            r = {(f, o) for o in by_file[f] if o != name and _word(o, body)}
            r |= {self._follow(tgt) for local, tgt in self.imports[f].items() if _word(local, body)}
            refs[(f, name)] = {x for x in r if x in blocks}
            self.reads[(f, name)] = mentions(body, tables)
        for _ in range(12):  # helpers naming helpers
            changed = False
            for key, rs in refs.items():
                add = set().union(*(self.reads[o] for o in rs)) - self.reads[key] if rs else set()
                if add:
                    self.reads[key] |= add
                    changed = True
            if not changed:
                break

    def _imports_of(self, f: str, t: str) -> dict:
        out = {}
        for m in _IMPORT.finditer(t):
            target = _resolve(m.group(3), f, self.web_root)
            if not target:
                continue
            if m.group(1):
                out[m.group(1)] = (target, "default")
            for local, exported in _names(m.group(2)):
                out[local] = (target, exported)
        return out

    def _follow(self, tgt, depth: int = 0):
        """Chase re-exports (index.ts barrels) to the file that declares the name."""
        f, name = tgt
        if (f, name) in self.reads or depth > 5 or not os.path.isfile(f):
            return tgt
        t = strip_ts_comments(open(f, encoding="utf-8").read())
        for m in _REEXPORT.finditer(t):
            if m.group(2):
                for local, exported in _names(m.group(1)):
                    if local == name:
                        nxt = _resolve(m.group(2), f, self.web_root)
                        if nxt:
                            return self._follow((nxt, exported), depth + 1)
            elif m.group(3):
                nxt = _resolve(m.group(3), f, self.web_root)
                if nxt and (nxt, name) in self.reads:
                    return (nxt, name)
        if name in self.imports.get(f, {}):
            return self._follow(self.imports[f][name], depth + 1)
        return tgt

    def symbol(self, f: str, name: str) -> set:
        """The reads of a declaration, found by name anywhere in src/lib (landing functions)."""
        hits = [ts for (ff, n), ts in self.reads.items() if n == name]
        return set().union(*hits) if hits else set()

    def file_reads(self, path: str) -> set:
        """What a route file reads: tables it names itself, plus everything its value imports
        from src/lib read."""
        path = os.path.normpath(path)
        t = strip_ts_comments(open(path, encoding="utf-8").read())
        hit = mentions(t, self.tables)
        for _local, tgt in self._imports_of(path, t).items():
            hit |= self.reads.get(self._follow(tgt), set())
        return hit


def check(web_root: str) -> list:
    problems = list(lineage.problems())
    produced = set(lineage.table_producers())
    init = open(os.path.join(ROOT, "schema", "init.sql"), encoding="utf-8").read()
    every_table = {m.group(1).lower() for m in re.finditer(
        r"CREATE\s+(?:TABLE|MATERIALIZED VIEW|VIEW)\s+(?:IF NOT EXISTS\s+)?([a-z_][a-z0-9_]*)", init, re.I)}
    index = WebIndex(web_root, every_table)
    routes_top = os.path.join(web_root, "src", "routes")
    routes_root = os.path.join(routes_top, "(app)")

    covered, landing_decl = set(), set()
    for c in lineage.CONSUMERS:
        cid = lineage.consumer_id(c["key"])
        src = c.get("sources", {})
        got = set()
        for d in src.get("routes", ()):
            base = os.path.normpath(os.path.join(routes_root, d))
            files = [f for f in glob.glob(os.path.join(base, "**", "*.ts"), recursive=True) if ".test." not in f]
            if not files:
                problems.append(f"{cid}: no route files under {os.path.relpath(base, routes_top)}")
            for f in files:
                covered.add(os.path.normpath(f))
                got |= index.file_reads(f)
        for fn in src.get("landing", ()):
            found = index.symbol(None, fn)
            if not found:
                problems.append(f"{cid}: landing function {fn} reads nothing (or is not in src/lib)")
            got |= found
            landing_decl |= set(c["reads"])
        if "routes" in src or "landing" in src:
            declared = set(c["reads"])
            for t in sorted(got - declared):
                problems.append(f"{cid}: reads {t} in code but does not declare it")
            for t in sorted(declared - got):
                problems.append(f"{cid}: declares {t} but its code does not read it")

    # The route(s) that render the landing board read exactly what its panels declare.
    for rel in lineage.LANDING_ROUTES:
        f = os.path.normpath(os.path.join(routes_top, rel))
        covered.add(f)
        extra = index.file_reads(f) - landing_decl
        if extra:
            problems.append(f"landing route {rel} reads {', '.join(sorted(extra))}, which no landing panel declares")

    all_routes = glob.glob(os.path.join(routes_top, "**", "*.ts"), recursive=True)
    for f in sorted(all_routes):
        if ".test." in f or os.path.normpath(f) in covered:
            continue
        got = index.file_reads(f)
        rel = os.path.relpath(f, routes_top)
        if got and not any(rel.startswith(k) for k in lineage.NOT_CONSUMERS):
            problems.append(f"route {rel} reads {', '.join(sorted(got))} but belongs to no consumer")

    mcp_decl, mcp_mods = set(), set()
    for c in lineage.CONSUMERS:
        if "mcp" in c.get("sources", {}):
            mcp_decl |= set(c["reads"])
            mcp_mods |= set(c["sources"]["mcp"])
    mcp_got = set()
    for m in sorted(mcp_mods):
        p = os.path.join(ROOT, "nostrology_mcp", m)
        mcp_got |= mentions(strip_py_comments(open(p, encoding="utf-8").read()), every_table)
    for t in sorted(mcp_got - mcp_decl):
        problems.append(f"mcp: {t} is read by {', '.join(sorted(mcp_mods))} but no MCP consumer declares it")
    for t in sorted(mcp_decl - mcp_got):
        problems.append(f"mcp: an MCP consumer declares {t} but the MCP modules do not read it")

    for c in lineage.CONSUMERS:
        for t in c["reads"]:
            if t not in produced:
                problems.append(f"{lineage.consumer_id(c['key'])}: {t} is not written by any Dagster asset")
    return problems


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--web", default=os.path.join(os.path.dirname(ROOT), "nostrology-web"),
                    help="the nostrology-web checkout (default: the sibling directory)")
    args = ap.parse_args(argv)
    if not os.path.isdir(os.path.join(args.web, "src", "routes")):
        print(f"check_lineage: no nostrology-web checkout at {args.web}", file=sys.stderr)
        return 2
    problems = check(args.web)
    for p in problems:
        print(f"DRIFT {p}")
    if problems:
        print(f"check_lineage: {len(problems)} difference(s) between lineage.py and the code")
        return 1
    print(f"check_lineage: consistent ({len(lineage.CONSUMERS)} consumers)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
