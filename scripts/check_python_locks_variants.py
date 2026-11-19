#!/usr/bin/env python3
"""Mutation variants for scripts/check_python_locks.py.

Each variant edits a scratch copy of the working tree (tracked and untracked files, not ignored
ones), runs the guard on the copy and compares its exit code with the expected one: 0 for the
controls, 1 for every bypass. The working tree itself is never modified. The S1/S2/N1-N4 names
are findings of the reviews of nostrology-data#7 (Lock the full Python dependency trees of the
three images (mcp, Dagster, pipeline)).

Run it after any change to the guard, a registered Dockerfile or a compose file:

    /usr/bin/python3 scripts/check_python_locks_variants.py

It runs the guard under /usr/bin/python3 when present (an operating system's own Python, which
can be as old as 3.9, the oldest the guard must support), else under the interpreter running it.
A variant whose anchor text is gone from the tree reports STALE and fails the run: move the
anchor, do not drop the variant. Run it as a normal user; the unlistable-directory variant
cannot fail as root.
"""
import os
import re
import shutil
import subprocess
import sys
import tempfile

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = "/usr/bin/python3" if os.path.exists("/usr/bin/python3") else sys.executable
T = tempfile.mkdtemp(prefix="guard-variants-")
TREE = os.path.join(T, "tree")
files = subprocess.run(["git", "-C", REPO, "ls-files", "-co", "--exclude-standard", "-z"],
                       capture_output=True, check=True).stdout.decode().split("\0")
for rel in filter(None, files):
    src, dst = os.path.join(REPO, rel), os.path.join(TREE, rel)
    if os.path.isfile(src):
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(src, dst)

saved, created, modes = {}, [], {}


def p(rel):
    return os.path.join(TREE, rel)


def text(rel):
    return open(p(rel), encoding="utf-8", newline="").read()


def put(rel, content):
    if os.path.exists(p(rel)):
        saved.setdefault(rel, open(p(rel), "rb").read())
    else:
        created.append(rel)
        os.makedirs(os.path.dirname(p(rel)), exist_ok=True)
    with open(p(rel), "w", encoding="utf-8", newline="") as f:
        f.write(content)


def sub(rel, old, new):
    t = text(rel)
    assert old in t, "%s: %r not found" % (rel, old)
    put(rel, t.replace(old, new, 1))


def chmod(rel, mode):
    modes.setdefault(rel, os.stat(p(rel)).st_mode)
    os.chmod(p(rel), mode)


def restore():
    for rel, mode in modes.items():
        os.chmod(p(rel), mode)
    for rel, data in saved.items():
        open(p(rel), "wb").write(data)
    for rel in created:
        os.remove(p(rel))
    saved.clear()
    created.clear()
    modes.clear()


MCP, DAG = "deploy/Dockerfile.mcp", "deploy/dagster/Dockerfile.dagster"
# The deployment's compose files left this tree on 2026-11-16, and are held to the guard's compose
# rules where they live; a compose file in this tree is still checked. So the S2 variants edit one of
# their own, EXTRA, which builds a registered Dockerfile as the deployment's do, beside a pulled image.
EXTRA = "deploy/compose.variant.yml"
EXTRA_BASE = (
    "# A compose file of the variants' own\n"
    "services:\n"
    "  x:\n"
    "    build:\n"
    "      context: ..\n"
    "      dockerfile: deploy/Dockerfile.mcp\n"
    "      args:\n"
    "        GIT_SHA: ${T:-unknown}\n"
    "    image: nostrology-mcp:${T:-dev}\n"
    "    container_name: x\n"
    "    environment:\n"
    "      A: \"1\"\n"
    "  pg:\n"
    "    image: postgres:16\n")


def extra(old=None, new=None):
    """Writes EXTRA from EXTRA_BASE, with one edit."""
    def edit():
        body = EXTRA_BASE
        if old is not None:
            assert old in body, "%s: %r not found" % (EXTRA, old)
            body = body.replace(old, new, 1)
        put(EXTRA, body)
    return edit
CANON = "RUN pip install --no-cache-dir --require-hashes --only-binary :all: -r /tmp/mcp-requirements.txt \\\n    && pip check\n"
W = "WORKDIR /app\n"


def before_workdir(line):
    return lambda: sub(MCP, W, line + W)


def canonical_edit(old, new):
    return lambda: sub(MCP, old, new)


def base_edit(new):
    """Replaces the whole FROM line. Every base has been pinned by digest since 2026-10-26, so an
    edit of the tag alone leaves that digest behind whatever it writes, and the variant then tests
    a line nobody meant."""
    def edit():
        t = text(MCP)
        m = re.search(r"^FROM python:3\.12-slim-bookworm(?:@sha256:[0-9a-f]{64})?$", t, re.M)
        assert m, "%s: no FROM line of the allowed base" % MCP
        put(MCP, t.replace(m.group(0), new, 1))
    return edit


SEPS = [("CR", "\r"), ("FF", "\x0c"), ("VT", "\x0b"), ("FS", "\x1c"), ("GS", "\x1d"), ("RS", "\x1e"),
        ("NEL", "\x85"), ("LS", "\u2028"), ("PS", "\u2029")]
V = [("baseline: unmodified", 0, lambda: None)]
V += [("S1 separator %s hides && pip install" % n, 1, before_workdir("RUN echo ok" + s + "&& pip install requests\n")) for n, s in SEPS]
V += [
    ("control: plain RUN echo", 0, before_workdir("RUN echo ok\n")),
    ("S1 BOM + escape directive", 1, lambda: put(MCP, "\ufeff# escape=`\n" + text(MCP))),
    ("S1 #! then syntax directive", 1, lambda: put(MCP, "#!/bin/sh\n# syntax=example.org/x:1\n" + text(MCP))),
    ("directive syntax at top", 1, lambda: put(MCP, "# syntax=docker/dockerfile:1\n" + text(MCP))),
    ("directive check after a comment", 1, lambda: put(MCP, "# hello\n# check=skip=all\n" + text(MCP))),
    ("tab after RUN", 1, before_workdir("RUN\tpip install requests\n")),
    ("non-ASCII in an instruction", 1, before_workdir("LABEL x=\"caf\u00e9\"\n")),
    ("unknown instruction", 1, before_workdir("FOO bar\n")),
    ("HEALTHCHECK CMD pip", 1, before_workdir("HEALTHCHECK CMD pip install requests\n")),
    ("heredoc", 1, before_workdir("RUN <<EOF\npip install requests\nEOF\n")),
    ("pip --timeout 60 install", 1, before_workdir("RUN pip --timeout 60 install requests\n")),
    ("python -m pip", 1, before_workdir("RUN python -m pip install requests\n")),
    ("blank line inside continuation", 1, before_workdir("RUN echo a \\\n\n  && pip install requests\n")),
    ("pip check || true", 1, canonical_edit("&& pip check\n", "&& pip check || true\n")),
    ("pip check; true", 1, canonical_edit("&& pip check\n", "&& pip check; true\n")),
    ("sh -c", 1, before_workdir("RUN sh -c \"pip install requests\"\n")),
    ("exec form", 1, before_workdir("RUN [\"pip\", \"install\", \"requests\"]\n")),
    ("$PIP", 1, before_workdir("RUN $PIP install requests\n")),
    ("second --only-binary", 1, canonical_edit(":all: -r", ":all: --only-binary :none: -r")),
    ("2>&1", 1, canonical_edit("&& pip check\n", "&& pip check 2>&1\n")),
    ("ADD", 1, before_workdir("ADD nostrology_mcp/README.md /tmp/x\n")),
    ("COPY --from", 1, before_workdir("COPY --from=python:3.12 /x /tmp/x\n")),
    ("SHELL", 1, before_workdir("SHELL [\"/bin/bash\", \"-c\"]\n")),
    ("ONBUILD", 1, before_workdir("ONBUILD RUN echo hi\n")),
    ("ENV PIP_EXTRA_INDEX_URL", 1, before_workdir("ENV PIP_EXTRA_INDEX_URL=https://example.org\n")),
    ("dir COPY over the lock path", 1, canonical_edit(CANON, "COPY nostrology_mcp/ /tmp/\n" + CANON)),
    ("unregistered Dockerfile with pip", 1, lambda: put("tools/Dockerfile", "FROM python:3.12\nRUN pip install x\n")),
    ("CMD with pip", 1, before_workdir("CMD [\"pip\", \"list\"]\n")),
    ("two installs", 1, canonical_edit(CANON, CANON + CANON)),
    ("uv pip install", 1, before_workdir("RUN uv pip install requests\n")),
    ("apt python3-pip", 1, before_workdir("RUN apt-get install -y python3-pip\n")),
    ("lock COPY into a directory", 1, lambda: (sub(MCP, "/tmp/mcp-requirements.txt\n", "/tmp/\n"),
                                              sub(MCP, "-r /tmp/mcp-requirements.txt", "-r /tmp/requirements.txt"))),
    ("N1 RUN between lock COPY and install", 1, canonical_edit(CANON, "RUN sed -i s/a/b/ /tmp/mcp-requirements.txt\n" + CANON)),
    ("N1 control: COPY between", 0, canonical_edit(CANON, "COPY nostrology_mcp/README.md /tmp/readme.md\n" + CANON)),
    ("N2 FROM example.org", 1, base_edit("FROM example.org/python:3.12-slim-bookworm")),
    ("N2 control: FROM digest pin", 0, base_edit("FROM python:3.12-slim-bookworm@sha256:" + "a" * 64)),
    ("N2 FROM AS stage", 1, base_edit("FROM python:3.12-slim-bookworm AS base")),
    ("N2 ARG before FROM", 1, base_edit("ARG X=1\nFROM python:3.12-slim-bookworm")),
    ("N3 COPY pip.conf into /etc", 1, lambda: (put("deploy/conf.ini", "[global]\n"), before_workdir("COPY deploy/conf.ini /etc/pip.conf\n")())),
    ("N3 COPY a .pth into site-packages", 1, before_workdir("COPY nostrology_mcp/README.md /usr/local/lib/python3.12/site-packages/x.pth\n")),
    ("N4 deploy/ unlistable + bad Dockerfile.mcp", 1, lambda: (before_workdir("RUN pip install requests\n")(), chmod("deploy", 0o111))),
    ("dagster Dockerfile: RUN between", 1, lambda: sub(DAG, "COPY deploy/dagster/wheels/*.whl /tmp/dagster-wheels/\n",
                                                        "COPY deploy/dagster/wheels/*.whl /tmp/dagster-wheels/\nRUN true\n")),
    ("S2 control: a compose that builds a registered Dockerfile", 0, extra()),
    ("S2 compose command with pip", 1, extra("    container_name: x\n",
                                             "    container_name: x\n    command: [\"sh\", \"-c\", \"pip install requests && python -m nostrology_mcp.server\"]\n")),
    ("S2 dockerfile: deploy/mcp.dockerfile", 1, lambda: (put("deploy/mcp.dockerfile", "FROM python:3.12-slim-bookworm\nRUN echo hi\n"),
                                                         extra("dockerfile: deploy/Dockerfile.mcp", "dockerfile: deploy/mcp.dockerfile")())),
    ("S2 BUILDKIT_SYNTAX arg", 1, extra("        GIT_SHA: ${T:-unknown}\n",
                                        "        GIT_SHA: ${T:-unknown}\n        BUILDKIT_SYNTAX: example.org/x:1\n")),
    ("S2 other build arg", 1, extra("        GIT_SHA: ${T:-unknown}", "        FOO: bar")),
    ("S2 dockerfile_inline", 1, extra("      context: ..\n",
                                      "      context: ..\n      dockerfile_inline: |\n        FROM python:3.12\n        RUN echo hi\n")),
    ("S2 additional_contexts", 1, extra("      dockerfile: deploy/Dockerfile.mcp\n",
                                        "      dockerfile: deploy/Dockerfile.mcp\n      additional_contexts:\n        x: ../..\n")),
    ("S2 target under build", 1, extra("      dockerfile: deploy/Dockerfile.mcp\n", "      dockerfile: deploy/Dockerfile.mcp\n      target: base\n")),
    ("S2 build short form", 1, extra("    build:\n      context: ..\n      dockerfile: deploy/Dockerfile.mcp\n      args:\n        GIT_SHA: ${T:-unknown}\n",
                                     "    build: ..\n")),
    ("S2 context elsewhere", 1, extra("context: ..", "context: ../nostrology_mcp")),
    ("S2 extends", 1, extra("    container_name: x\n", "    container_name: x\n    extends:\n      file: other.yml\n      service: y\n")),
    ("S2 image swapped to a foreign one", 1, extra("image: postgres:16", "image: example.org/python:3.12")),
    ("S2 NEL in a compose comment", 1, extra("# A compose file of", "# A compose file\x85of")),
    ("S2 control: env var added", 0, extra('      A: "1"\n', '      A: "1"\n      FOO_BAR: "1"\n')),
    ("S2 control: extra compose, pulled image", 0, lambda: put("deploy/compose.extra.yml", "services:\n  x:\n    image: postgres:16\n")),
    ("S2 extra compose building an unknown Dockerfile", 1, lambda: put("deploy/compose.extra.yml",
        "services:\n  x:\n    build:\n      context: ..\n      dockerfile: deploy/other.Dockerfile\n    image: nostrology-mcp:${T:-dev}\n")),
    ("S2 extra compose naming an image nothing here builds", 1, lambda: put("deploy/compose.extra.yml",
        "services:\n  x:\n    image: nostrology-pipeline:${T:-dev}\n")),
]

bad = 0
try:
    for name, want, mutate in V:
        try:
            mutate()
        except AssertionError as e:
            restore()
            bad += 1
            print("%-5s %-48s %s" % ("STALE", name, str(e)[:130]))
            continue
        try:
            r = subprocess.run([PYTHON, p("scripts/check_python_locks.py"), TREE], capture_output=True, text=True)
        finally:
            restore()
        rc = 1 if r.returncode else 0
        first = next((l for l in r.stdout.splitlines() if " FAIL " in l),
                     r.stdout.strip().splitlines()[-1] if r.stdout.strip() else r.stderr.strip()[-120:])
        ok = rc == want
        bad += not ok
        print("%-5s %-48s rc=%d want=%d | %s" % ("PASS" if ok else "FAIL", name, rc, want, first.replace("[lock-guard] ", "")[:130]))
    print("%d/%d variants as expected (guard run under %s)" % (len(V) - bad, len(V), PYTHON))
finally:
    shutil.rmtree(T)
sys.exit(1 if bad else 0)
