#!/usr/bin/env python3
"""Refuse to build a Python image whose dependencies are not fully hash-locked.

Run by a deployment before every build of an image, and on a workstation after changing a
lock. Stdlib only, and runnable on Python 3.9, the oldest /usr/bin/python3 it must support. After
changing this file, run scripts/check_python_locks_variants.py: the
bypasses the reviews found, and controls, as mutation variants on a scratch copy of the tree.

    /usr/bin/python3 scripts/check_python_locks.py [repo-root]

The Dockerfile rules fail closed instead of parsing shell. A Dockerfile is read the way BuildKit
reads it (split on LF only); characters the two could read differently, non-Dockerfile
instruction words and parser directives are refused. In the Python image Dockerfiles
(IMAGE_INSTALLS):
  - the first instruction is the only FROM: python:3.12-slim-bookworm, a digest pin allowed;
  - any instruction that names an installer (pip in any spelling, uv, poetry, conda,
    easy_install, setup.py and relatives) must be exactly

        RUN pip install --no-cache-dir --require-hashes --only-binary :all: \\
            [--find-links <absolute dir>] -r <absolute path> && pip check

    once, with the -r path provided by a single-file `COPY <that image's lock> <the path>`, only
    COPYs between the two, and no later multi-file COPY that could overwrite the lock;
  - COPY writes only under /app and /tmp; ADD, COPY --from, SHELL, ONBUILD, PIP_* or UV_*
    variables and heredocs are refused.
No other Dockerfile may name an installer at all: a new Python image is registered in
IMAGE_INSTALLS. Compose files decide what is built and run, so any in this tree is checked line by
line (check_compose): builds only of BUILDS Dockerfiles from their contexts, setting nothing but
context, dockerfile and the GIT_SHA arg; images only those built here or in PULLED_IMAGES; and no
installer, PIP_* or UV_* variable, BUILDKIT_*, dockerfile_inline, additional_contexts, extends or
include on any line.

It also fails when:
  - a lock lacks the scripts/python-lock.sh header, carries an option line (an index URL,
    -e, -r, ...), or has an entry that is not `name==version` with sha256 hashes;
  - a pin in an .in file is missing from its lock or has another version there;
  - a package in two locks installed together has two versions;
  - a committed wheel's sha256 is not among its lock entry's hashes.

What it cannot see: a lock edited by hand consistently (a real version with its real hashes)
looks exactly like generated output. Its installer checks are keyword tripwires, not a boundary:
a script or program that a RUN, CMD, HEALTHCHECK or compose `command:` executes can install
anything, and so can an obfuscated spelling (`\\x70ip` in a YAML string, `python -c` calling
os.system). `scripts/python-lock.sh check` and the reviewed diff are the controls for those.
"""
import hashlib
import json
import os
import posixpath
import re
import shlex
import subprocess
import sys

# Each lock and the .in file scripts/python-lock.sh generates it from.
LOCKS = {
    "nostrology_mcp/requirements.txt": "nostrology_mcp/requirements.in",
    "nostrology_mcp/requirements-dev.txt": "nostrology_mcp/requirements-dev.in",
    "dagster_code/requirements.txt": "dagster_code/requirements.in",
    "dagster_code/requirements-test.txt": "dagster_code/requirements-test.in",
    "requirements-pipeline.txt": "requirements-pipeline.in",
    "scripts/requirements-uv.txt": "scripts/requirements-uv.in",
    "scripts/requirements-wheel-build.txt": "scripts/requirements-wheel-build.in",
    "crosscheck/requirements.txt": "crosscheck/requirements.in",
}
# Each Python image Dockerfile and the lock it must install. The other locks are test tools and
# workstation tooling.
IMAGE_INSTALLS = {
    "deploy/Dockerfile.mcp": "nostrology_mcp/requirements.txt",
    "deploy/dagster/Dockerfile.dagster": "dagster_code/requirements.txt",
}
# Locks installed into one environment, so a package in both must have one version.
INSTALLED_TOGETHER = [("nostrology_mcp/requirements.txt", "nostrology_mcp/requirements-dev.txt"),
                      ("dagster_code/requirements.txt", "dagster_code/requirements-test.txt")]
# Committed wheels, and the lock whose entry must carry each wheel's sha256.
WHEEL_DIRS = {"deploy/dagster/wheels": "dagster_code/requirements.txt"}
SKIP_DIRS = {".git", "node_modules", "__pycache__", "blocklists"}

NAME = r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?"
VERSION = r"[A-Za-z0-9.+!_-]+"
LOCK_ENTRY = re.compile(r"^(" + NAME + r")==(" + VERSION + r")((?:\s+--hash=sha256:[0-9a-f]{64})+)$")
IN_PIN = re.compile(r"^(" + NAME + r")==(" + VERSION + r")$")
GENERATED_BY = re.compile(r"^#\s+scripts/python-lock\.sh compile [a-z-]+\s*$", re.M)

# Anything that installs Python packages, in any spelling: pip3, pip3.12, /usr/local/bin/pip,
# python -m pip, $PIP, "pip" in exec form, pip.conf, get-pip.py, python3-pip, and relatives.
INSTALLER = re.compile(
    r"(?<!\w)(?:pip[0-9.]*|pipx|pipenv|poetry|conda|mamba|micromamba|easy_install|uv|setup\.py)(?!\w)",
    re.I)
CANONICAL_INSTALL = re.compile(
    r"^pip install --no-cache-dir --require-hashes --only-binary :all:"
    r"(?: --find-links /[\w./-]+)?"
    r" -r (?P<path>/[\w./-]+) && pip check$")
CANONICAL_FORM = ("RUN pip install --no-cache-dir --require-hashes --only-binary :all: "
                  "[--find-links <absolute dir>] -r <absolute path> && pip check")
DIRECTIVE = re.compile(r"^#\s*([A-Za-z]+)\s*=")
CONTINUATION = re.compile(r"\\[ \t]*$")
COPY_FLAG = re.compile(r"^--(?:chown|chmod)=\S+$|^--link$")
PIP_OR_UV_VARIABLE = re.compile(r"(?:^|[\s=])(?:PIP|UV)_[A-Za-z0-9_]*")

# Dockerfile reading (review S1). BuildKit splits a Dockerfile on LF only; these characters would
# make a Python reading differ (splitlines() breaks lines on most of them), and a BOM is trimmed
# by BuildKit before its parser directives.
UNSAFE_CHARACTER = re.compile(r"[\x00-\x08\x0b-\x1f\x7f\x85\u2028\u2029\ufeff]")
KNOWN_DIRECTIVE = re.compile(r"^[ \t]*#[ \t]*(?:syntax|escape|check)[ \t]*=", re.I)
DOCKER_INSTRUCTIONS = {"ADD", "ARG", "CMD", "COPY", "ENTRYPOINT", "ENV", "EXPOSE", "FROM", "HEALTHCHECK",
                       "LABEL", "MAINTAINER", "ONBUILD", "RUN", "SHELL", "STOPSIGNAL", "USER", "VOLUME",
                       "WORKDIR"}
# The one base a Python image may start from (a digest pin is allowed), and where its COPYs may
# write: pip reads configuration from /etc and $HOME, and a .pth file in site-packages runs code.
BASE_IMAGE = re.compile(r"^python:3\.12-slim-bookworm(?:@sha256:[0-9a-f]{64})?$")
IMAGE_COPY_ROOTS = ("/app", "/tmp")

# Compose files (review S2) decide what is built and run. Each Dockerfile a compose file may
# build, with the build context it must use (repo-relative): the guard resolves a Python image's
# COPY sources against the repo root.
BUILDS = {
    "deploy/Dockerfile.mcp": ".",
    "deploy/dagster/Dockerfile.dagster": ".",
    "cashu_redeemer/Dockerfile": "cashu_redeemer",
}
BUILD_KEYS = {"context", "dockerfile", "args"}
BUILD_ARGS = {"GIT_SHA"}
# The images a compose file may name: those built above, under a tag variable, and pulled images
# that run no code of ours.
BUILT_IMAGE = re.compile(r"^(?:nostrology-mcp|nostrology-dagster|cashu-redeemer)"
                         r":\$\{[A-Za-z0-9_]+(?::-[A-Za-z0-9._-]+)?\}$")
PULLED_IMAGES = {"postgres:16"}
# Compose files checked by path, whether or not the walk finds them: none since the deployment's
# left this tree (2026-11-16). Any compose file the walk finds is checked all the same.
COMPOSE_FILES = set()
COMPOSE_FILE = re.compile(r"(?:^(?:docker-)?compose(?:[.-][\w.-]*)?|\.compose)\.ya?ml$")
COMPOSE_REFUSED = re.compile(r"dockerfile_inline|additional_contexts|BUILDKIT_|(?<![\w-])(?:extends|include)[ \t]*:")


def canon(name):
    """PEP 503 normalized project name."""
    return re.sub(r"[-_.]+", "-", name).lower()


def read(root, rel):
    with open(os.path.join(root, rel), encoding="utf-8") as f:
        return f.read()


def requirement_lines(text):
    """(line number, logical line) pairs: continuations joined, comments and blanks dropped."""
    out, parts, first = [], [], None
    for number, raw in enumerate(text.splitlines(), 1):
        line = re.sub(r"(?:^|\s)#.*$", "", raw).rstrip()
        continued = line.endswith("\\")
        if continued:
            line = line[:-1]
        if first is None and line.strip():
            first = number
        parts.append(line.strip())
        if not continued:
            joined = " ".join(p for p in parts if p)
            if joined:
                out.append((first, joined))
            parts, first = [], None
    joined = " ".join(p for p in parts if p)
    if joined:
        out.append((first, joined))
    return out


def dockerfile_lines(root, rel, problem, dockerfile=True):
    """The lines BuildKit (or compose's YAML reader) sees: the bytes split on LF only, one trailing
    CR dropped. A character that Python and those readers treat differently (UNSAFE_CHARACTER) is
    refused; in a Dockerfile, so are non-ASCII outside a comment line and a leading #! line
    (BuildKit skips it, then reads parser directives)."""
    with open(os.path.join(root, rel), "rb") as f:
        data = f.read()
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        problem(rel, 0, "not valid UTF-8 (%s)" % exc.reason)
        return []
    lines = []
    for number, line in enumerate(text.split("\n"), 1):
        if line.endswith("\r"):
            line = line[:-1]
        odd = UNSAFE_CHARACTER.search(line)
        if odd:
            problem(rel, number, "character U+%04X, which Docker and this guard would read differently" % ord(odd.group()))
        elif dockerfile and not line.isascii() and not line.lstrip(" \t").startswith("#"):
            problem(rel, number, "non-ASCII outside a comment line")
        lines.append(line)
    if dockerfile and lines and lines[0].startswith("#!"):
        problem(rel, 1, "a leading #! line: BuildKit skips it and then reads parser directives")
    return lines


def dockerfile_instructions(lines):
    """(line number, INSTRUCTION, arguments) triples, joined the way Docker joins them: a line
    ending in a backslash (optionally followed by blanks) continues the instruction, and
    comment lines and empty lines are dropped, inside a continued instruction too."""
    out, parts, first = [], [], None

    def emit():
        word, rest = (re.split(r"[ \t]+", " ".join(p for p in parts if p), 1) + [""])[:2]
        out.append((first, word.upper(), rest.strip(" \t")))

    for number, raw in enumerate(lines, 1):
        stripped = raw.strip(" \t")
        if not stripped or stripped.startswith("#"):
            continue
        if first is None:
            first = number
        match = CONTINUATION.search(raw)
        if match:
            parts.append(raw[:match.start()].strip(" \t"))
            continue
        parts.append(stripped)
        emit()
        parts, first = [], None
    if parts:
        emit()
    return out


def check_dockerfile(root, rel, expected_lock, problem):
    """Check one Dockerfile. expected_lock is the lock a Python image Dockerfile must install,
    or None for any other Dockerfile. Every Python image here is built from the repo root, so
    COPY sources are repo paths."""
    lines = dockerfile_lines(root, rel, problem)
    image = expected_lock is not None
    leading = True
    for number, line in enumerate(lines, 1):
        stripped = line.strip(" \t")
        if stripped and not stripped.startswith("#"):
            leading = False
        match = DIRECTIVE.match(stripped) if leading else KNOWN_DIRECTIVE.match(line)
        if match:
            problem(rel, number, "'%s' is, or looks like, a parser directive, which the guard does not support "
                                 "(reword a leading comment)" % stripped[:40])

    workdir = "/"
    history = []  # (step, instruction)
    files = []    # (step, image path, repo path) of single-file COPYs
    trees = []    # (step, image directory) that other COPYs wrote into
    installs = froms = 0
    for step, (number, word, arguments) in enumerate(dockerfile_instructions(lines), 1):
        history.append((step, word))
        if word not in DOCKER_INSTRUCTIONS:
            problem(rel, number, "'%s' is not a Dockerfile instruction" % word[:40])
            continue
        if "<<" in arguments:
            problem(rel, number, "%s uses a heredoc, which the guard cannot inspect" % word)
            continue
        if image:
            if word == "FROM":
                froms += 1
                if step != 1 or not BASE_IMAGE.match(arguments):
                    problem(rel, number, "a Python image starts with its one FROM python:3.12-slim-bookworm "
                                         "(optionally @sha256:<digest>), not FROM %s" % arguments[:80])
                continue
            if word in ("ADD", "SHELL", "ONBUILD"):
                problem(rel, number, "%s is not allowed in a Python image Dockerfile" % word)
                continue
            if word in ("ENV", "ARG") and PIP_OR_UV_VARIABLE.search(arguments):
                problem(rel, number, "%s sets a PIP_* or UV_* variable, which changes what pip or uv installs" % word)
                continue
            if word == "WORKDIR":
                if "$" in arguments:
                    problem(rel, number, "WORKDIR with a variable cannot be resolved by the guard")
                    continue
                workdir = posixpath.normpath(posixpath.join(workdir, arguments.strip("\"'")))
            elif word == "COPY":
                try:
                    parts = json.loads(arguments) if arguments.startswith("[") else shlex.split(arguments)
                except ValueError as exc:
                    problem(rel, number, "cannot parse this COPY (%s)" % exc)
                    continue
                flags = [p for p in parts if p.startswith("--")]
                paths = [p for p in parts if not p.startswith("--")]
                refused = [f for f in flags if not COPY_FLAG.match(f)]
                if refused:
                    problem(rel, number, "COPY %s is not allowed in a Python image Dockerfile" % " ".join(refused))
                    continue
                if len(paths) < 2 or any("$" in p for p in paths):
                    problem(rel, number, "the guard cannot resolve this COPY")
                    continue
                sources, destination = paths[:-1], paths[-1]
                target = posixpath.normpath(posixpath.join(workdir, destination))
                if not any(target == r or target.startswith(r + "/") for r in IMAGE_COPY_ROOTS):
                    problem(rel, number, "COPY into %s: a Python image copies only under %s"
                            % (target, " and ".join(IMAGE_COPY_ROOTS)))
                    continue
                single = (len(sources) == 1 and not destination.endswith("/")
                          and not re.search(r"[*?\[]", sources[0])
                          and os.path.isfile(os.path.join(root, sources[0])))
                if single:
                    files.append((step, target, posixpath.normpath(sources[0])))
                else:
                    trees.append((step, target))

        # Every instruction: HEALTHCHECK CMD and ENV run or configure things too.
        normalized = re.sub(r"\s+", " ", arguments).strip()
        if not INSTALLER.search(normalized):
            continue
        if not image:
            problem(rel, number, "%s names an installer, but this is not a registered Python image "
                                 "(IMAGE_INSTALLS in scripts/check_python_locks.py)" % word)
            continue
        match = CANONICAL_INSTALL.match(normalized) if word == "RUN" else None
        if not match:
            problem(rel, number, "%s names an installer; the only form allowed is: %s" % (word, CANONICAL_FORM))
            continue
        path = match.group("path")
        copy = next((c for c in reversed(files) if c[1] == path), None)
        if copy is None:
            problem(rel, number, "-r %s: no single-file COPY provides that path (write: COPY %s %s)"
                    % (path, expected_lock, path))
            continue
        if copy[2] != expected_lock:
            problem(rel, number, "-r %s is %s, but this image must install %s" % (path, copy[2], expected_lock))
            continue
        overwriting = [d for s, d in trees if s > copy[0] and (path == d or path.startswith(d.rstrip("/") + "/"))]
        if overwriting:
            problem(rel, number, "-r %s: a later multi-file COPY into %s could replace the lock" % (path, overwriting[-1]))
            continue
        between = sorted({w for s, w in history if copy[0] < s < step} - {"COPY"})
        if between:
            problem(rel, number, "-r %s: only COPY may come between the lock's COPY and the install, not %s"
                    % (path, ", ".join(between)))
            continue
        installs += 1
    if image and froms != 1:
        problem(rel, 0, "must contain exactly one FROM (found %d)" % froms)
    if image and installs != 1:
        problem(rel, 0, "must contain exactly one locked install of %s (found %d)" % (expected_lock, installs))


def compose_scalar(text):
    """A scalar value on a compose line, without a trailing comment or matching quotes."""
    value = re.sub(r"(?:^|[ \t])#.*$", "", text).strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        value = value[1:-1]
    return value


def check_compose(root, rel, problem):
    """Check one compose file line by line, failing closed instead of parsing YAML: a build block
    is checked where it is written, anchors and merge keys included. A build block sets only
    BUILD_KEYS and BUILD_ARGS and builds a BUILDS Dockerfile from that Dockerfile's context; an
    image is a BUILT_IMAGE or in PULLED_IMAGES; and no line, comments included, names an
    installer, a PIP_* or UV_* variable, BUILDKIT_*, dockerfile_inline, additional_contexts,
    extends or include."""
    lines = dockerfile_lines(root, rel, problem, dockerfile=False)
    base = posixpath.dirname(rel)
    for number, line in enumerate(lines, 1):
        for pattern, what in ((INSTALLER, "names an installer"),
                              (PIP_OR_UV_VARIABLE, "names a PIP_* or UV_* variable"),
                              (COMPOSE_REFUSED, "uses a key the guard does not allow")):
            found = pattern.search(line)
            if found:
                problem(rel, number, "%s: %s" % (what, found.group().strip()))
        image = re.match(r"^[ \t]*image[ \t]*:(.*)$", line)
        if image and not (BUILT_IMAGE.match(compose_scalar(image.group(1)))
                          or compose_scalar(image.group(1)) in PULLED_IMAGES):
            problem(rel, number, "image %s is neither built here (BUILT_IMAGE) nor in PULLED_IMAGES"
                    % compose_scalar(image.group(1))[:80])
        build = re.match(r"^( *)build[ \t]*:(.*)$", line)
        if not build:
            continue
        if compose_scalar(build.group(2)):
            problem(rel, number, "build must be a block of %s" % ", ".join(sorted(BUILD_KEYS)))
            continue
        keys, child, under_args = {}, None, False
        for offset, text in enumerate(lines[number:], number + 1):
            if not text.strip() or text.lstrip(" ").startswith("#"):
                continue
            depth = len(text) - len(text.lstrip(" "))
            if depth <= len(build.group(1)):
                break
            child = depth if child is None else child
            entry = re.match(r"^ *([A-Za-z_][\w.-]*)[ \t]*:(.*)$", text)
            if depth == child:
                under_args = bool(entry) and entry.group(1) == "args"
                if not entry or entry.group(1) not in BUILD_KEYS or (under_args and compose_scalar(entry.group(2))):
                    problem(rel, offset, "build may set only %s (args as a block)" % ", ".join(sorted(BUILD_KEYS)))
                else:
                    keys[entry.group(1)] = compose_scalar(entry.group(2))
            elif depth < child:
                problem(rel, offset, "inconsistent indentation in a build block")
            elif not under_args:
                problem(rel, offset, "a nested value under a build key other than args")
            else:
                arg = re.match(r"^ *(?:-[ \t]*)?([A-Za-z_][A-Za-z0-9_]*)[ \t]*[:=]", text)
                if not arg or arg.group(1) not in BUILD_ARGS:
                    problem(rel, offset, "build args may set only %s" % ", ".join(sorted(BUILD_ARGS)))
        context, dockerfile = keys.get("context", ""), keys.get("dockerfile", "Dockerfile")
        if not context or "$" in context + dockerfile:
            problem(rel, number, "a build needs a literal context (and a literal dockerfile, when set)")
            continue
        context = posixpath.normpath(posixpath.join(base, context))
        dockerfile = posixpath.normpath(posixpath.join(context, dockerfile))
        if BUILDS.get(dockerfile) != context:
            problem(rel, number, "builds %s from context %s, which BUILDS does not allow" % (dockerfile, context))


def build_files(root, problem):
    """(Dockerfiles, compose files) under root, repo-relative. A directory the walk cannot list
    is a problem, since the guard must not pass what it could not read, unless git ignores it:
    ignored runtime output holds nothing the deploy builds (registered paths are checked
    directly, and compose builds only registered Dockerfiles)."""
    dockerfiles, composes = [], []

    def unreadable(error):
        where = os.path.relpath(error.filename, root).replace(os.sep, "/") if error.filename else "."
        try:
            ignored = subprocess.run(["git", "-C", root, "check-ignore", "-q", "--", where],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
        except OSError:
            ignored = False
        if not ignored:
            problem(where, 0, "cannot list this directory (%s)" % error.strerror)

    for directory, subdirectories, files in os.walk(root, onerror=unreadable):
        subdirectories[:] = sorted(d for d in subdirectories if d not in SKIP_DIRS and not d.startswith(".venv"))
        for name in sorted(files):
            rel = os.path.relpath(os.path.join(directory, name), root).replace(os.sep, "/")
            if name == "Dockerfile" or name.startswith("Dockerfile.") or name.lower().endswith(".dockerfile"):
                dockerfiles.append(rel)
            elif COMPOSE_FILE.search(name):
                composes.append(rel)
    return dockerfiles, composes


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def main(argv):
    root = os.path.abspath(argv[1]) if len(argv) > 1 else os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    problems = []

    def problem(rel, number, message):
        problems.append("%s:%s: %s" % (rel, number, message))

    # 1. Locks, and the .in pins they must agree with.
    locks = {}
    for lock, source in sorted(LOCKS.items()):
        entries = {}
        locks[lock] = entries
        try:
            text = read(root, lock)
        except OSError as exc:
            problem(lock, 0, "unreadable (%s)" % exc.strerror)
            continue
        if not GENERATED_BY.search(text):
            problem(lock, 1, "no 'scripts/python-lock.sh compile' header: locks are generated, never written by hand")
        for number, line in requirement_lines(text):
            if line.startswith("-"):
                problem(lock, number, "option line in a lock: " + line.split()[0])
                continue
            match = LOCK_ENTRY.match(line)
            if not match:
                problem(lock, number, "not 'name==version --hash=sha256:...': " + line[:100])
                continue
            name = canon(match.group(1))
            if name in entries:
                problem(lock, number, "%s listed twice" % name)
            entries[name] = (match.group(2), set(re.findall(r"sha256:([0-9a-f]{64})", match.group(3))), number)
        if not entries:
            problem(lock, 1, "no entries")
        try:
            source_text = read(root, source)
        except OSError as exc:
            problem(source, 0, "unreadable (%s)" % exc.strerror)
            continue
        for number, line in requirement_lines(source_text):
            if re.match(r"^(?:-c|--constraint)\s+\S+$", line):
                continue
            match = IN_PIN.match(line)
            if not match:
                problem(source, number, "an .in requirement must be an exact 'name==version' pin: " + line[:100])
                continue
            name, version = canon(match.group(1)), match.group(2)
            if name not in entries:
                problem(source, number, "%s==%s is not in %s (run scripts/python-lock.sh compile)" % (name, version, lock))
            elif entries[name][0] != version:
                problem(source, number, "%s==%s, but %s has %s (run scripts/python-lock.sh compile)"
                        % (name, version, lock, entries[name][0]))

    for first, second in INSTALLED_TOGETHER:
        for name in sorted(set(locks[first]) & set(locks[second])):
            if locks[first][name][0] != locks[second][name][0]:
                problem(second, locks[second][name][2], "%s==%s, but %s has %s"
                        % (name, locks[second][name][0], first, locks[first][name][0]))

    # 2. Committed wheels.
    wheels = 0
    for directory, lock in sorted(WHEEL_DIRS.items()):
        path = os.path.join(root, directory)
        for name in sorted(os.listdir(path)) if os.path.isdir(path) else []:
            if not name.endswith(".whl"):
                continue
            project, version = name[:-4].split("-")[:2]
            entry = locks[lock].get(canon(project))
            digest = sha256(os.path.join(path, name))
            if entry is None:
                problem(directory + "/" + name, 0, "%s is not in %s" % (canon(project), lock))
            elif entry[0] != version:
                problem(directory + "/" + name, 0, "wheel version %s, but %s pins %s" % (version, lock, entry[0]))
            elif digest not in entry[1]:
                problem(directory + "/" + name, 0, "sha256 %s is not among the hashes %s lists for %s"
                        % (digest, lock, canon(project)))
            else:
                wheels += 1

    # 3. Dockerfiles and compose files: the registered ones by path, whether or not the walk
    # reaches them, and every other one the walk finds.
    found_dockerfiles, found_composes = build_files(root, problem)
    dockerfiles = sorted(set(IMAGE_INSTALLS) | set(BUILDS) | set(found_dockerfiles))
    composes = sorted(COMPOSE_FILES | set(found_composes))
    for rel in dockerfiles + composes:
        if not os.path.isfile(os.path.join(root, rel)):
            problem(rel, 0, "missing")
        elif rel in COMPOSE_FILES or rel in found_composes:
            check_compose(root, rel, problem)
        else:
            check_dockerfile(root, rel, IMAGE_INSTALLS.get(rel), problem)

    if problems:
        for line in problems:
            print("[lock-guard] FAIL " + line)
        print("[lock-guard] %d problem(s); refusing to build" % len(problems))
        return 1
    packages = sum(len(entries) for entries in locks.values())
    print("[lock-guard] OK: %d locks (%d pinned+hashed entries), %d Dockerfiles and %d compose files "
          "checked, %d image Dockerfiles each with one canonical locked install + pip check, %d wheel(s) "
          "hash-matched" % (len(locks), packages, len(dockerfiles), len(composes), len(IMAGE_INSTALLS), wheels))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
