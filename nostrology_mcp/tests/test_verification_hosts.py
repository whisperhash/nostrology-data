"""The batteries hold no copy of the deployment's coordinates: the Aurora host is read on the box, from
the read-only login's ~/.pgpass, the ssh destination from NOSTROLOGY_RO_SSH, and the archive's AWS
profile from AWS_PROFILE when a reference runs."""

import ast
import os
import subprocess

import pytest

from nostrology_mcp.verification import nutzaps_truth, routstr_truth, truth_sql

# The ssh options that take a value (ssh(1)), so an ssh command's destination is its first argument
# that is neither an option nor an option's value.
_SSH_VALUE_OPTIONS = {"-B", "-b", "-c", "-D", "-E", "-e", "-F", "-I", "-i", "-J", "-L", "-l", "-m",
                      "-O", "-o", "-P", "-p", "-Q", "-R", "-S", "-W", "-w"}


def _destination(argv):
    """The destination of an ssh argv. An element that is not a string (an expression in the
    source, below) counts as an argument."""
    args = iter(argv[1:])
    for a in args:
        if a in _SSH_VALUE_OPTIONS:
            next(args, None)
        elif not (isinstance(a, str) and a.startswith("-")):
            return a
    return None


def _ssh_commands_in_source(module):
    """Every ssh command a module writes as a list literal, its string elements as strings and any
    other element as its expression node."""
    tree = ast.parse(open(module.__file__).read())
    return [[e.value if isinstance(e, ast.Constant) else e for e in node.elts]
            for node in ast.walk(tree)
            if isinstance(node, ast.List) and node.elts and isinstance(node.elts[0], ast.Constant)
            and node.elts[0].value == "ssh"]


def test_the_aurora_host_is_no_literal():
    assert "rds.amazonaws.com" not in truth_sql.PGHOST
    assert "~/.pgpass" in truth_sql.PGHOST


def test_the_host_expression_reads_the_users_entry_from_pgpass(tmp_path):
    # Two entries, the read-only role's second, so taking the first line would pick the wrong one.
    (tmp_path / ".pgpass").write_text(
        "other-host:5432:otherdb:other_role:x\n"
        f"example-host:5432:{truth_sql.PGDATABASE}:{truth_sql.PGUSER}:y\n")
    out = subprocess.run(
        ["bash", "-c", "printf %s " + truth_sql.PGHOST],
        env={"HOME": str(tmp_path), "PATH": os.environ["PATH"]},
        capture_output=True, text=True, check=True).stdout
    assert out == "example-host"


def test_a_missing_entry_names_itself(tmp_path):
    (tmp_path / ".pgpass").write_text("other-host:5432:otherdb:other_role:x\n")
    out = subprocess.run(
        ["bash", "-c", "printf %s " + truth_sql.PGHOST],
        env={"HOME": str(tmp_path), "PATH": os.environ["PATH"]},
        capture_output=True, text=True, check=True).stdout
    assert out == "no-pgpass-entry-for-" + truth_sql.PGUSER


def test_the_reference_needs_aws_profile(monkeypatch):
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    with pytest.raises(RuntimeError, match="AWS_PROFILE"):
        routstr_truth._aws_profile()
    monkeypatch.setenv("AWS_PROFILE", "a-reader")
    assert routstr_truth._aws_profile() == "a-reader"


def test_the_batteries_ssh_destination_comes_from_the_environment(monkeypatch, tmp_path):
    """The batteries carry no ssh destination of their own: every ssh command they build takes it
    from NOSTROLOGY_RO_SSH, read only when a script runs, and run_script stops before it writes
    anything when the variable is unset."""
    # In the source, no ssh command's destination is a string literal.
    for module in (truth_sql, nutzaps_truth):
        commands = _ssh_commands_in_source(module)
        assert commands, module.__name__  # the command this test guards is still there
        for argv in commands:
            dest = _destination(argv)
            assert dest is not None and not isinstance(dest, str), (module.__name__, dest)

    # At run time, the destination is the variable's value, and nothing else.
    monkeypatch.setenv("NOSTROLOGY_RO_SSH", "  a-reader-alias ")
    assert truth_sql.ssh_host() == "a-reader-alias"
    assert _destination(nutzaps_truth._ssh_argv()) == "a-reader-alias"
    ran = []
    monkeypatch.setattr(truth_sql.tempfile, "tempdir", str(tmp_path))  # run_script's script file
    def fake_run(argv, **kw):
        ran.append(argv)
        return subprocess.CompletedProcess(argv, 0, "", "")
    monkeypatch.setattr(truth_sql.subprocess, "run", fake_run)
    assert truth_sql.run_script("SELECT 1;") == {}
    assert [_destination(argv) for argv in ran] == ["a-reader-alias"]

    monkeypatch.delenv("NOSTROLOGY_RO_SSH", raising=False)
    with pytest.raises(RuntimeError, match="NOSTROLOGY_RO_SSH"):
        truth_sql.ssh_host()
    with pytest.raises(RuntimeError, match="NOSTROLOGY_RO_SSH"):
        truth_sql.run_script("SELECT 1;")
    with pytest.raises(RuntimeError, match="NOSTROLOGY_RO_SSH"):
        nutzaps_truth._ssh_argv()
    assert len(ran) == 1
