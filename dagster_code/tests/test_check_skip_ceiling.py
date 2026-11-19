"""scripts/check_skip_ceiling.py: the gate's suites may not quietly test less than they do today."""
import os
import subprocess
import sys

import pytest

SCRIPT = os.path.join(os.path.dirname(__file__), "..", "..", "scripts", "check_skip_ceiling.py")


def run(*args):
    if not os.path.exists(SCRIPT):
        pytest.skip("no scripts/ in this tree")
    return subprocess.run([sys.executable, SCRIPT, *args], capture_output=True, text=True)


def report(tmp_path, name="suite.xml", tests=10, skipped=2):
    path = tmp_path / name
    path.write_text(
        '<?xml version="1.0" encoding="utf-8"?><testsuites name="pytest tests">'
        '<testsuite name="pytest" errors="0" failures="0" skipped="%d" tests="%d" time="1.0"/>'
        "</testsuites>" % (skipped, tests))
    return str(path)


def test_at_the_ceiling_passes(tmp_path):
    result = run(report(tmp_path) + ":2")
    assert result.returncode == 0, result.stdout
    assert "10 tests, 2 skipped, ceiling 2" in result.stdout
    assert "come down" not in result.stdout


def test_one_above_the_ceiling_fails_and_names_the_report(tmp_path):
    path = report(tmp_path, skipped=3)
    result = run(path + ":2")
    assert result.returncode == 1
    assert path in result.stdout and "3 of 10 tests skipped" in result.stdout


def test_below_the_ceiling_passes_and_says_it_can_come_down(tmp_path):
    result = run(report(tmp_path, skipped=1) + ":2")
    assert result.returncode == 0, result.stdout
    assert "the ceiling can come down to 1" in result.stdout


def test_a_missing_report_counts_as_a_suite_not_run(tmp_path):
    assert run(str(tmp_path / "absent.xml") + ":2").returncode == 1


def test_an_unreadable_report_counts_as_a_suite_not_run(tmp_path):
    path = tmp_path / "broken.xml"
    path.write_text("<testsuites><testsuite")
    assert run(str(path) + ":2").returncode == 1


def test_a_report_with_no_suite_counts_as_a_suite_not_run(tmp_path):
    path = tmp_path / "empty.xml"
    path.write_text("<testsuites/>")
    assert run(str(path) + ":2").returncode == 1


def test_a_suite_without_the_counts_counts_as_a_suite_not_run(tmp_path):
    path = tmp_path / "odd.xml"
    path.write_text('<testsuites><testsuite name="pytest"/></testsuites>')
    assert run(str(path) + ":2").returncode == 1


def test_a_suite_that_ran_nothing_fails(tmp_path):
    assert run(report(tmp_path, tests=0, skipped=0) + ":2").returncode == 1


def test_one_bad_suite_among_good_ones_fails_and_all_are_reported(tmp_path):
    good = report(tmp_path, "good.xml")
    bad = report(tmp_path, "bad.xml", skipped=5)
    last = report(tmp_path, "last.xml")
    result = run(good + ":2", bad + ":2", last + ":2")
    assert result.returncode == 1
    assert good in result.stdout and bad in result.stdout and last in result.stdout


def test_below_the_floor_fails_and_says_how_many_ran(tmp_path):
    result = run(report(tmp_path, tests=10, skipped=2) + ":2:9")
    assert result.returncode == 1
    assert "only 8 of 10 tests ran, below the floor of 9" in result.stdout


def test_at_the_floor_passes(tmp_path):
    result = run(report(tmp_path, tests=10, skipped=2) + ":2:8")
    assert result.returncode == 0, result.stdout


def test_the_floor_is_about_tests_that_ran_so_a_skip_counts_against_it(tmp_path):
    assert run(report(tmp_path, tests=10, skipped=5) + ":5:6").returncode == 1


def test_a_suite_that_lost_tests_without_skipping_any_fails(tmp_path):
    """The hole the ceiling alone leaves: a collect_ignore loses a file, with nothing skipped and exit 0."""
    (tmp_path / "conftest.py").write_text('collect_ignore_glob = ["test_lost*"]\n')
    (tmp_path / "test_kept.py").write_text("def test_kept():\n    pass\n")
    (tmp_path / "test_lost.py").write_text("def test_a():\n    pass\ndef test_b():\n    pass\n")
    path = str(tmp_path / "real.xml")
    done = subprocess.run(
        [sys.executable, "-m", "pytest", str(tmp_path), "-q", "-p", "no:cacheprovider", "--junitxml", path],
        capture_output=True, text=True, cwd=tmp_path)
    assert done.returncode == 0, done.stdout
    assert run(path + ":0").returncode == 0
    assert run(path + ":0:3").returncode == 1


@pytest.mark.parametrize("spec", ["no-ceiling", "report.xml:many", "report.xml:-1", ":2",
                                  "report.xml:2:many", "report.xml:2:-1", ":2:3"])
def test_a_malformed_argument_fails(spec):
    assert run(spec).returncode == 1


def test_no_argument_is_a_usage_error():
    assert run().returncode == 2


def test_it_reads_what_pytest_itself_writes(tmp_path):
    """The contract with the tool, not a hand-written report: a pytest upgrade that renamed these
    attributes would otherwise turn every run of the gate red, or worse, green."""
    (tmp_path / "test_sample.py").write_text(
        "import pytest\n"
        "def test_runs():\n    pass\n"
        "@pytest.mark.skip(reason='on purpose')\ndef test_skips():\n    pass\n"
        "def test_also_skips():\n    pytest.skip('on purpose')\n")
    path = str(tmp_path / "real.xml")
    done = subprocess.run(
        [sys.executable, "-m", "pytest", str(tmp_path), "-q", "-p", "no:cacheprovider", "--junitxml", path],
        capture_output=True, text=True, cwd=tmp_path)
    assert done.returncode == 0, done.stdout
    assert run(path + ":2").returncode == 0
    assert "3 tests, 2 skipped" in run(path + ":2").stdout
    assert run(path + ":1").returncode == 1
