"""scripts/gate_summary.py: the few lines the test gate's Discord post carries."""
import json
import os
import subprocess
import sys

import pytest

SCRIPT = os.path.join(os.path.dirname(__file__), "..", "..", "scripts", "gate_summary.py")


def run(*args, **env):
    if not os.path.exists(SCRIPT):
        pytest.skip("no scripts/ in this tree")
    full_env = {k: v for k, v in os.environ.items() if k not in ("STEPS", "GATE_STARTED", "GITHUB_OUTPUT")}
    full_env.update(env)
    return subprocess.run([sys.executable, SCRIPT, *args], capture_output=True, text=True, env=full_env)


def report(tmp_path, name="suite.xml", tests=10, skipped=2, failures=0, errors=0, cases=""):
    path = tmp_path / name
    path.write_text(
        '<?xml version="1.0" encoding="utf-8"?><testsuites name="pytest tests">'
        '<testsuite name="pytest" errors="%d" failures="%d" skipped="%d" tests="%d" time="1.0">%s</testsuite>'
        "</testsuites>" % (errors, failures, skipped, tests, cases))
    return str(path)


def failing_case(name, kind="failure", classname="loader.test_x"):
    return '<testcase classname="%s" name="%s"><%s message="boom"/></testcase>' % (classname, name, kind)


def test_a_green_run_gives_the_counts_with_thousands_separated(tmp_path):
    out = run("dagster=" + report(tmp_path, "a.xml", tests=742), "mcp=" + report(tmp_path, "b.xml", tests=1548, skipped=80))
    assert out.returncode == 0
    assert out.stdout.splitlines() == ["dagster: 740 passed, 2 skipped", "mcp: 1,468 passed, 80 skipped"]


def test_a_red_run_names_the_failed_tests_and_the_failed_steps(tmp_path):
    cases = failing_case("test_a") + failing_case("test_b", "error", "loader.test_y") + '<testcase classname="loader.test_x" name="test_ok"/>'
    steps = {"loader-tests": {"outcome": "failure"}, "skip-ceiling": {"outcome": "success"},
             "dagster-tests": {"outcome": "skipped"}, "mcp-tests": {"outcome": "cancelled"}}
    out = run("loader=" + report(tmp_path, tests=5, skipped=1, failures=1, errors=1, cases=cases), STEPS=json.dumps(steps))
    assert out.returncode == 0
    assert out.stdout.splitlines() == [
        "loader: 2 passed, 1 skipped, 2 failed",
        "Failed steps: loader-tests, mcp-tests",
        "Failed tests:",
        "loader.test_x::test_a",
        "loader.test_y::test_b",
    ]


def test_a_step_can_fail_with_every_test_passing(tmp_path):
    """The ceiling is a step, and the tests under it all passed."""
    out = run("mcp=" + report(tmp_path), STEPS=json.dumps({"skip-ceiling": {"outcome": "failure"}}))
    assert "Failed steps: skip-ceiling" in out.stdout
    assert "Failed tests" not in out.stdout


@pytest.mark.parametrize("content", [None, "", "not xml", "<other/>", '<testsuite name="x"/>'])
def test_a_report_it_cannot_read_is_said_and_never_fails_the_step(tmp_path, content):
    """The post matters most when something has already gone wrong."""
    path = tmp_path / "suite.xml"
    if content is not None:
        path.write_text(content)
    out = run("loader=" + str(path), "other=" + report(tmp_path, "b.xml"))
    assert out.returncode == 0, out.stderr
    assert out.stdout.startswith("loader: no usable report (")
    assert "other: 8 passed, 2 skipped" in out.stdout


@pytest.mark.parametrize("steps", [None, "", "not json", "[1, 2]", '{"a": 1}'])
def test_steps_it_cannot_read_are_no_reason_to_fail(tmp_path, steps):
    env = {} if steps is None else {"STEPS": steps}
    out = run("loader=" + report(tmp_path), **env)
    assert out.returncode == 0, out.stderr
    assert "Failed steps" not in out.stdout


def test_many_failures_are_capped_and_counted(tmp_path):
    cases = "".join(failing_case("test_%d" % i) for i in range(14))
    out = run("loader=" + report(tmp_path, tests=14, skipped=0, failures=14, cases=cases))
    lines = out.stdout.splitlines()
    assert "loader.test_x::test_9" in lines and "loader.test_x::test_10" not in lines
    assert lines[-1] == "and 4 more in the run's log"


def test_a_long_test_name_is_cut_and_the_whole_text_is_bounded(tmp_path):
    cases = "".join(failing_case("test_%d_%s" % (i, "x" * 400)) for i in range(40))
    labels = ["suite%d=%s" % (i, report(tmp_path, "s%d.xml" % i, tests=40, skipped=0, failures=40, cases=cases)) for i in range(3)]
    out = run(*labels, STEPS=json.dumps({"s" * 500: {"outcome": "failure"}}))
    assert out.returncode == 0
    assert len(out.stdout.rstrip("\n")) <= 1800
    assert all(len(line) <= 600 for line in out.stdout.splitlines() if not line.startswith("Failed steps"))


def test_it_says_how_long_the_job_took(tmp_path):
    import time
    out = run("loader=" + report(tmp_path), GATE_STARTED=str(int(time.time()) - 204))
    assert out.stdout.splitlines()[-1] == "Took 3.4 min."
    assert "Took" not in run("loader=" + report(tmp_path), GATE_STARTED="junk").stdout


def test_the_output_file_holds_text_a_test_name_cannot_break_out_of(tmp_path):
    """A test's name is not ours to trust, so one holding a newline and a fake delimiter stays inside."""
    output = tmp_path / "github_output"
    output.write_text("earlier=1\n")
    cases = failing_case("test_a&#10;text&lt;&lt;EOF&#10;injected=1")
    run("loader=" + report(tmp_path, tests=1, skipped=0, failures=1, cases=cases), GITHUB_OUTPUT=str(output))
    lines = output.read_text().splitlines()
    assert lines[0] == "earlier=1"
    assert lines[1].startswith("text<<gate_summary_")
    assert lines[-1] == lines[1].split("<<")[1]
    assert lines.count(lines[-1]) == 1


def test_no_arguments_is_a_usage_error(tmp_path):
    assert run().returncode == 2
