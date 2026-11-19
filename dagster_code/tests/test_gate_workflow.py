"""The shape of .github/workflows/gate.yml, the test gate every deploy that changes what runs waits for.

The gate was proven before any deploy waited for it. These hold it there, because a slip here fails
silently: a secret or an OIDC permission in the
job hands it to every locked package, a check that cannot fail lets anything deploy, and a summary the
deployment cannot read leaves a red gate unexplained. Which deploys wait for the gate is the
deployment's to say, and its own checks hold that.
"""
import base64
import json
import os
import re
import subprocess

import pytest

yaml = pytest.importorskip("yaml")

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
PATH = os.path.join(ROOT, ".github", "workflows", "gate.yml")


@pytest.fixture(scope="module")
def workflow():
    if not os.path.exists(PATH):
        pytest.skip("no .github/ in this tree")
    return yaml.safe_load(open(PATH))


@pytest.fixture(scope="module")
def jobs(workflow):
    return workflow["jobs"]


def step(jobs, step_id):
    return next(s for s in jobs["test"]["steps"] if s.get("id") == step_id)


def test_it_has_the_one_job_test(jobs):
    """So a new job is read here before it runs."""
    assert list(jobs) == ["test"]


def test_the_job_has_no_name_of_its_own(jobs):
    """The deployment refuses a gate whose one job is not named test, the name GitHub takes from the
    key when the job has none."""
    assert "name" not in jobs["test"]


def test_it_runs_only_when_dispatched_never_for_a_push_or_a_pull_request(workflow):
    """A deploy dispatches it on main and waits for that run: any other run is one nothing reads."""
    # PyYAML reads the bare key `on` as True.
    assert list(workflow[True]) == ["workflow_dispatch"]


def test_it_takes_the_verb_as_an_optional_string_and_shows_it_nowhere(workflow, jobs):
    inputs = workflow[True]["workflow_dispatch"]["inputs"]
    assert inputs == {"action": {"description": inputs["action"]["description"], "type": "string",
                                 "required": False, "default": ""}}
    # A constant title: the run list of a public repository names no deploy verb.
    assert workflow["run-name"] == "Test gate"
    assert "inputs." not in json.dumps(jobs)


def test_nothing_holds_a_permission_it_does_not_say_it_needs(workflow, jobs):
    assert workflow["permissions"] == {}
    assert jobs["test"]["permissions"] == {"contents": "read"}


def test_it_holds_no_secret_and_asks_for_no_token(workflow):
    """A package running here with sudo can read any secret the job references, from the runner's memory."""
    text = json.dumps(workflow)
    assert "secrets" not in text
    assert "id-token" not in text and "write-all" not in text


def test_the_job_holds_these_expressions_and_no_other(jobs):
    """So a new one is read here before it runs."""
    found = set(re.findall(r"\$\{\{[^}]*\}\}", json.dumps(jobs["test"])))
    assert found == {"${{ !cancelled() }}", "${{ always() }}", "${{ toJSON(steps) }}",
                     "${{ steps.keys.outputs.dagster }}", "${{ steps.keys.outputs.mcp }}",
                     "${{ steps.dagster-restore.outputs.cache-hit != 'true' }}",
                     "${{ steps.mcp-restore.outputs.cache-hit != 'true' }}"}


def test_no_expression_is_spliced_into_a_shell_script(jobs):
    """A test name or a branch name reaches this job: it has to arrive in an env var."""
    for s in jobs["test"]["steps"]:
        assert "${{" not in s.get("run", ""), s.get("name")


def test_every_action_is_pinned_by_commit(jobs):
    for s in jobs["test"]["steps"]:
        if "uses" in s:
            assert re.match(r"^[\w./-]+@[0-9a-f]{40}$", s["uses"]), s["uses"]


def test_the_job_runs_on_ubuntu_24_04_and_expects_nothing_a_previous_job_left(jobs):
    """Docker for the scratch Postgres, and CPython 3.12 on x86_64 for the locks' wheels. Each job
    starts on a fresh machine, so no step may count on a directory an earlier one left."""
    assert jobs["test"]["runs-on"] == "ubuntu-24.04"
    assert not re.search(r"(?<![\w.-])/cache\b", json.dumps(jobs["test"]))


def test_a_machine_without_docker_is_named_first(jobs):
    """Else it fails later, in a way that reads like a broken test."""
    assert [s.get("id") for s in jobs["test"]["steps"]][:2] == ["checkout", "docker"]
    run = step(jobs, "docker")["run"]
    assert "command -v docker" in run and "exit 1" in run


def test_only_the_summary_and_the_cache_may_fail_without_failing_the_gate(jobs):
    """A cache that cannot be read or written costs a build, never a red gate. Nothing else may."""
    lenient = [s for s in jobs["test"]["steps"] if "continue-on-error" in s]
    assert [s.get("id") for s in lenient] == ["dagster-restore", "dagster-save", "mcp-restore", "mcp-save", "summary"]
    for s in lenient[:-1]:
        assert s["uses"].startswith(("actions/cache/restore@", "actions/cache/save@")), s["id"]
    assert "continue-on-error" not in jobs["test"] and "if" not in jobs["test"]


def test_each_check_runs_whatever_failed_before_it(jobs):
    for step_id in ("cashu-redeemer", "scratch-postgres", "dagster-tests", "loader-tests", "mcp-tests",
                    "deploy-tooling", "skip-ceiling"):
        assert step(jobs, step_id)["if"] == "${{ !cancelled() }}", step_id


def test_the_ceilings_and_floors_are_the_ones_decided(jobs):
    """Raising one is a decision, so it costs a second line in the same change."""
    assert step(jobs, "skip-ceiling")["run"] == ('python3 scripts/check_skip_ceiling.py "$RUNNER_TEMP/dagster.xml:2:700" '
                                                 '"$RUNNER_TEMP/loader.xml:1:425" "$RUNNER_TEMP/mcp.xml:80:1325"')


def test_everything_that_can_hang_has_a_limit(jobs):
    """A hung step fails the job within minutes, not at GitHub's default of six hours."""
    assert jobs["test"]["timeout-minutes"] == 12
    limits = {"checkout": 3, "dagster-restore": 3, "dagster-environment": 8, "dagster-save": 3,
              "mcp-restore": 3, "mcp-environment": 4, "mcp-save": 3, "setup-node": 3,
              "cashu-redeemer-dependencies": 3, "cashu-redeemer": 5, "scratch-postgres": 5, "dagster-tests": 5,
              "loader-tests": 5, "mcp-tests": 5, "deploy-tooling": 3}
    assert {k: step(jobs, k)["timeout-minutes"] for k in limits} == limits


def test_the_checkout_leaves_no_token_for_the_tests_to_read(jobs):
    assert step(jobs, "checkout")["with"]["persist-credentials"] is False


def test_a_kept_environment_is_checked_before_it_is_trusted(jobs):
    """A kept environment that went bad rebuilds, rather than turn the gate red on every re-run."""
    for step_id, imports in (("dagster-environment", "import dagster, duckdb, pytest, yaml"),
                             ("mcp-environment", "import jsonschema, mcp, pytest")):
        run = step(jobs, step_id)["run"]
        kept = next(line for line in run.splitlines() if ".complete\" ]" in line and line.strip().startswith("if "))
        assert imports in kept, step_id


def test_the_steps_the_summary_names_all_have_ids(jobs):
    """toJSON(steps) holds only steps with an id, so one without could fail and go unnamed."""
    named = [s for s in jobs["test"]["steps"] if s.get("name") != "Clean up"]
    assert [s.get("name") or s.get("uses") for s in named if "id" not in s] == []


def test_each_suite_has_a_report_and_a_ceiling_and_the_contract_tests_their_database(jobs):
    """A suite that loses a dependency skips and exits 0, so a report missing from the ceiling goes blind."""
    ceiling = step(jobs, "skip-ceiling")["run"]
    summary = step(jobs, "summary")["run"]
    for step_id, report in (("dagster-tests", "dagster"), ("loader-tests", "loader"), ("mcp-tests", "mcp")):
        suite = step(jobs, step_id)
        assert suite["if"] == "${{ !cancelled() }}" and suite["timeout-minutes"] == 5
        assert '--junitxml="$RUNNER_TEMP/%s.xml"' % report in suite["run"]
        assert '"$RUNNER_TEMP/%s.xml:' % report in ceiling
        assert "$RUNNER_TEMP/%s.xml" % report in summary
    assert step(jobs, "loader-tests")["env"]["NOSTROLOGY_CONTRACT_DSN"].endswith("/loader")
    assert step(jobs, "mcp-tests")["env"]["NOSTROLOGY_CONTRACT_DSN"].endswith("/mcp")


def test_each_environment_is_restored_by_its_exact_key_and_saved_before_any_test(jobs):
    """A partial key match would hand over an environment built from other locks, and a save after the
    tests would be skipped by a red one, so the next run would build it again."""
    ids = [s.get("id") for s in jobs["test"]["steps"]]
    for env in ("dagster", "mcp"):
        key = "gate-%s-${{ steps.keys.outputs.%s }}" % (env, env)
        restore, save = step(jobs, env + "-restore"), step(jobs, env + "-save")
        assert restore["uses"].startswith("actions/cache/restore@") and save["uses"].startswith("actions/cache/save@")
        assert restore["with"] == {"path": "~/gate-py/" + env, "key": key}
        assert save["with"] == {"path": "~/gate-py/" + env, "key": key}
        assert save["if"] == "${{ steps.%s-restore.outputs.cache-hit != 'true' }}" % env
        assert 'venv="$HOME/gate-py/%s"' % env in step(jobs, env + "-environment")["run"]
        at = ids.index(env + "-environment")
        assert ids[at - 1] == env + "-restore" and ids[at + 1] == env + "-save"
        assert ids.index(env + "-save") < ids.index("cashu-redeemer")


def test_each_environments_key_names_files_that_exist_this_one_among_them(jobs):
    """`cat` of a missing file fails the step, and this file's own path is in each key."""
    run = step(jobs, "keys")["run"]
    for env in ("dagster", "mcp"):
        key = run[run.index(env + "=$("):]
        key = key[:key.index("sha256sum")]
        assert "python3 -V" in key and ".github/workflows/gate.yml" in key, env
        for path in re.findall(r"[\w./-]+\.(?:txt|yml)\b", key):
            assert os.path.exists(os.path.join(ROOT, path)), path
        assert 'echo "%s=$%s" >> "$GITHUB_OUTPUT"' % (env, env) in run


def test_npms_cache_is_keyed_on_the_redeemers_lock(jobs):
    """The lock `npm ci` installs from, so a changed dependency is never served from a stale cache."""
    assert step(jobs, "setup-node")["with"]["cache"] == "npm"
    assert step(jobs, "setup-node")["with"]["cache-dependency-path"] == "cashu_redeemer/package-lock.json"


def test_the_scratch_postgres_is_the_same_major_as_aurora_and_pinned(jobs):
    assert re.search(r"postgres:16-alpine@sha256:[0-9a-f]{64}", step(jobs, "scratch-postgres")["run"])


def test_the_scratch_postgres_sorts_text_as_aurora_does(jobs):
    """The image's default compares bytes, and plain ICU en-US sorts punctuation as bytes do, so a plain
    ORDER BY on text could pass here and sort differently in production, whose default collation is
    glibc's en_US.UTF-8. One setting, and the order asked of both suites' databases by behaviour."""
    run = step(jobs, "scratch-postgres")["run"]
    assert run.count("POSTGRES_INITDB_ARGS") == 1
    assert "-e POSTGRES_INITDB_ARGS='--locale-provider=icu --icu-locale=en-US-u-ka-shifted'" in run
    assert "for db in loader mcp; do" in run
    assert """psql -U postgres -d "$db" -Atc "SELECT 'a' < 'B' AND 'https://minta.io' < 'https://mint.coinos.io'\"""" in run
    assert 'if [ "$sorts" != t ]; then' in run
    assert run.index("CREATE DATABASE mcp") < run.index("for db in loader mcp; do")


# -- What it hands the deployment, the summary step run for real

def test_the_summary_runs_whatever_happened_and_cannot_turn_a_green_gate_red(jobs):
    summary = step(jobs, "summary")
    assert summary["if"] == "${{ always() }}" and summary["continue-on-error"] is True
    assert summary["env"]["STEPS"] == "${{ toJSON(steps) }}"
    assert "outputs" not in jobs["test"]


def summarize(tmp_path, jobs, reports):
    for name, text in reports.items():
        (tmp_path / name).write_text(text)
    done = subprocess.run(["bash", "-e", "-c", step(jobs, "summary")["run"]], cwd=ROOT, capture_output=True, text=True,
                          env={"PATH": os.environ["PATH"], "RUNNER_TEMP": str(tmp_path),
                               "STEPS": json.dumps({"loader-tests": {"outcome": "failure"}})})
    return done.stdout


def test_the_summary_ends_with_the_line_the_deployment_reads(tmp_path, jobs):
    """The deployment reads the last GATE_SUMMARY_B64= line of the log: the base64 of what the step
    printed above it."""
    report = ('<testsuites><testsuite name="a" tests="3" failures="1" errors="0" skipped="1">'
              '<testcase classname="a" name="x"><failure/></testcase></testsuite></testsuites>')
    out = summarize(tmp_path, jobs, {"dagster.xml": report, "loader.xml": report, "mcp.xml": report})
    lines = out.rstrip("\n").split("\n")
    m = re.fullmatch(r"GATE_SUMMARY_B64=([A-Za-z0-9+/]+=*)", lines[-1])
    assert m, out
    text = base64.b64decode(m.group(1)).decode()
    assert "dagster_code/tests: 1 passed, 1 skipped, 1 failed" in text
    assert "nostrology_mcp/tests: 1 passed, 1 skipped, 1 failed" in text
    assert "Failed steps: loader-tests" in text
    assert "\n".join(lines[:-1]) + "\n" == text


def test_the_summary_still_prints_the_line_with_no_reports(tmp_path, jobs):
    """A red gate matters most, and its reports may never have been written."""
    out = summarize(tmp_path, jobs, {})
    assert re.fullmatch(r"GATE_SUMMARY_B64=[A-Za-z0-9+/]+=*", out.rstrip("\n").split("\n")[-1]), out
