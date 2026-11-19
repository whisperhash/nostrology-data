"""Unit tests for the nativized dial slice (pipeline/config.py + pipeline/aurora.py).

These import NO boto3 and NO dagster — the logic modules take their clients as arguments, so the
clients are plain MagicMocks here. Runs in the local gate (unlike the dagster-import tests).
"""

from unittest.mock import MagicMock

import pytest

from dagster_code.pipeline import aurora
from dagster_code.pipeline.config import REQUIRED, MissingConfig, PipelineConfig
from dagster_code.tests.fake_env import FAKE_ENV


class _Fault(Exception):
    """A stand-in for botocore's ClientError, duck-typed on .response the way aurora inspects it."""

    def __init__(self, code: str = "InvalidDBClusterStateFault"):
        super().__init__(code)
        self.response = {"Error": {"Code": code, "Message": code}}


# ── config ───────────────────────────────────────────────────────────────────────────────────
def test_config_defaults():
    cfg = PipelineConfig.from_env(FAKE_ENV)
    assert cfg.aws_region == "us-east-1"
    assert (cfg.dial_load_min_acu, cfg.dial_load_max_acu) == (1.0, 1.0)
    # The REST bracket is BOTH ends. dial_rest_min_acu defaults to the Terraform warm floor
    # (0.5), not 0 - a 0 here would re-arm scale-to-zero on the first nightly load.
    assert (cfg.dial_rest_min_acu, cfg.dial_rest_max_acu) == (0.5, 1.0)
    assert cfg.pg_port == 5432 and cfg.pg_owner == "nostrology"
    assert cfg.glue_table == "events"
    assert (cfg.aurora_cluster, cfg.extracts_bucket, cfg.glue_db) == ("example-cluster", "example_extracts", "example_db")


def test_config_the_deployments_coordinates_have_no_default():
    # Each comes from its variable. None has a default, so no real resource name lives in the tree,
    # and a missing one fails the run that builds the config, naming every missing variable.
    assert all(getattr(PipelineConfig, field) == "" for field in REQUIRED)
    with pytest.raises(MissingConfig) as exc:
        PipelineConfig.from_env({})
    for var in REQUIRED.values():
        assert var in str(exc.value)
    partial = {k: v for k, v in FAKE_ENV.items() if k != "NOZZL_ARCHIVE_BUCKET"}
    with pytest.raises(MissingConfig, match="^set NOZZL_ARCHIVE_BUCKET: "):
        PipelineConfig.from_env(partial)


def test_config_requires_exactly_these_names():
    # The deployment's Dagster compose sets these names, and its checker pins the same list: a name
    # added or renamed here and not there passes the gate and fails every run after the next deploy.
    assert set(REQUIRED.values()) == {"NOSTROLOGY_AURORA_CLUSTER", "NOSTROLOGY_ATHENA_WORKGROUP",
                                      "NOSTROLOGY_EXTRACTS_BUCKET", "NOSTROLOGY_GLUE_DB", "NOZZL_ARCHIVE_BUCKET"}


def test_config_glue_names_must_be_identifiers():
    # The extract's SQL names the Glue table directly, so a value that is no identifier fails here, before
    # any flush or register, rather than at Athena.
    with pytest.raises(ValueError, match="NOSTROLOGY_GLUE_DB"):
        PipelineConfig.from_env({**FAKE_ENV, "NOSTROLOGY_GLUE_DB": "db; drop"})
    with pytest.raises(ValueError, match="NOSTROLOGY_GLUE_TABLE"):
        PipelineConfig.from_env({**FAKE_ENV, "NOSTROLOGY_GLUE_TABLE": "a.b"})


def test_config_required_narrows_the_check_to_what_a_caller_uses():
    cfg = PipelineConfig.from_env({"NOZZL_ARCHIVE_BUCKET": "example_archive"}, required=("archive_bucket",))
    assert cfg.archive_bucket == "example_archive" and cfg.aurora_cluster == ""
    with pytest.raises(MissingConfig, match="NOZZL_ARCHIVE_BUCKET"):
        PipelineConfig.from_env({}, required=("archive_bucket",))


def test_config_env_overrides_use_runsh_var_names():
    cfg = PipelineConfig.from_env(
        {
            **FAKE_ENV,
            "NOSTROLOGY_AURORA_CLUSTER": "prod-nostrology",
            "AWS_REGION": "eu-west-1",
            "NOSTROLOGY_DIAL_ACU": "2",
            "NOSTROLOGY_LOAD_MAX_ACU": "4",
            "NOSTROLOGY_MIN_ACU": "0",
            "NOSTROLOGY_MAX_ACU": "1",
            "NOSTROLOGY_PG_PORT": "6543",
            "NOZZL_ADMIN_URL": "http://nostr-nozzl:9091",
        }
    )
    assert cfg.aurora_cluster == "prod-nostrology" and cfg.aws_region == "eu-west-1"
    assert (cfg.dial_load_min_acu, cfg.dial_load_max_acu) == (2.0, 4.0)
    assert (cfg.dial_rest_min_acu, cfg.dial_rest_max_acu) == (0.0, 1.0)
    assert cfg.pg_port == 6543 and cfg.flush_url == "http://nostr-nozzl:9091"


# ── endpoint / secret resolution ───────────────────────────────────────────────────────────────
def test_resolve_endpoint():
    rds = MagicMock()
    rds.describe_db_clusters.return_value = {"DBClusters": [{"Endpoint": "host.rds.amazonaws.com"}]}
    assert aurora.resolve_endpoint(rds, "example-cluster") == "host.rds.amazonaws.com"


def test_resolve_endpoint_missing_raises():
    rds = MagicMock()
    rds.describe_db_clusters.return_value = {"DBClusters": []}
    with pytest.raises(RuntimeError, match="could not resolve the Aurora endpoint"):
        aurora.resolve_endpoint(rds, "example-cluster")


def test_load_master_password_parses_password_field():
    rds = MagicMock()
    rds.describe_db_clusters.return_value = {"DBClusters": [{"MasterUserSecret": {"SecretArn": "arn:secret"}}]}
    sm = MagicMock()
    sm.get_secret_value.return_value = {"SecretString": '{"username":"nostrology","password":"hunter2"}'}
    assert aurora.load_master_password(rds, sm, "example-cluster") == "hunter2"
    sm.get_secret_value.assert_called_once_with(SecretId="arn:secret")


def test_load_master_password_no_password_raises():
    rds = MagicMock()
    rds.describe_db_clusters.return_value = {"DBClusters": [{"MasterUserSecret": {"SecretArn": "arn:secret"}}]}
    sm = MagicMock()
    sm.get_secret_value.return_value = {"SecretString": '{"username":"nostrology"}'}
    with pytest.raises(RuntimeError, match="no .password field"):
        aurora.load_master_password(rds, sm, "example-cluster")


def test_current_scaling():
    rds = MagicMock()
    rds.describe_db_clusters.return_value = {
        "DBClusters": [{"ServerlessV2ScalingConfiguration": {"MinCapacity": 0.0, "MaxCapacity": 1.0}}]
    }
    assert aurora.current_scaling(rds, "example-cluster") == (0.0, 1.0)


# ── _modify_scaling retry semantics ──────────────────────────────────────────────────────────
def test_modify_scaling_success_first_try_no_sleep():
    rds = MagicMock()
    slept: list = []
    aurora.modify_scaling(rds, "c", 1, 1, label="dial-up", sleep=slept.append)
    rds.modify_db_cluster.assert_called_once_with(
        DBClusterIdentifier="c", ServerlessV2ScalingConfiguration={"MinCapacity": 1, "MaxCapacity": 1}
    )
    assert slept == []


def test_modify_scaling_retries_the_race_then_succeeds():
    rds = MagicMock()
    rds.modify_db_cluster.side_effect = [_Fault(), _Fault(), None]  # busy, busy, ok
    slept: list = []
    aurora.modify_scaling(rds, "c", 0, 1, label="dial-down", sleep=slept.append)
    assert rds.modify_db_cluster.call_count == 3
    assert slept == [5, 10]  # capped backoff: 5, 10, …


def test_modify_scaling_fails_fast_on_other_error():
    rds = MagicMock()
    rds.modify_db_cluster.side_effect = _Fault(code="DBClusterNotFoundFault")
    slept: list = []
    with pytest.raises(RuntimeError, match="modify-db-cluster failed"):
        aurora.modify_scaling(rds, "c", 1, 1, label="dial-up", sleep=slept.append)
    assert rds.modify_db_cluster.call_count == 1  # not retried
    assert slept == []


def test_modify_scaling_backoff_caps_at_30s():
    rds = MagicMock()
    rds.modify_db_cluster.side_effect = _Fault()  # always busy
    slept: list = []
    with pytest.raises(RuntimeError):
        aurora.modify_scaling(rds, "c", 1, 1, label="dial-up", max_attempts=8, sleep=slept.append)
    # sleeps before attempts 2-8 = 7 sleeps; backoff 5,10,15,20,25, then CAPPED at 30
    assert slept == [5, 10, 15, 20, 25, 30, 30]


def test_modify_scaling_exhausts_attempts():
    rds = MagicMock()
    rds.modify_db_cluster.side_effect = _Fault()  # always busy
    slept: list = []
    with pytest.raises(RuntimeError, match="modify-db-cluster failed"):
        aurora.modify_scaling(rds, "c", 1, 1, label="dial-up", max_attempts=3, sleep=slept.append)
    assert rds.modify_db_cluster.call_count == 3
    assert slept == [5, 10]  # slept before attempts 2 and 3, not after the final failure


# ── dial_up idempotency / dial_down fail-loud ────────────────────────────────────────────────
def test_dial_up_idempotent_skips_modify_when_already_at_load():
    rds = MagicMock()
    rds.describe_db_clusters.return_value = {
        "DBClusters": [{"ServerlessV2ScalingConfiguration": {"MinCapacity": 1.0, "MaxCapacity": 1.0}}]
    }
    aurora.dial_up(rds, "c", 1.0, 1.0, sleep=lambda _s: None)
    rds.modify_db_cluster.assert_not_called()


def test_dial_up_modifies_when_at_rest():
    rds = MagicMock()
    rds.describe_db_clusters.return_value = {
        "DBClusters": [{"ServerlessV2ScalingConfiguration": {"MinCapacity": 0.0, "MaxCapacity": 1.0}}]
    }
    aurora.dial_up(rds, "c", 1.0, 1.0, sleep=lambda _s: None)
    rds.modify_db_cluster.assert_called_once()


def test_dial_down_raises_helpful_message_on_failure():
    rds = MagicMock()
    rds.modify_db_cluster.side_effect = _Fault()  # never clears (no-op sleep -> 12 instant attempts)
    with pytest.raises(RuntimeError, match="STUCK at load capacity"):
        aurora.dial_down(rds, "c", 0.5, 1.0, sleep=lambda _s: None)


def test_dial_down_restores_the_configured_rest_floor_not_zero():
    """THE regression test for a latent trap: dial_down used to hardcode MinCapacity=0.
    With a non-zero Terraform `min_capacity` (dev runs 0.5 to keep Aurora's ~15 s resume
    off the public MCP/web path), that hardcode meant the first nightly load silently
    re-armed scale-to-zero in its `finally` - undoing a change to the cluster's infrastructure
    code every night, showing only as `terraform plan` drift."""
    rds = MagicMock()
    aurora.dial_down(rds, "c", 0.5, 1.0, sleep=lambda _s: None)
    rds.modify_db_cluster.assert_called_once_with(
        DBClusterIdentifier="c",
        ServerlessV2ScalingConfiguration={"MinCapacity": 0.5, "MaxCapacity": 1.0},
    )


def test_dial_down_still_supports_scale_to_zero():
    """The floor is config, not policy: passing 0 must still fully re-arm pausing, so
    reverting the warm floor stays a one-value change on both sides."""
    rds = MagicMock()
    aurora.dial_down(rds, "c", 0.0, 1.0, sleep=lambda _s: None)
    rds.modify_db_cluster.assert_called_once_with(
        DBClusterIdentifier="c",
        ServerlessV2ScalingConfiguration={"MinCapacity": 0.0, "MaxCapacity": 1.0},
    )
