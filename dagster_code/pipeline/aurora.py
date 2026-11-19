"""Aurora endpoint/secret resolution + the load-time capacity dial.

The boto3 port of run.sh's resolve_endpoint / resolve_secret_arn / load_secret / _modify_scaling /
DIAL_UP / DIAL_DOWN. Every function takes its boto3 client(s) as arguments (dependency injection)
and imports no boto3/botocore — the InvalidDBClusterStateFault is duck-typed off the exception's
.response — so the retry + idempotency logic unit-tests with mock clients and no AWS.
"""

import json
import time
from typing import Callable

# 12 attempts => 11 sleeps before attempts 2-12 (5,10,15,20,25,30,30,30,30,30,30) = ~255s worst
# case. (run.sh's comment says "~285s" — an off-by-one we don't copy; the backoff itself matches.)
_DIAL_RETRY_MAX_ATTEMPTS = 12
_DIAL_IDEMPOTENT_ACU_TOLERANCE = 0.05  # well under Aurora's 0.5-ACU granularity


def _noop_log(_msg: str) -> None:
    pass


def _is_invalid_cluster_state(exc: Exception) -> bool:
    """True iff exc is a boto3 ClientError for InvalidDBClusterStateFault.

    RDS serializes cluster modifications, so a modify fired while a prior one is still applying
    raises this — the dial RACE a fast back-to-back up/down hits. Duck-typed on .response so this
    module needn't import botocore (keeps it boto3-free for local tests)."""
    resp = getattr(exc, "response", None)
    if not isinstance(resp, dict):
        return False
    return resp.get("Error", {}).get("Code") == "InvalidDBClusterStateFault"


def resolve_endpoint(rds, cluster: str) -> str:
    """The Aurora writer endpoint (resolved from the cluster id; changes on replacement)."""
    clusters = rds.describe_db_clusters(DBClusterIdentifier=cluster).get("DBClusters", [])
    endpoint = clusters[0].get("Endpoint") if clusters else None
    if not endpoint:
        raise RuntimeError(
            f"could not resolve the Aurora endpoint for {cluster} "
            "(is the cluster up + the instance role allowed rds:DescribeDBClusters?)"
        )
    return endpoint


def resolve_secret_arn(rds, cluster: str) -> str:
    """The cluster's managed master-user secret ARN (manage_master_user_password)."""
    clusters = rds.describe_db_clusters(DBClusterIdentifier=cluster).get("DBClusters", [])
    arn = clusters[0].get("MasterUserSecret", {}).get("SecretArn") if clusters else None
    if not arn:
        raise RuntimeError(f"could not resolve the master-secret ARN for {cluster}")
    return arn


def load_master_password(rds, secretsmanager, cluster: str) -> str:
    """Fetch the owner password fresh from Secrets Manager (rotation-safe; never cached/logged)."""
    arn = resolve_secret_arn(rds, cluster)
    secret_string = secretsmanager.get_secret_value(SecretId=arn).get("SecretString")
    if not secret_string:
        raise RuntimeError("could not read the Aurora master secret (instance role secretsmanager:GetSecretValue?)")
    password = json.loads(secret_string).get("password")
    if not password:
        raise RuntimeError("Aurora master secret has no .password field")
    return password


def current_scaling(rds, cluster: str) -> tuple[float, float]:
    """(MinCapacity, MaxCapacity) ACU of the cluster's ServerlessV2 scaling config."""
    clusters = rds.describe_db_clusters(DBClusterIdentifier=cluster).get("DBClusters", [])
    if not clusters:
        # Symmetry with resolve_endpoint/resolve_secret_arn: a missing cluster fails loud here
        # rather than reporting (0,0) and silently triggering a dial-up against nothing.
        raise RuntimeError(f"could not resolve scaling config for {cluster} (no such cluster?)")
    cfg = clusters[0].get("ServerlessV2ScalingConfiguration", {})
    return float(cfg.get("MinCapacity", 0)), float(cfg.get("MaxCapacity", 0))


def modify_scaling(
    rds,
    cluster: str,
    min_acu: float,
    max_acu: float,
    *,
    label: str,
    log: Callable[[str], None] = _noop_log,
    max_attempts: int = _DIAL_RETRY_MAX_ATTEMPTS,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Online modify-db-cluster scaling, retried through the InvalidDBClusterStateFault dial race.

    Retries ONLY that transient fault with capped backoff (5,10,…,30s) up to max_attempts; fails
    fast (raises) on any other error or once attempts are exhausted. `sleep` is injectable so tests
    don't actually wait. (A non-transient state — reboot/failover — surfaces the same fault and will
    burn all retries before failing loud; acceptable, it still ends red.)"""
    attempt = 1
    while True:
        try:
            rds.modify_db_cluster(
                DBClusterIdentifier=cluster,
                ServerlessV2ScalingConfiguration={"MinCapacity": min_acu, "MaxCapacity": max_acu},
            )
            suffix = f" (after {attempt} attempts; race cleared)" if attempt > 1 else ""
            log(f"[dial] {label}: OK — Min={min_acu}/Max={max_acu} ACU applied{suffix}")
            return
        except Exception as exc:
            if not _is_invalid_cluster_state(exc) or attempt >= max_attempts:
                raise RuntimeError(
                    f"[dial] {label}: modify-db-cluster failed (attempt {attempt}/{max_attempts}): {exc}"
                ) from exc
            secs = min(attempt * 5, 30)
            log(f"[dial] {label}: cluster busy (InvalidDBClusterStateFault); retry {attempt}/{max_attempts} in {secs}s")
            sleep(secs)
            attempt += 1


def dial_up(
    rds,
    cluster: str,
    load_min_acu: float,
    load_max_acu: float,
    *,
    log: Callable[[str], None] = _noop_log,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Raise the cluster to the load bracket. Idempotent: skips the modify if already there
    (so a re-entrant dial doesn't fire a needless modify that could trip the race)."""
    cur_min, cur_max = current_scaling(rds, cluster)
    # Idempotency: skip the modify if already at the load bracket. TOLERANCE compare (not exact
    # float ==) — the ACU values round-trip through the RDS API as floats. (run.sh compared
    # integer parts via `${x%.*}`, which would wrongly skip a fractional bracket like 0.5 vs 0;
    # the tolerance is well under Aurora's 0.5-ACU granularity, so this is an intentional tightening.)
    tol = _DIAL_IDEMPOTENT_ACU_TOLERANCE
    if abs(cur_min - load_min_acu) < tol and abs(cur_max - load_max_acu) < tol:
        log(f"[dial] already Min={load_min_acu}/Max={load_max_acu} ACU — skipping")
        return
    log(f"[dial] raising -> Min={load_min_acu}/Max={load_max_acu} ACU (warm load floor + burst ceiling)")
    modify_scaling(rds, cluster, load_min_acu, load_max_acu, label="dial-up", log=log, sleep=sleep)


def dial_down(
    rds,
    cluster: str,
    rest_min_acu: float,
    rest_max_acu: float,
    *,
    log: Callable[[str], None] = _noop_log,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Lower the cluster back to the REST bracket. Raises (fail-loud) if it ultimately can't -
    a cluster left STUCK at load capacity must fail the run, never hide behind a green.

    `rest_min_acu` is a PARAMETER, not the hardcoded 0 it used to be. That hardcode was a
    latent trap: it silently asserted "rest means scale-to-zero", so the instant Terraform
    set a non-zero `min_capacity` (dev did, 2026-08-03, to keep the ~15 s resume off a
    public endpoint), the first nightly load would have dialled the floor back to 0 in its
    `finally` and re-armed pausing for good - undoing a change to the cluster's infrastructure
    code nightly, while every `terraform plan` showed drift. Both ends of the bracket must come
    from config for the same reason `rest_max_acu` already did.
    """
    log(f"[dial] lowering -> Min={rest_min_acu}/Max={rest_max_acu} ACU (rest bracket)")
    try:
        modify_scaling(rds, cluster, rest_min_acu, rest_max_acu, label="dial-down", log=log, sleep=sleep)
    except Exception as exc:
        raise RuntimeError(
            "dial-down FAILED — the cluster may be STUCK at load capacity (the capacity alarm "
            f"backstops it). Fix manually: aws rds modify-db-cluster --db-cluster-identifier {cluster} "
            f"--serverless-v2-scaling-configuration MinCapacity={rest_min_acu},MaxCapacity={rest_max_acu}"
        ) from exc
