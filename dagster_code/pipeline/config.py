"""Static + env-overridable configuration for the nativized pipeline.

Mirrors the constants that sat at the top of the deployment's run.sh in the bash pipeline, with
the SAME env-var names so a deploy that exports NOSTROLOGY_* keeps working across the cutover.
Pure: no AWS imports and no I/O beyond os.environ, so it unit-tests without boto3 or a live box.
"""

import os
import re
from dataclasses import dataclass

# The deployment's own AWS coordinates: each field and the variable that supplies it. None has a
# default, so no real resource name lives in this tree, and a deployment that forgets one fails
# loudly instead of reading or writing a bucket someone else could own.
REQUIRED = {
    "aurora_cluster": "NOSTROLOGY_AURORA_CLUSTER",
    "athena_workgroup": "NOSTROLOGY_ATHENA_WORKGROUP",
    "extracts_bucket": "NOSTROLOGY_EXTRACTS_BUCKET",
    "glue_db": "NOSTROLOGY_GLUE_DB",
    "archive_bucket": "NOZZL_ARCHIVE_BUCKET",
}


class MissingConfig(RuntimeError):
    """A variable in REQUIRED is unset; the message names every one that is."""


@dataclass(frozen=True)
class PipelineConfig:
    # --- Aurora cluster + the load-time "dial" ACU bracket (run.sh DIAL_ACU/LOAD_MAX_ACU/MAX_ACU).
    aurora_cluster: str = ""  # NOSTROLOGY_AURORA_CLUSTER (required)
    aws_region: str = "us-east-1"
    dial_load_min_acu: float = 1.0  # load FLOOR  (run.sh DIAL_ACU)
    dial_load_max_acu: float = 1.0  # load CEILING (run.sh LOAD_MAX_ACU)
    dial_rest_min_acu: float = 0.5  # resting FLOOR - MUST equal the Terraform steady-state min_capacity
    dial_rest_max_acu: float = 1.0  # resting CEILING — MUST equal the Terraform steady-state max_capacity

    # --- Aurora connection coordinates (the writer endpoint is resolved at runtime from the
    #     cluster id; the owner password comes from the cluster's managed master secret).
    pg_port: int = 5432
    pg_owner: str = "nostrology"
    pg_database: str = "nostrology"

    # --- Athena extract + Glue catalog + the producer archive (extract phase).
    athena_workgroup: str = ""  # NOSTROLOGY_ATHENA_WORKGROUP (required)
    extracts_bucket: str = ""  # NOSTROLOGY_EXTRACTS_BUCKET (required)
    glue_db: str = ""  # NOSTROLOGY_GLUE_DB (required)
    glue_table: str = "events"
    archive_bucket: str = ""  # NOZZL_ARCHIVE_BUCKET (required)

    # --- nozzl admin (the WAL flush issued before an extract).
    flush_url: str = "http://127.0.0.1:19190"

    # --- Where the loader fetches the S3 UNLOAD parts to. MUST be a real-disk dir, NOT a /tmp
    #     that is a small tmpfs; a deployment points NOSTROLOGY_EXTRACT_TMP at its Dagster
    #     volume. Resolved in from_env (HOME-relative default).
    extract_tmp: str = ""

    @classmethod
    def from_env(cls, env: dict | None = None, *, required=tuple(REQUIRED)) -> "PipelineConfig":
        """Build from os.environ (or a supplied mapping, for tests), honoring run.sh's var names.

        Raises MissingConfig, naming them, when variables in REQUIRED are unset. The assets call this
        inside a run, never at import, so a missing variable fails the run that needs it and leaves
        the code location loaded, with the failure sensor that reports it. `required` narrows the
        check to the fields a caller uses."""
        e = os.environ if env is None else env
        cfg = cls(
            aurora_cluster=e.get("NOSTROLOGY_AURORA_CLUSTER", cls.aurora_cluster),
            aws_region=e.get("AWS_REGION", cls.aws_region),
            dial_load_min_acu=float(e.get("NOSTROLOGY_DIAL_ACU", cls.dial_load_min_acu)),
            dial_load_max_acu=float(e.get("NOSTROLOGY_LOAD_MAX_ACU", cls.dial_load_max_acu)),
            dial_rest_min_acu=float(e.get("NOSTROLOGY_MIN_ACU", cls.dial_rest_min_acu)),
            dial_rest_max_acu=float(e.get("NOSTROLOGY_MAX_ACU", cls.dial_rest_max_acu)),
            pg_port=int(e.get("NOSTROLOGY_PG_PORT", cls.pg_port)),
            athena_workgroup=e.get("NOSTROLOGY_ATHENA_WORKGROUP", cls.athena_workgroup),
            extracts_bucket=e.get("NOSTROLOGY_EXTRACTS_BUCKET", cls.extracts_bucket),
            glue_db=e.get("NOSTROLOGY_GLUE_DB", cls.glue_db),
            glue_table=e.get("NOSTROLOGY_GLUE_TABLE", cls.glue_table),
            archive_bucket=e.get("NOZZL_ARCHIVE_BUCKET", cls.archive_bucket),
            flush_url=e.get("NOZZL_ADMIN_URL", cls.flush_url),
            extract_tmp=(
                e.get("NOSTROLOGY_EXTRACT_TMP")
                or os.path.join(e.get("HOME", "/home/ec2-user"), ".cache", "nostrology-extract")
            ),
        )
        for field, var in (("glue_db", "NOSTROLOGY_GLUE_DB"), ("glue_table", "NOSTROLOGY_GLUE_TABLE")):
            value = getattr(cfg, field)
            if value and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
                raise ValueError(f"{var} must be an SQL identifier (the extract's SQL names it): {value!r}")
        missing = sorted(REQUIRED[f] for f in required if not getattr(cfg, f))
        if missing:
            raise MissingConfig(
                "set " + ", ".join(missing) + ": the deployment's own AWS coordinates have no default")
        return cfg
