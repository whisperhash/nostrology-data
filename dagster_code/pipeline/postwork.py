"""The nativized surface POST-STEPS — boto3/psycopg2 ports of run.sh's cmd_rebuild_nutzap_active_users
(run.sh:1293-1309) and cmd_load_signers (run.sh:1216-1240). Neither touches S3/Athena; both run
inside the run's Aurora dial bracket (the calling assets require aurora_dial).

  rebuild_nutzap_mau  — TRUNCATE+INSERT active_user_counts_kind_9321_nutzaps from the loaded k=9321
                        table, via schema/nutzap_active_users.sql (pure SQL). Returns {rows}.
  classify_nutzap_mints - rebuild nutzap_mint_classes (which nutzap mints count, #9) from the
                        image-baked mint registry + the loaded kind-9321 (nutzaps) / kind-10019 (nutzap-configs)
                        mint URLs, by running
                        the loader's --classify-nutzap-mints mode as a Python subprocess. Returns {rows}.
  load_signers        — TRUNCATE+reinsert lightning_provider_signers from the image-baked YAML, by
                        running the loader's --load-lightning-signers mode as a Python subprocess
                        (no compose, no docker socket). Returns {rows}.

Endpoint/secret resolution + the password-free DSN + PGPASSWORD-in-env hygiene are shared with the
load path (pipeline/load.py); the password is never logged nor placed in argv.
"""

import os
import sys

from . import aurora, aws, pg, proc

_LOADER = "loader/load_jsonl_exports_into_postgres.py"
_NUTZAP_MAU_SQL = ("schema", "nutzap_active_users.sql")
_SIGNERS_YAML = ("data", "lightning", "provider_signers.yaml")
_MINT_REGISTRY_YAML = ("data", "nutzaps", "mint_registry.yaml")
_MINT_CLASSES_TABLE = "nutzap_mint_classes"
_MAU_TABLE = "active_user_counts_kind_9321_nutzaps"
_SIGNERS_TABLE = "lightning_provider_signers"


def _connect(cfg):
    """(conn, dsn, password) for the Aurora owner — endpoint/secret resolved fresh (rotation-safe)."""
    rds = aws.client("rds", cfg.aws_region)
    sm = aws.client("secretsmanager", cfg.aws_region)
    host = aurora.resolve_endpoint(rds, cfg.aurora_cluster)
    password = aurora.load_master_password(rds, sm, cfg.aurora_cluster)
    dsn = pg.build_dsn(host, cfg.pg_port, cfg.pg_owner, cfg.pg_database)
    return pg.connect(dsn, password), dsn, password


def _count_best_effort(cfg, table: str, context) -> int:
    """Row count as a BEST-EFFORT observability stat — a count failure must NOT fail a post-step
    whose real work (TRUNCATE+INSERT / signer load) already committed. Mirrors the bash
    `psql … 2>/dev/null || true; [[ rows =~ ^[0-9]+$ ]] || rows=0` contract (run.sh:1234,1302). On a
    fresh connection so a transient blip here can't poison the committing connection."""
    try:
        conn, _dsn, _pw = _connect(cfg)
        try:
            return pg.count_rows(conn, table)
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 — observability only; the work already committed
        context.log.warning(f"[postwork] best-effort count of {table} failed (reporting 0): {exc}")
        return 0


def rebuild_nutzap_mau(context, cfg, *, repo_root: str) -> dict:
    """Rebuild the /nutzaps Overview-MAU counts from the loaded k=9321 table (run.sh:1293-1309).

    Runs schema/nutzap_active_users.sql (idempotent TRUNCATE+INSERT, its own BEGIN/COMMIT) and
    returns {rows} - the row count of the rebuilt counts table. Reads events_kind_9321_nutzaps and
    nutzap_mint_classes, so it must run AFTER the 9321 load AND the mint classification (the
    asset's deps enforce that)."""
    conn, _dsn, _pw = _connect(cfg)
    try:
        context.log.info(f"[nutzap-mau] rebuilding {_MAU_TABLE} from events_kind_9321_nutzaps")
        pg.execute_sql_file(conn, os.path.join(repo_root, *_NUTZAP_MAU_SQL))
    finally:
        conn.close()
    rows = _count_best_effort(cfg, _MAU_TABLE, context)
    context.log.info(f"[nutzap-mau] done rows={rows}")
    return {"rows": rows}


def classify_nutzap_mints(context, cfg, *, repo_root: str, timeout: int | None = None) -> dict:
    """Rebuild nutzap_mint_classes (nostrology-data#9) from data/nutzaps/mint_registry.yaml and the
    mint URLs in the loaded kind-9321 (nutzaps) and kind-10019 (nutzap-configs) tables, via the
    loader's --classify-nutzap-mints mode.

    The loader validates the registry before writing and rebuilds the table in one transaction, so a
    malformed registry fails this step with the previous classification still serving. Its stdout
    (the class mix and the `Review:` line naming new unreviewed mints) lands in the step log."""
    _conn, dsn, password = _connect(cfg)
    _conn.close()
    yaml_path = os.path.join(repo_root, *_MINT_REGISTRY_YAML)
    if not os.access(yaml_path, os.R_OK):
        raise RuntimeError(f"mint registry not readable at {yaml_path}")
    argv = [sys.executable, _LOADER, "--classify-nutzap-mints", yaml_path, "--db-url", dsn]
    env = dict(os.environ)
    env["PGPASSWORD"] = password
    context.log.info(f"[mint-classes] classifying from {yaml_path} -> {_MINT_CLASSES_TABLE}")
    rc = proc.run_streamed(context, argv, env=env, cwd=repo_root, timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"mint classification exited {rc} - see the step log")
    rows = _count_best_effort(cfg, _MINT_CLASSES_TABLE, context)
    context.log.info(f"[mint-classes] done rows={rows}")
    return {"rows": rows}


def load_signers(context, cfg, *, repo_root: str, timeout: int | None = None) -> dict:
    """Load lightning_provider_signers from the image-baked YAML (run.sh:1216-1240).

    Applies the idempotent schema first (parity — the table must exist; also the resume gate), then
    runs the loader's --load-lightning-signers mode as a Python subprocess (TRUNCATE+reinsert; the
    YAML is the source of truth). Returns {rows}. The DSN is password-free; the secret rides in
    PGPASSWORD, never argv/log."""
    conn, dsn, password = _connect(cfg)
    try:
        pg.execute_sql_file(conn, os.path.join(repo_root, "schema", "init.sql"))
        context.log.info("[load-signers] schema applied (idempotent)")
    finally:
        conn.close()

    yaml_path = os.path.join(repo_root, *_SIGNERS_YAML)
    if not os.access(yaml_path, os.R_OK):
        raise RuntimeError(f"signer manifest not readable at {yaml_path}")

    argv = [sys.executable, _LOADER, "--load-lightning-signers", yaml_path, "--db-url", dsn]
    env = dict(os.environ)
    env["PGPASSWORD"] = password
    context.log.info(f"[load-signers] loading {yaml_path} -> {_SIGNERS_TABLE}")
    rc = proc.run_streamed(context, argv, env=env, cwd=repo_root, timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"signer loader exited {rc} — see the step log")

    rows = _count_best_effort(cfg, _SIGNERS_TABLE, context)
    context.log.info(f"[load-signers] done rows={rows}")
    return {"rows": rows}
