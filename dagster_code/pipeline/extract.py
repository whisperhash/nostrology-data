"""The nativized per-kind extract — boto3 port of run.sh cmd_refresh_extract_only (run.sh:653-723).

Sequence (the registration ordering, structural here): flush the kind's WAL to S3 (fail-loud) ->
full per-kind Glue diff-register (the catalog must cover the just-flushed leaves before the catalog
-mode query reads it) -> render + run the Athena UNLOAD to the durable s3://EXTRACTS/k=K/RUN_ID/
prefix -> count the .gz parts (the extract_nonempty observability signal the load re-validates).

Every AWS client is built via aws.client (the lone lazy boto3 importer) and handed to the injected
helpers — so each piece (flush/sql/athena/glue_catalog) unit-tests with mocks; this orchestrator is
exercised live (a relays run). Returns {run_id, scanned_bytes, parts}.
"""

import os

from . import athena, aws, flush, glue_catalog, s3util, sql

# Matches run.sh's REFRESH_TIMEOUT (the Athena poll ceiling).
_QUERY_TIMEOUT_S = 1800


def run_extract(
    context,
    cfg,
    *,
    kind,
    run_id: str,
    into: str = "",
    repo_root: str,
    query_timeout_s: int = _QUERY_TIMEOUT_S,
) -> dict:
    """flush -> register -> Athena UNLOAD -> count parts. Raises (fail-loud) on a non-200 flush, a
    registration gate miss, or an Athena FAILED/CANCELLED/timeout. `into` selects the zapstore
    subset (validated in sql.extra_where)."""
    log = context.log.info

    # 1) Flush k=KIND's WAL to S3 (synchronous-to-S3; refuse to extract on anything but 200).
    log(f"[extract] flushing nozzl k={kind} (fail-loud, synchronous-to-S3)")
    flush.flush_kind(kind, cfg.flush_url, log=log)

    # 2) Full per-kind diff-register — the catalog must cover the just-flushed leaves BEFORE the
    #    catalog-mode UNLOAD reads it. register_partitions runs its own completeness
    #    + read-back gates internally; any miss raises and aborts the extract.
    log(f"[extract] registering k={kind} partitions (full diff-register)")
    glue_catalog.register_partitions(
        aws.client("s3", cfg.aws_region),
        aws.client("glue", cfg.aws_region),
        archive_bucket=cfg.archive_bucket,
        glue_db=cfg.glue_db,
        glue_table=cfg.glue_table,
        kind=kind,
        log=log,
    )

    # 3) Render + run the UNLOAD (the dedup runs serverless in Athena, off-box).
    query = sql.render(
        os.path.join(repo_root, "sql"),
        kind=kind, bucket=cfg.extracts_bucket, run_id=run_id,
        glue_db=cfg.glue_db, glue_table=cfg.glue_table, into=into,
    )
    log(f"[extract] starting Athena query (kind={kind} run={run_id} wg={cfg.athena_workgroup})")
    scanned = athena.run_unload(
        aws.client("athena", cfg.aws_region),
        workgroup=cfg.athena_workgroup, query=query, log=log, timeout_s=query_timeout_s,
    )

    # 4) Count the UNLOAD parts (observability; the load step re-fetches + re-validates).
    prefix = f"k={kind}/{run_id}/"
    parts = s3util.count_gz_parts(aws.client("s3", cfg.aws_region), cfg.extracts_bucket, prefix)
    log(f"[extract] done (kind={kind} run={run_id} scanned={scanned}B parts={parts})")
    return {"run_id": run_id, "scanned_bytes": scanned, "parts": parts}
