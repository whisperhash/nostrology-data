"""dagster_code — the Dagster code location for the nostrology-data loads.

The data pipeline is **native** in-process Python (boto3 + psycopg2) under `pipeline/` —
extract (flush → Glue register → Athena UNLOAD), load (fetch → loader subprocess), the MV
refresh, and the post-steps (the /nutzaps Overview-MAU rebuild, the /lightning signer load).
Dagster owns the graph, the run-scoped Aurora dial, scheduling, concurrency, and the per-step
run history. Each webapp surface is one job/run holding the dial for a single bracket:
/relays (k=10002), /nutzaps (k=9321 + k=10019 + MAU), /lightning (k=0 + k=9735 + signers),
/zapstore (k=32267/30063/30267 + the 9735→zapstore subset).

See docs/design/dagster-orchestration-design.md for the full design and the staged roadmap.
"""
