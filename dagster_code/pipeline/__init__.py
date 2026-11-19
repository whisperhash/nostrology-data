"""Nativized nostrology load pipeline (boto3/psycopg2): the port of the deployment's run.sh data
path, called directly from the Dagster assets (no bash shell-out, no aws-cli).

Design for testability: the AWS-touching logic lives in single-responsibility modules whose
functions take their boto3 client(s) as ARGUMENTS (dependency injection). Only `aws.py` imports
boto3 (lazily), so the logic modules (`config`, `aurora`, …) are unit-testable with mock clients
on a machine without boto3 or AWS creds. See docs/design/dagster-orchestration-design.md
(Stage 3: native-ization).
"""
