"""Resources for the nostrology Dagster code location.

AuroraDial — the run-scoped Aurora capacity dial. Dagster initializes the resource ONCE per run
(these jobs run on the in_process_executor — see definitions.py — so a resource is shared across
all the run's steps, not re-initialized per step), so the cluster dials UP once at run start and
DOWN once at run end regardless of how many assets/partitions execute. Teardown failure RAISES ->
the run fails (a stuck cluster is paged, never hidden behind a green run).

It is boto3-NATIVE (no run.sh shell-out): the dial logic lives in pipeline/aurora.py and is
unit-tested with mock clients. (The whole data path is native now — extract/load/post-steps — so
the old NOSTROLOGY_DIAL_HELD env cover for run.sh's seam verbs is gone.)

It never logs a secret (it touches none) and holds no credential — the dial is pure
modify-db-cluster against the instance role.
"""

from contextlib import contextmanager

from dagster import ConfigurableResource

from .pipeline import aurora, aws
from .pipeline.config import PipelineConfig


class AuroraDial(ConfigurableResource):
    """Dial the deployment's Aurora cluster up for a load and back to rest after the run.

    The cluster is NOSTROLOGY_AURORA_CLUSTER, read when the run starts through PipelineConfig, the
    variable the extract and load steps read too, so the dial cannot move a cluster the data path
    does not use. `cluster` overrides it."""

    # Defaults match the live box config (load Min=Max=1 ACU, rest Min=0.5/Max=1). BOTH rest
    # values MUST equal the Terraform steady-state `min_capacity`/`max_capacity` in
    # the cluster's infrastructure code, or the dial and `terraform apply` fight each other:
    # whichever ran last wins, and the loser shows as drift on the next plan.
    #
    # rest_min_acu exists because it used to be a hardcoded 0 inside dial_down. When dev took
    # a 0.5 warm floor (2026-08-03) to keep Aurora's ~15 s resume off the public MCP/web path,
    # that hardcode would have reverted it in the `finally` of the very first nightly load and
    # every load after - a change to the cluster's infrastructure code silently undone. If the
    # floor ever goes back to scale-to-zero, this goes back to 0.0 in the same change.
    enabled: bool = True
    cluster: str = ""  # "" = NOSTROLOGY_AURORA_CLUSTER, read when the run starts
    region: str = ""  # "" = AWS_REGION, as the rest of the pipeline reads it
    load_min_acu: float = 1.0
    load_max_acu: float = 1.0
    rest_min_acu: float = 0.5
    rest_max_acu: float = 1.0

    @contextmanager
    def yield_for_execution(self, context):
        if not self.enabled:
            context.log.info("[dial] AuroraDial disabled (enabled=false) — not touching cluster capacity")
            yield self
            return

        cfg = PipelineConfig.from_env(required=() if self.cluster else ("aurora_cluster",))
        cluster, region = self.cluster or cfg.aurora_cluster, self.region or cfg.aws_region
        rds = aws.client("rds", region)
        aurora.dial_up(rds, cluster, self.load_min_acu, self.load_max_acu, log=context.log.info)
        try:
            yield self
        finally:
            # A stuck dial-down RAISES -> the run fails (dial_down wraps the failure with the
            # manual-fix hint). On a clean dial-down this is a no-op.
            aurora.dial_down(
                rds, cluster, self.rest_min_acu, self.rest_max_acu, log=context.log.info
            )
