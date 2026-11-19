"""The DEPLOYED defaults are a third copy of values that also live in
nostrology_mcp/cashu.py and in the sidecar's config.ts, and the deployed one
silently wins: `${CASHU_MINTS:-...}` in the deployment's compose file is what
the container actually gets, so editing the Python constant changes nothing on
the box.

That is the same class of bug the price fix caught (a literal `21` in the
registry beside an env-driven `cashu.price_sats()`), and the same class the
accept-vs-advertise split cannot survive: a deployed default that drifts from
DEFAULT_UNADVERTISED_MINTS republishes the test mint on the live service while
every other unit test here stays green.

The deployment's compose file left this tree on 2026-11-16, and the
deployment's own checks hold its defaults to the values below, as they hold the
hide list to the MCP server alone. So these tests pin the code's side to the
same values: a change here is a change to the deployment too, made in both
places together."""

import os

from nostrology_mcp import cashu

# Resolved from this file, not the cwd: a path that only works when pytest is
# invoked from the repo root is a test that silently stops reading the file -
# and a config pin that reads nothing passes.
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SIDECAR_CONFIG = os.path.join(_ROOT, "cashu_redeemer", "src", "config.ts")

# What the deployment gives the containers by default.
DEPLOYED_PRICE_SATS = 21
DEPLOYED_MINTS = "https://mint.minibits.cash/Bitcoin,https://testnut.cashu.space"
DEPLOYED_UNADVERTISED_MINTS = "https://testnut.cashu.space"


def test_the_code_price_is_the_deployed_default():
    assert cashu.DEFAULT_PRICE_SATS == DEPLOYED_PRICE_SATS


def test_the_code_accept_list_is_the_deployed_default():
    """The list the sidecar ENFORCES. A drift here means the challenge offers
    mints the sidecar refuses, or accepts mints nobody was told about."""
    assert cashu.DEFAULT_MINTS == DEPLOYED_MINTS


def test_the_code_hide_list_is_the_deployed_default():
    """The security-relevant one: if this drifts to empty or to a different
    spelling, the deployed service advertises the FakeWallet test mint."""
    assert cashu.DEFAULT_UNADVERTISED_MINTS == DEPLOYED_UNADVERTISED_MINTS


def test_the_sidecars_own_defaults_match_ours():
    """Cross-language pin. The sidecar is the enforcer; if its fallback price
    or accept list differs from ours, every honest payer is rejected the moment
    the env is not set (a local run, a docker run without the compose)."""
    with open(SIDECAR_CONFIG) as fh:
        body = fh.read()
    assert f"'{cashu.DEFAULT_MINTS}'" in body
    assert f"'CASHU_PRICE_SATS', {cashu.DEFAULT_PRICE_SATS}" in body
