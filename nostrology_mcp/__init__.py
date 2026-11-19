"""nostrology_mcp - MCP server exposing Nostrology metrics to AI agents.

Scope (see README.md): THREE tools, csv|markdown
output. nostrology_query_metric serves the free quantitative metrics
(metric.py): the PUBLISHING and ZAPS daily series plus the LEADERBOARD metrics
(app_zaps, app_stack_count, publisher_app_count), the latter scopeable to one
publisher or one app via entity filters - plus the Routstr domain and the free
nutzaps domain (payment_activity, mint_leaderboard, mint_activity, and
mint_active_users by day, week or month; scopeable to one mint via mint_url).
nostrology_query_reference serves the free qualitative REFERENCE records
(reference.py): app_catalog,
publisher_profile, app_releases - entity data with no measures, behind the
nostrolo.gy app / publisher pages. nostrology_query_nutzaps
serves the network-wide kind-9321 (nutzaps) active users PAID per call in Cashu ecash
(cashu.py + the cashu_redeemer/ sidecar); it is registered unless the
kill-switch env disables it.
Two transports: stdio (local dev) and streamable HTTP (the dev-box container
behind Caddy at nostrolo.gy/mcp), plus the DB-free /health + /registry.json
routes and the nostrology://registry resource.
The package is named nostrology_mcp - NOT mcp/ - because a top-level directory named
`mcp` would shadow the `mcp` SDK package as soon as the repo root lands on sys.path
(pytest puts it there).
"""

__version__ = "0.5.1"
