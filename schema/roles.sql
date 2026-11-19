-- nostrology-data — operational roles (Postgres 16).
--
-- Four read-only LOGIN roles, all SELECT-everything / mutate-nothing, deliberately
-- SEPARATE so each consumer has its own credential + connection cap (rotate/revoke
-- one without touching the other; distinct rolname in pg_stat_activity):
--   * nostrology_ro  - a read-only troubleshooting role; ad-hoc, low-concurrency.
--   * nostrology_hermes_ro - the SAME posture for a second troubleshooting reader. One
--                      role per reader, not one shared role: pg_stat_activity then
--                      names which reader ran a query, and a leaked password rotates
--                      one role, not both.
--   * nostrology_web — the SvelteKit web app's pooled reader (nostrology-web's
--                      container). A pg.Pool keeps several connections open, so its
--                      cap is sized for the pool, NOT the 3 a troubleshooting role gets.
--   * nostrology_mcp — the MCP server's reader (nostrology-mcp container; PUBLIC
--                      unauthenticated endpoint behind Caddy). Connect-per-call,
--                      no pool — so its cap bounds concurrent tool calls, and its
--                      statement_timeout is the containment line (see below).
--
-- Kept SEPARATE from init.sql: a deployment applies these roles (idempotently) on its
-- own, and binds each password as it does.
--
-- All four are created NOLOGIN / passwordless here - INERT until the deployment binds a
-- password it generates (never in git / Terraform / argv / logs).

-- Idempotent create (Postgres has no CREATE ROLE IF NOT EXISTS).
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'nostrology_ro') THEN
    CREATE ROLE nostrology_ro NOLOGIN;
  END IF;
END
$$;

-- Safe-by-construction on a LIVE serving database: a read-only default + a tight
-- statement timeout keep a stray troubleshooting query from holding locks or
-- disturbing the serving path; the connection cap keeps a runaway loop from
-- eating into the connection budget. (An older version of this comment said
-- "max_connections (20)"; that was the pre-Aurora container default. This cluster
-- reports max_connections=194 less 3 superuser-reserved = 191 usable, per the
-- nostrology_mcp block below.) All are per-role session defaults (a reader
-- can raise them in its own session if ever needed — it still has no write
-- privilege either way).
ALTER ROLE nostrology_ro SET default_transaction_read_only = on;
ALTER ROLE nostrology_ro SET statement_timeout = '30s';
ALTER ROLE nostrology_ro SET idle_in_transaction_session_timeout = '15s';
ALTER ROLE nostrology_ro CONNECTION LIMIT 3;

-- Present-object grants. ON ALL TABLES covers tables, views, AND materialized
-- views (all relations under the TABLES grant) — so every serving table (the
-- relays / lightning / zapstore / nutzaps slices) + the three relay_* MVs are
-- all readable.
GRANT CONNECT ON DATABASE nostrology TO nostrology_ro;
GRANT USAGE   ON SCHEMA   public      TO nostrology_ro;
GRANT SELECT  ON ALL TABLES IN SCHEMA public TO nostrology_ro;

-- Future-object grants: every table/MV the owner (nostrology) creates later —
-- the /lightning (9735), /zapstore, and /nutzaps (9321 / 10019) slices —
-- is auto-granted SELECT, so this role never needs re-granting as the schema
-- grows. Default privileges apply to objects created by the role running THIS
-- statement, which is exactly the owner (the deployment's role apply and init.sql both run
-- as nostrology).
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO nostrology_ro;


-- --- nostrology_hermes_ro - the SECOND troubleshooting role ------------------
-- A clone of nostrology_ro for a second reader, with its OWN password and its own
-- cap. Same reasoning as the web/mcp split one section down: separate credential,
-- separate rolname in pg_stat_activity and in Aurora's logs, separate revocation.
-- Deliberately NOT one shared role - when a slow or surprising query shows up, the
-- whole point is being able to say WHICH reader ran it.
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'nostrology_hermes_ro') THEN
    CREATE ROLE nostrology_hermes_ro NOLOGIN;
  END IF;
END
$$;

ALTER ROLE nostrology_hermes_ro SET default_transaction_read_only = on;
ALTER ROLE nostrology_hermes_ro SET statement_timeout = '30s';
ALTER ROLE nostrology_hermes_ro SET idle_in_transaction_session_timeout = '15s';
ALTER ROLE nostrology_hermes_ro CONNECTION LIMIT 3;

GRANT CONNECT ON DATABASE nostrology TO nostrology_hermes_ro;
GRANT USAGE   ON SCHEMA   public      TO nostrology_hermes_ro;
GRANT SELECT  ON ALL TABLES IN SCHEMA public TO nostrology_hermes_ro;

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO nostrology_hermes_ro;


-- ── nostrology_web — the web app's pooled read-only role ─────────────────────────
-- Same read-only posture as nostrology_ro, but its OWN credential + connection cap.
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'nostrology_web') THEN
    CREATE ROLE nostrology_web NOLOGIN;
  END IF;
END
$$;

-- read-only by default (the app only SELECTs; this is belt-and-suspenders against a
-- stray write ever slipping into a query). statement_timeout headroom over the
-- slowest page (/lightning ~12s cold). CONNECTION LIMIT 10 against the app's pg.Pool
-- max=5 (postgres.ts) - NOT the 3 a troubleshooting role gets.
--
-- CORRECTED 2026-09-23: this comment used to justify the 10 as "pg.Pool max 5, doubled:
-- the cap must admit a FULL overlap [during a container recreate] or the recreate it
-- exists to survive is the thing it refuses". THERE IS NO SUCH OVERLAP: the deployment
-- builds while the old container still serves, then recreates it stop-then-start, with no
-- start-first ordering. Pools are lazy besides - postgres.ts sets no `min`, so a new
-- container begins at zero connections. The rule was repeated into nostrology-web's
-- postgres.ts as an invariant forbidding any raise, which is how a sizing note became a
-- constraint nobody could question. 10 is simply conservative headroom over 5; it is not
-- derived, and raising either number is permissible for a measured reason.
--
-- Budget for the roles this file caps, re-derived 2026-11-12 (the block below is a
-- 30-line account of what happens when nobody re-derives this):
-- max_connections=194 less the 3 superuser-reserved slots => 191;
--   web 10 + troubleshooting 3+3 + mcp 32 + owner 20 = 68 (<=191).
-- Roles created anywhere else draw on the same 191: re-derive from pg_roles.rolconnlimit,
-- never from this file alone. Note the troubleshooting roles, plural: one role per reader,
-- so this grows by 3 for every reader added - and by whatever a new SERVICE role takes,
-- which is the part most easily missed.
-- The "owner 20" is nominal: see the re-measurement below, where it reads -1 in practice.
-- An earlier revision budgeted against
-- max_connections=20, which this cluster has never had — see the nostrology_mcp block
-- below for what that mis-derivation cost.
-- The PRIORITY path - the loader / refresh / schema apply - connects as the OWNER
-- `nostrology`. NOTE: on Aurora that role is NOT a superuser (`pg_roles.rolsuper` is
-- false; only `rdsadmin` is), so it gets NONE of the protection an earlier revision of
-- this comment claimed for it: superuser_reserved_connections keys on rolsuper, and
-- RDS's `rds.rds_superuser_reserved_connections` (which WOULD cover it, via its
-- rds_superuser membership) does not exist on Aurora - verified live 2026-08-03,
-- `unrecognized configuration parameter`. It competes for the same 191 slots as the
-- readers, which is why it is capped explicitly below rather than left unbounded.
ALTER ROLE nostrology_web SET default_transaction_read_only = on;
ALTER ROLE nostrology_web SET statement_timeout = '30s';
ALTER ROLE nostrology_web SET idle_in_transaction_session_timeout = '15s';
ALTER ROLE nostrology_web CONNECTION LIMIT 10;

GRANT CONNECT ON DATABASE nostrology TO nostrology_web;
GRANT USAGE   ON SCHEMA   public      TO nostrology_web;
GRANT SELECT  ON ALL TABLES IN SCHEMA public TO nostrology_web;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO nostrology_web;


-- ── nostrology_mcp — the MCP server's read-only role ─────────────────────────────
-- Same read-only posture, its OWN credential + cap. The MCP endpoint is PUBLIC and
-- unauthenticated (by design: nothing here is secret and the ACU dial caps cost),
-- which changes what the timeouts are FOR: on today's metrics (~16k-row scans)
-- statement_timeout never fires - it exists so a future metric over a real table
-- can never hold a connection open indefinitely, taking one of the role's capped
-- connections for as long as it runs. (It was first written for the scale-to-zero
-- cluster, which any open connection kept from pausing; the cluster has held a
-- 0.5 ACU floor since 2026-08-03.) Tighter than the readers' 30s on purpose: an
-- anonymous caller gets less rope than the operator's own app.
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'nostrology_mcp') THEN
    CREATE ROLE nostrology_mcp NOLOGIN;
  END IF;
END
$$;

-- CONNECTION LIMIT 32: connect-per-call (no pool), so this is literally "how many
-- tool calls may be in flight at once".
--
-- WAS 3, AND 3 WENT STALE - it was correct when written, and nobody revisited it.
-- The original comment budgeted against "max_connections=20", which was the REAL
-- setting of the containerized Postgres this DB used to run on (an explicit
-- `max_connections=20` in that compose `command:`). The Aurora migration replaced
-- that container with Serverless v2, which at the at-rest dial (Min 0 / Max 1.0
-- ACU) reports max_connections=194 and superuser_reserved_connections=3 - 191
-- available to non-superuser logins - and the role caps were never re-derived.
-- Every cap here was a fair share of a ceiling ~10x smaller than the live one. If you are
-- about to size another role, read 191, not 17 - and verify it, do not trust this
-- comment either:  SHOW max_connections;
--
-- What 3 cost, on 2026-08-03: a ContextVM client issued four concurrent tool
-- calls in one agent turn against a cold cluster. Three got connections, the
-- fourth got `FATAL: too many connections for role`, and the client's shared
-- AbortController then cancelled the three that were already succeeding. One
-- rejection, five calls lost, on a consumer's first real interaction with us.
--
-- Why 32: the measured per-turn fan-out is 4, and ContextVM's gateway runs in
-- per-client mode - each Nostr client pubkey opens its OWN MCP session here - so
-- concurrency is (simultaneous clients) x (parallel calls per turn). 8 x 4 = 32
-- covers eight concurrent consumers of a server now listed in a public directory.
-- Saturated it still leaves 159 slots, ~16x the entire non-MCP peak demand (web
-- pool 5, loader ~3 under Dagster max_concurrent_runs=1, troubleshooting 1-2) and 30x
-- the highest total this cluster has ever reached (5, over 30 days of CloudWatch
-- DatabaseConnections).
--
-- Why not higher: at Max 1.0 ACU the instance cannot usefully SERVE many more
-- than a couple of dozen concurrent queries. A larger cap would not buy
-- throughput, it would turn a burst into a slow-for-everyone queue instead of a
-- fast rejection carrying the retry hint the server already returns. 32 sits
-- above every legitimate pattern measured and below where admitting more work
-- stops helping.
--
-- NB `server.py _db_error_hint` deliberately does NOT name this number any more
-- (it said "3" and went stale here); it describes the remedy instead. What DOES
-- still name it in prose: nostrology_mcp/README.md and nostrology-web/deploy/Caddyfile's
-- @mcp comment - a DIFFERENT REPO, so it will not turn up in a grep of this one.
ALTER ROLE nostrology_mcp SET default_transaction_read_only = on;
ALTER ROLE nostrology_mcp SET statement_timeout = '10s';
ALTER ROLE nostrology_mcp SET idle_in_transaction_session_timeout = '15s';
ALTER ROLE nostrology_mcp CONNECTION LIMIT 32;

GRANT CONNECT ON DATABASE nostrology TO nostrology_mcp;
GRANT USAGE   ON SCHEMA   public      TO nostrology_mcp;
GRANT SELECT  ON ALL TABLES IN SCHEMA public TO nostrology_mcp;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO nostrology_mcp;


-- ── nostrology — the owner / loader / refresh role ───────────────────────────────
-- NOT created here (it is the cluster's master user, created by Terraform with the
-- Secrets-Manager password) and NOT read-only: this is the role the Dagster loads,
-- schema applies and refreshes write through. Only its connection cap is set here,
-- because that is a shared-pool concern and this file is where the pool is budgeted.
--
-- CONNECTION LIMIT 20, changed from unlimited. Two facts make the cap worth having:
--
--   * It is ENFORCED. On Aurora the master user is not a superuser (rolsuper=false,
--     verified live 2026-08-03), and CONNECTION LIMIT is only ignored for real
--     superusers. So unlike on a self-managed Postgres, this line actually binds.
--   * Nothing else bounded it. With every reader capped and the owner unlimited, the
--     owner was the ONLY role on this cluster that could exhaust the 191 shared slots
--     — and, per the note above, it has no reserved slots to fall back on when it does.
--
-- 20 against an observed demand of ~3: Dagster runs jobs one at a time
-- (`max_concurrent_runs: 1`) and the loader opens a schema connection plus per-step
-- connections sequentially; the highest TOTAL this cluster has reached in 30 days of
-- CloudWatch DatabaseConnections is 5, across all roles. The cap is a backstop against
-- a runaway or a stuck-connection leak, not a throttle on normal work - if a load ever
-- legitimately needs more, raise it; there are 126 free slots to raise it into.
--
-- BEST-EFFORT, AND IT MAY LEGITIMATELY NOT APPLY. This script runs AS `nostrology`,
-- and Postgres 16 requires CREATEROLE *plus ADMIN OPTION on the target role* to alter
-- it - which a role never holds on ITSELF. On RDS only `rdsadmin` has admin option on
-- the master, and `rdsadmin` is not reachable, so there is no in-band way to grant it.
-- A bare ALTER would therefore raise `permission denied to alter role`, and under
-- `psql -v ON_ERROR_STOP=1` that aborts the deployment's role apply at THIS line - skipping
-- every step after it, including binding the reader roles' passwords. On an
-- already-provisioned deployment that is a red deploy; on a FRESH one it is an outage,
-- because the web and MCP containers would have no credentials. A comment-motivated cap
-- must not be able to do that, hence the exception handler: it applies where permitted and
-- warns loudly where it is not.
--
-- MEASURED 2026-08-03: on THIS cluster the ALTER SUCCEEDS - after the role apply,
-- `nostrology` reports rolconnlimit=20 and no warning was raised. Aurora evidently
-- grants the master role admin option on itself, where stock Postgres 16 does not
-- (reproduced there: `ERROR: permission denied to alter role`). The handler stays
-- anyway: it costs one DO block, the behaviour is an undocumented RDS detail that a
-- version bump may revoke, and the failure it prevents is not "the cap is missing"
-- but "the role apply aborts before binding the reader passwords".
--
-- To check on any given cluster:
--   SELECT rolname, rolconnlimit FROM pg_roles WHERE rolname = 'nostrology';
-- A -1 does NOT prove the ALTER was refused. It applies on this cluster and is undone
-- later - see the re-measurement below, which distinguishes the two by looking for the
-- handler's warning in the deploy log. Either way the durable fix is the same: move the
-- loader off the master role onto a dedicated login the owner CREATES (it then holds
-- admin option on it, and the cap becomes enforceable) - which is better
-- least-privilege anyway.
--
-- [RE-MEASURED 2026-09-23: rolconnlimit=-1. THE CAP DOES NOT HOLD, and the
--  "transient sample" explanation written here on 2026-09-11 is now ruled out by its
--  own reasoning. That note argued a -1 reading was "a moment before re-application",
--  since the DO block re-applies on every role apply. But the role apply last ran
--  2026-09-09 16:04:58Z (the deploy log),
--  the 09-11 reading of 20 came two days AFTER that run, and the 09-23 reading of -1
--  came fourteen days after it with NO run in between. A value cannot decay from 20 to
--  -1 by being sampled before a re-application that never happened.
--
--  So the direction is the opposite of what was recorded: 20 is the transient, applied
--  by the role apply and then lost; -1 is the steady state between runs. Two further facts
--  narrow it. `rolsuper` is false (re-verified 2026-09-23), so the cap WOULD bind if
--  set - this is not the superuser exemption. And no "owner connection cap NOT applied"
--  warning appears anywhere in 45 days of deploy logs, so the ALTER is not failing into
--  the handler below; it applies, and something later undoes it. The likely agent is an
--  RDS-side operation on the master user (Secrets Manager password rotation, a
--  ModifyDBCluster, minor-version patching), all of which are known to reset master-user
--  attributes and none of which re-run the role apply afterwards.
--
--  CONSEQUENCE, and why this is worth the paragraphs: the owner is the one role that can
--  exhaust the shared slots, so while it is uncapped every other CONNECTION LIMIT in this
--  file is advisory. Do not read a green role apply as evidence the cap is in force -
--  re-run the check query below instead. The durable fix is the one already named above:
--  move the loader off the master role onto a dedicated login the owner CREATES, which
--  the owner holds admin option on and which no RDS operation resets.]
DO $$
BEGIN
  EXECUTE 'ALTER ROLE nostrology CONNECTION LIMIT 20';
EXCEPTION WHEN insufficient_privilege THEN
  RAISE WARNING 'owner connection cap NOT applied: no ADMIN OPTION on self (expected on Aurora). nostrology stays unbounded; see schema/roles.sql.';
END
$$;
