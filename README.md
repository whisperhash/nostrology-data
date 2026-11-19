# nostrology-data

nostrology-data turns the archive of Nostr events that nozzl keeps into the metrics behind [nostrolo.gy](https://nostrolo.gy). Every day it extracts the kinds each domain needs from nozzl's archive in S3, through Athena, and loads them, or the metrics computed from them, into PostgreSQL, where nostrology-web reads them.

It also serves those metrics over MCP (the Model Context Protocol), for AI agents. A public instance answers at `https://nostrolo.gy/mcp`, read-only and without authentication, and [nostrolo.gy/data](https://nostrolo.gy/data) has the line that registers it in each MCP client.

The design is in [docs/design/](docs/design/), starting with [dagster-orchestration-design.md](docs/design/dagster-orchestration-design.md).

```
nozzl ---> Parquet archive in S3
              |  per kind: the Glue catalog, then an Athena UNLOAD
              v
           gzipped JSON lines in S3
              |  the loader, or a domain's own computation in Python
              v
           PostgreSQL ---> nostrology-web
                      ---> the MCP server
```

## What it does

- **One Dagster job per domain**, each run daily shortly after midnight UTC, since every domain counts whole UTC days. The jobs queue and run one at a time.
- **Extracts through Athena.** For each kind, a job first has nozzl publish its buffered events, and goes no further unless nozzl confirms they were written. It then registers the archive's new partitions in the Glue catalog, checks that none is missing, and unloads the kind to S3 as gzipped JSON lines.
- **Loads PostgreSQL whole.** The loader replaces each table outright, leaves out the events of pubkeys on an optional blocklist of bots and spam ([blocklists/README.md](blocklists/README.md)), and refuses a load that would shrink a table below its floor. The views and tables built from the loaded ones are refreshed after.
- **Serves metrics over MCP** for the Zapstore, Nutzaps and Routstr domains, and reference records for Zapstore and Routstr.
- **Reports failures.** A failed run, and the recovery after it, is posted to Discord, naming what it leaves stale on nostrolo.gy and in the MCP server, and a watchdog reports a job whose latest run failed or is over a day old.

## Jobs

| Job | What it covers |
|---|---|
| `zapstore_refresh` | Zapstore's apps, their releases and the zaps they receive |
| `marketplaces_refresh` | listings on Nostr marketplaces |
| `ngit_refresh` | git collaboration on Nostr: repositories, pushes, stars and zaps |
| `lightning_refresh` | zaps by the Lightning provider that signed each receipt |
| `nutzaps_refresh` | nutzaps, zaps paid in ecash, and the mints behind them |
| `relays_refresh` | the relays people list in their relay lists |
| `routstr_refresh` | Routstr, a marketplace for AI inference |

nozzl's `config/kinds.toml` lists the kinds these jobs read, grouped by domain. [docs/dataset-reference/](docs/dataset-reference/) defines what each metric of the Marketplaces, Git-on-Nostr and Routstr domains counts.

## Running it

### The pipeline

Dagster runs the pipeline from one image, built from the repository root with `deploy/dagster/Dockerfile.dagster`: a code server, a daemon that fires the schedules and sensors, and a web UI, beside a PostgreSQL that keeps Dagster's run history. The settings are environment variables, `NOSTROLOGY_*` and `NOZZL_*`, read in [dagster_code/pipeline/config.py](dagster_code/pipeline/config.py), and `DAGSTER_DISCORD_WEBHOOK` turns on the Discord posts. The deployment's own AWS coordinates have no default, so every job fails, naming each one that is missing, until all five are set: `NOSTROLOGY_AURORA_CLUSTER`, `NOSTROLOGY_ATHENA_WORKGROUP`, `NOSTROLOGY_EXTRACTS_BUCKET`, `NOSTROLOGY_GLUE_DB` and `NOZZL_ARCHIVE_BUCKET` (the nozzl archive's bucket). The Nectar job also needs `NECTAR_BUCKET`. The pipeline applies [schema/init.sql](schema/init.sql) before each load. [schema/roles.sql](schema/roles.sql) creates the read-only roles that nostrology-web and the MCP server connect as, without passwords: a deployment gives each one a password.

### The MCP server

From the repository root, in the MCP environment under Development:

```sh
.venv-mcp/bin/python -m nostrology_mcp.server                     # over stdio
NOSTROLOGY_MCP_TRANSPORT=streamable-http \
  .venv-mcp/bin/python -m nostrology_mcp.server                   # over HTTP, on 127.0.0.1:8080
```

It reads PostgreSQL as a read-only role, through `NOSTROLOGY_PGHOST`, `NOSTROLOGY_PGPORT`, `NOSTROLOGY_PGDATABASE`, `NOSTROLOGY_PGUSER` and `NOSTROLOGY_PGPASSWORD`. `NOSTROLOGY_MCP_HOST` and `NOSTROLOGY_MCP_PORT` change the HTTP address. Over HTTP it serves:

| Route | What it does |
|---|---|
| `/mcp` | the MCP endpoint (Streamable HTTP) |
| `GET /registry.json` | every metric and reference record, with its methodology |
| `GET /health` | `200 (ok)` while the server runs; it touches no database |

The server's image builds from the repository root with `deploy/Dockerfile.mcp`, and [nostrology_mcp/README.md](nostrology_mcp/README.md) is its reference.

## Documentation

- [docs/design/dagster-orchestration-design.md](docs/design/dagster-orchestration-design.md): the orchestration, from the job per domain to the extract and load steps and the alerts.
- [docs/design/](docs/design/): a design doc each for the Marketplaces, Git-on-Nostr, Nutzaps and Routstr domains, and the design docs of the MCP server's domains.
- [docs/dataset-reference/](docs/dataset-reference/): what every metric of the Marketplaces, Git-on-Nostr and Routstr domains counts.
- [nostrology_mcp/README.md](nostrology_mcp/README.md): the MCP server's tools, domains and records.

## Development

You need Python 3.12, which the locks were compiled for. From the repository root:

```sh
# the pipeline and the loader
python3.12 -m venv .venv
.venv/bin/pip install --require-hashes --only-binary :all: --find-links deploy/dagster/wheels -r dagster_code/requirements.txt
.venv/bin/pip install --require-hashes --only-binary :all: -r dagster_code/requirements-test.txt
.venv/bin/python -m pytest dagster_code/tests loader

# the MCP server
python3.12 -m venv .venv-mcp
.venv-mcp/bin/pip install --require-hashes --only-binary :all: -r nostrology_mcp/requirements.txt -r nostrology_mcp/requirements-dev.txt
.venv-mcp/bin/python -m pytest nostrology_mcp/tests
```

- The loader's and the MCP server's database tests run when `NOSTROLOGY_CONTRACT_DSN` names a scratch PostgreSQL 16 database, one per suite, and skip otherwise. [.github/workflows/gate.yml](.github/workflows/gate.yml), which runs all of the above, creates them with the collation production uses.
- The tests that run the extract queries on DuckDB skip unless `duckdb` is installed, at the version [requirements-pipeline.txt](requirements-pipeline.txt) pins.
- The Python dependencies are hash-locked. Change one in its `.in` file and regenerate the lock with `scripts/python-lock.sh`, never by editing a lock.
- [CLAUDE.md](CLAUDE.md) has the contributor conventions.

| Path | What it holds |
|---|---|
| `dagster_code/` | the jobs, schedules and sensors, and in `pipeline/` the extract, load and compute steps |
| `loader/` | the loader that streams the extracts into PostgreSQL |
| `sql/` | the Athena extract queries |
| `schema/` | the serving tables and views, and the read-only roles |
| `data/` | the reviewed nutzap mint registry and the Lightning provider signers |
| `src/shared/` | the zap receipt parser the loader and the Git-on-Nostr computation share |
| `nostrology_mcp/` | the MCP server |
| `deploy/` | the images' Dockerfiles |
| `scripts/` | the lock tooling and the repository's checks |
| `docs/` | the design docs and the dataset references |

## License

Copyright (C) 2026 the Nostrology authors.

nostrology-data is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

nostrology-data is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

The license's full text is in [LICENSE](LICENSE). What this repository ships from other projects keeps its own license: [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) lists it.
