# FinProject

## Project overview

This project is a batch ELT pipeline built with Dagster. Assets in
`src/FinProject/defs/assets` load the branches, customers, and transactions CSV
files into ClickHouse staging and warehouse tables.

The daily schedule is defined in `src/FinProject/defs/schedules.py`. Warehouse
primary-key uniqueness checks are defined in
`src/FinProject/defs/assets/dwh_checks.py`.

## Getting started

### Start ClickHouse locally

Requirements: Docker Desktop (or Docker Engine) with the Docker Compose v2
plugin.

If you do not already have a `.env` file, create one with the local connection
settings below. Keep any existing `.env` and update its settings rather than
overwriting it:

```dotenv
DB_HOST=localhost
DB_PORT=8123
DB_USER=finproject
DB_PASSWORD=finproject-local-only
DB_NAME_STAGING=FIN_STG
DB_NAME_DWH=FIN_DWH
```

The Compose defaults use the same local-only credentials, so you can also start
ClickHouse before creating `.env`:

```powershell
docker compose up -d
docker compose ps
```

The Compose setup exposes ClickHouse HTTP on `localhost:8123`, persists its data
in the `clickhouse-data` volume, and creates the `FIN_STG` and `FIN_DWH`
databases using an idempotent initialization service. `.env` is ignored by
Git. If you change the credentials, use the same values for both Compose and
Dagster. Keep `DB_HOST=localhost` for Dagster running on your host and use the
database names shown above.

To stop ClickHouse without deleting its data:

```powershell
docker compose down
```

### Installing dependencies

**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

**Option 2: pip**

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Run Dagster

Start Dagster with the ClickHouse connection settings from `.env`:

```bash
uv run --env-file .env dg dev
```

Open http://localhost:3000 in your browser to see the project.
The `daily_asset_schedule` is configured to run at 6:00 AM Asia/Jakarta time.
Enable it in the Dagster UI to materialize the assets. The customers and
transactions warehouse assets use daily partitions.


## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
