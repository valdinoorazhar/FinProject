# FinProject

## How to uunderstand this codebase

This is an ELT Pipeline utilizing Dagster.
Each information is describe by Dagster "Assets" in src\FinProject\defs\assets.

These assets are materialized from source csv file into Clickhouse table.

There are 4 Python scripts there, 3 define the definition and logic of data asset and 1 for data quality check.

These "assets" are scheduled in src\FinProject\defs\Scheduled.py in daily manner.

## Getting started

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

### Running Dagster

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.
Enable `daily_asset_schedule` in the Dagster UI to materialize all assets daily
at 6:00 AM Asia/Jakarta time. Partitioned assets process the latest completed
daily partition.



## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
