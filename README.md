# OpenLineage Validation

This repo contains a simple CLI that can be used with `openlineage-python` to emit OpenLineage messages from a JSON file to a backend.

The `events/` directory contains several sets of OpenLineage messages. Some of these were derived from Marquez seed data, while others were captured from running pipelines.

## Usage

To set up:

```
% python -m venv venv
% source venv/bin/activate
% pip install -r requirements.txt
```

To use:

```
% OPENLINEAGE_CONFIG=/path/to/openlineage.yml ./olval.py emit events/dbt-bigquery.json
% OPENLINEAGE_URL=http://localhost:5000 ./olval.py emit events/airflow-postgres.json
```
