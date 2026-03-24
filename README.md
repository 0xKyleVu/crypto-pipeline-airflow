# crypto-pipeline-airflow

Production-style Airflow project for ingesting cryptocurrency market data from CoinGecko and loading it into BigQuery via GCS.

## What this project does

- Fetches top 10 crypto assets from CoinGecko API.
- Stores raw JSON in Google Cloud Storage.
- Transforms records to tabular CSV format.
- Loads transformed data into BigQuery.

## DAG

- DAG ID: `crypto_exchange_pipeline`
- Schedule: every 10 minutes
- Flow: `CoinGecko API -> local worker file -> GCS -> BigQuery`

## Architecture and diagrams

- Architecture notes: [`docs/architecture.md`](docs/architecture.md)
- Editable diagram: [`docs/crypto_pipeline_airflow.drawio`](docs/crypto_pipeline_airflow.drawio)
- Exported images folder: [`docs/images/`](docs/images/)

Place exported PNG/SVG diagrams in `docs/images/` and embed them here:

```md
![Crypto Pipeline Architecture](docs/images/crypto_pipeline_airflow.png)
```

## Project structure

```text
crypto-pipeline-airflow/
├─ dags/
│  └─ crypto_exchange_pipeline.py
├─ tests/
│  └─ test_crypto_exchange_pipeline.py
├─ docs/
│  ├─ architecture.md
│  ├─ crypto_pipeline_airflow.drawio
│  └─ images/
├─ .github/workflows/ci.yml
├─ .env.example
├─ .gitignore
├─ requirements.txt
├─ LICENSE
└─ README.md
```

## Setup

1. Copy `.env.example` to `.env` and update values.
2. Make sure Airflow has the Google connection `google_cloud_default`.
3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Place DAG in your Airflow `dags` folder (or run this repo as your Airflow project root).

## Environment variables

- `GCP_PROJECT`
- `GCS_BUCKET`
- `GCS_RAW_DATA_PATH`
- `GCS_TRANSFORMED_PATH`
- `BIGQUERY_DATASET`
- `BIGQUERY_TABLE`

## Local validation

```bash
python -m pytest -q
```

## Notes

- This repo excludes local Airflow metadata/log volume output.
- Do not commit credential files or `.env`.
