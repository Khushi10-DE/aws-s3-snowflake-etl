# AWS S3 → Snowflake ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS_S3-Storage-569A31?style=flat-square&logo=amazon-s3&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-29B5E8?style=flat-square&logo=snowflake&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-Transform-150458?style=flat-square&logo=pandas&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)

A production-style ETL pipeline that ingests the **NYC Yellow Taxi** public dataset from **AWS S3**, transforms and validates it with **Python/Pandas**, and loads it into a **Snowflake** data warehouse for analytics.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        ETL PIPELINE                             │
│                                                                 │
│   ┌──────────┐    ┌────────────┐    ┌──────────┐    ┌────────┐ │
│   │  AWS S3  │───▶│  Extract   │───▶│Transform │───▶│  Load  │ │
│   │ (Source) │    │ boto3 API  │    │  Pandas  │    │  Snow- │ │
│   │.parquet  │    │            │    │ + Validate│   │  flake │ │
│   └──────────┘    └────────────┘    └──────────┘    └────────┘ │
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐  │
│   │                   Snowflake                             │  │
│   │   RAW schema → STAGING schema → ANALYTICS schema        │  │
│   │   (ingested)    (cleaned)        (aggregated KPIs)       │  │
│   └─────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow

| Step | Tool | Description |
|------|------|-------------|
| **Extract** | `boto3` | Downloads `.parquet` file from S3 into a Pandas DataFrame |
| **Transform** | `pandas` | Cleans nulls, removes outliers, engineers features |
| **Validate** | custom | Data quality checks before any data touches Snowflake |
| **Load** | `snowflake-connector` | Bulk-loads clean DataFrame using `write_pandas` |

---

## Dataset

**NYC Yellow Taxi Trip Records** — public dataset from the NYC Taxi & Limousine Commission.

- Source: [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- Format: Parquet (~50MB per monthly file)
- Key fields: pickup/dropoff times, trip distance, fare amount, tip, payment type, location IDs

---

## Project Structure

```
aws-s3-snowflake-etl/
│
├── src/
│   ├── etl_pipeline.py       # Main orchestrator (Extract → Transform → Load)
│   ├── transform.py          # All Pandas cleaning & feature engineering
│   ├── validate.py           # Data quality checks
│   └── config_loader.py      # YAML + env variable config loader
│
├── sql/
│   ├── 01_create_schema.sql  # Snowflake DB, schema, and table setup
│   └── 02_analytics_queries.sql  # Sample analytical queries
│
├── config/
│   └── config.yaml           # Non-sensitive config (bucket, table names)
│
├── tests/
│   └── test_transform.py     # pytest unit tests for transform logic
│
├── .env.example              # Template for credentials (never commit .env)
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Transformations Applied

| Transformation | Detail |
|---|---|
| Column standardisation | Lowercase + snake_case all column names |
| Datetime parsing | Parse pickup/dropoff to `TIMESTAMP_NTZ` |
| Null removal | Drop rows missing critical fields |
| Duplicate removal | Drop exact duplicate rows |
| Outlier filtering | Fare: $1–$500 · Distance: 0.1–100 mi · Passengers: 1–6 |
| Feature: trip duration | `(dropoff - pickup)` in minutes |
| Feature: pickup hour | Hour of day (0–23) for time-of-day analysis |
| Feature: day of week | Monday–Sunday for demand pattern analysis |
| Feature: fare per mile | `fare_amount / trip_distance` |
| ETL audit column | `etl_loaded_at` timestamp added to every row |

---

## Setup & Usage

### 1. Clone the repo
```bash
git clone https://github.com/Khushi10-DE/aws-s3-snowflake-etl.git
cd aws-s3-snowflake-etl
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure credentials
```bash
cp .env.example .env
# Edit .env with your AWS and Snowflake credentials
```

### 4. Set up Snowflake
```sql
-- Run in your Snowflake worksheet
-- See sql/01_create_schema.sql
```

### 5. Run the pipeline
```bash
python src/etl_pipeline.py
```

### 6. Run tests
```bash
pytest tests/ -v
```

---

## Sample Output

```
2024-01-15 10:22:01 | INFO | ETL Pipeline started
2024-01-15 10:22:01 | INFO | Extracting from s3://your-bucket/raw/nyc_taxi/yellow_tripdata_2024-01.parquet
2024-01-15 10:22:08 | INFO | Extracted 2,964,624 rows | 19 columns
2024-01-15 10:22:08 | INFO | Starting transform — input shape: (2964624, 19)
2024-01-15 10:22:12 | INFO | Dropped 12,847 rows (nulls + duplicates)
2024-01-15 10:22:14 | INFO | Removed 3,291 outlier rows
2024-01-15 10:22:15 | INFO | Transform complete — output shape: (2948486, 25)
2024-01-15 10:22:15 | INFO | Validation passed — 2,948,486 rows ready to load
2024-01-15 10:22:15 | INFO | Loading 2,948,486 rows → Snowflake table: YELLOW_TAXI_TRIPS
2024-01-15 10:22:41 | INFO | Load successful — 2,948,486 rows written in 30 chunk(s)
2024-01-15 10:22:41 | INFO | Pipeline completed successfully in 40s
```

---

## Analytics Insights (sample)

After loading, run `sql/02_analytics_queries.sql` in Snowflake:

- **Peak hours**: Most trips occur 6–9am and 4–7pm
- **Busiest day**: Friday has highest trip volume and average fare
- **Top pickup zone**: Midtown Manhattan (LocationID 161)
- **Payment split**: ~70% credit card, ~25% cash

---

## Skills Demonstrated

- `boto3` for AWS S3 data extraction
- `pandas` for large-scale data transformation and cleaning
- Snowflake data warehouse design (schemas, data types, bulk load)
- `snowflake-connector-python` with `write_pandas` for efficient loading
- Modular Python project structure (ETL separation of concerns)
- Data quality validation layer
- Environment-based credential management (`.env` + `python-dotenv`)
- Unit testing with `pytest`

---

## Author

**Khushbu Gohil** — Junior Data Engineer
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5?style=flat-square&logo=linkedin)](https://linkedin.com/in/khushbugohil10)
[![GitHub](https://img.shields.io/badge/GitHub-Khushi10--DE-181717?style=flat-square&logo=github)](https://github.com/Khushi10-DE)
