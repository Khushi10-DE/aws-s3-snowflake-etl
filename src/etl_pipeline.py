"""
ETL Pipeline: AWS S3 → Snowflake
---------------------------------
Extracts raw NYC Taxi trip data from AWS S3,
transforms and cleans with Pandas,
loads into Snowflake data warehouse.

Author: Khushbu Gohil
GitHub: https://github.com/Khushi10-DE
"""

import logging
import sys
from datetime import datetime

import boto3
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from config_loader import load_config
from transform import transform_taxi_data
from validate import validate_dataframe

# ── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"logs/etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    ],
)
log = logging.getLogger(__name__)


# ── Extract ───────────────────────────────────────────────────────────────────
def extract_from_s3(bucket: str, key: str, aws_config: dict) -> pd.DataFrame:
    """Download a CSV/Parquet file from S3 and return as a DataFrame."""
    log.info(f"Extracting from s3://{bucket}/{key}")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_config["aws_access_key_id"],
        aws_secret_access_key=aws_config["aws_secret_access_key"],
        region_name=aws_config["region"],
    )

    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"]

    if key.endswith(".parquet"):
        df = pd.read_parquet(body)
    else:
        df = pd.read_csv(body)

    log.info(f"Extracted {len(df):,} rows | {len(df.columns)} columns")
    return df


# ── Load ──────────────────────────────────────────────────────────────────────
def load_to_snowflake(df: pd.DataFrame, table: str, sf_config: dict, mode: str = "append") -> None:
    """Load a DataFrame into a Snowflake table using write_pandas."""
    log.info(f"Loading {len(df):,} rows → Snowflake table: {table}")

    conn = snowflake.connector.connect(
        user=sf_config["user"],
        password=sf_config["password"],
        account=sf_config["account"],
        warehouse=sf_config["warehouse"],
        database=sf_config["database"],
        schema=sf_config["schema"],
        role=sf_config.get("role", "SYSADMIN"),
    )

    try:
        if mode == "truncate":
            cur = conn.cursor()
            cur.execute(f"TRUNCATE TABLE IF EXISTS {table}")
            log.info(f"Truncated table {table} before load")

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table.upper(),
            auto_create_table=True,
            overwrite=(mode == "overwrite"),
        )

        if success:
            log.info(f"Load successful — {nrows:,} rows written in {nchunks} chunk(s)")
        else:
            raise RuntimeError("write_pandas returned success=False")

    finally:
        conn.close()


# ── Orchestrate ───────────────────────────────────────────────────────────────
def run_pipeline(config_path: str = "config/config.yaml") -> None:
    """End-to-end ETL orchestration."""
    start = datetime.now()
    log.info("=" * 60)
    log.info("ETL Pipeline started")
    log.info("=" * 60)

    config = load_config(config_path)
    aws_cfg = config["aws"]
    sf_cfg = config["snowflake"]
    pipeline_cfg = config["pipeline"]

    # ── EXTRACT
    raw_df = extract_from_s3(
        bucket=aws_cfg["bucket"],
        key=pipeline_cfg["s3_key"],
        aws_config=aws_cfg,
    )

    # ── TRANSFORM
    log.info("Transforming data...")
    clean_df = transform_taxi_data(raw_df)
    log.info(f"After transform: {len(clean_df):,} rows")

    # ── VALIDATE
    log.info("Validating data...")
    validate_dataframe(clean_df)

    # ── LOAD
    load_to_snowflake(
        df=clean_df,
        table=pipeline_cfg["target_table"],
        sf_config=sf_cfg,
        mode=pipeline_cfg.get("load_mode", "append"),
    )

    duration = (datetime.now() - start).seconds
    log.info("=" * 60)
    log.info(f"Pipeline completed successfully in {duration}s")
    log.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()
