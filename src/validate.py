"""
validate.py
-----------
Data quality checks before loading to Snowflake.
Raises ValueError if critical checks fail — stops a bad load early.

Author: Khushbu Gohil
"""

import logging

import pandas as pd

log = logging.getLogger(__name__)


def validate_dataframe(df: pd.DataFrame) -> None:
    """
    Run a suite of data quality checks.
    Raises ValueError on critical failures.
    Logs warnings on non-critical issues.
    """
    log.info("Running data quality checks...")
    errors = []
    warnings = []

    # ── Critical checks ───────────────────────────────────────────────────────
    if df.empty:
        errors.append("DataFrame is empty — nothing to load")

    if len(df) < 10:
        errors.append(f"Too few rows ({len(df)}) — likely a bad extract")

    required_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime",
                     "trip_distance", "fare_amount"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {missing}")

    # ── Non-critical checks ───────────────────────────────────────────────────
    null_pct = df.isnull().mean() * 100
    high_null_cols = null_pct[null_pct > 20].index.tolist()
    if high_null_cols:
        warnings.append(f"Columns with >20% nulls: {high_null_cols}")

    if "fare_amount" in df.columns:
        neg_fares = (df["fare_amount"] < 0).sum()
        if neg_fares > 0:
            warnings.append(f"{neg_fares} rows have negative fare_amount")

    if "trip_duration_minutes" in df.columns:
        zero_duration = (df["trip_duration_minutes"] <= 0).sum()
        if zero_duration > 0:
            warnings.append(f"{zero_duration} rows have zero or negative trip duration")

    # ── Report ────────────────────────────────────────────────────────────────
    for w in warnings:
        log.warning(f"Data quality warning: {w}")

    if errors:
        for e in errors:
            log.error(f"Data quality error: {e}")
        raise ValueError(f"Validation failed with {len(errors)} error(s): {errors}")

    log.info(f"Validation passed — {len(df):,} rows ready to load")
    _log_summary(df)


def _log_summary(df: pd.DataFrame) -> None:
    """Log a brief summary of the clean dataset."""
    log.info(f"  Rows       : {len(df):,}")
    log.info(f"  Columns    : {len(df.columns)}")
    if "fare_amount" in df.columns:
        log.info(f"  Avg fare   : ${df['fare_amount'].mean():.2f}")
    if "trip_distance" in df.columns:
        log.info(f"  Avg dist   : {df['trip_distance'].mean():.2f} miles")
    if "trip_duration_minutes" in df.columns:
        log.info(f"  Avg duration: {df['trip_duration_minutes'].mean():.1f} min")
