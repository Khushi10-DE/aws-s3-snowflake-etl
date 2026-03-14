"""
transform.py
------------
All cleaning and transformation logic for NYC Taxi trip data.
Keeps etl_pipeline.py clean — one function per concern.

Author: Khushbu Gohil
"""

import logging

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


def transform_taxi_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full transformation pipeline for NYC Yellow Taxi data.

    Steps:
        1. Standardise column names
        2. Parse & validate datetime columns
        3. Drop nulls and duplicates
        4. Remove outliers (fare, distance, duration)
        5. Engineer new features
        6. Cast final dtypes
    """
    log.info(f"Starting transform — input shape: {df.shape}")

    df = _standardise_columns(df)
    df = _parse_datetimes(df)
    df = _drop_nulls_and_duplicates(df)
    df = _remove_outliers(df)
    df = _engineer_features(df)
    df = _cast_dtypes(df)

    log.info(f"Transform complete — output shape: {df.shape}")
    return df


# ── Step helpers ──────────────────────────────────────────────────────────────

def _standardise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase and snake_case all column names."""
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[\s\-]+", "_", regex=True)
    )
    return df


def _parse_datetimes(df: pd.DataFrame) -> pd.DataFrame:
    """Parse pickup and dropoff timestamps."""
    for col in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def _drop_nulls_and_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows with critical nulls and exact duplicates."""
    critical_cols = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
        "pulocationid",
        "dolocationid",
    ]
    existing = [c for c in critical_cols if c in df.columns]
    before = len(df)
    df = df.dropna(subset=existing)
    df = df.drop_duplicates()
    after = len(df)
    log.info(f"Dropped {before - after:,} rows (nulls + duplicates)")
    return df


def _remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out physically impossible or extreme values."""
    before = len(df)

    # Fare: $1 – $500
    if "fare_amount" in df.columns:
        df = df[(df["fare_amount"] >= 1) & (df["fare_amount"] <= 500)]

    # Distance: 0.1 – 100 miles
    if "trip_distance" in df.columns:
        df = df[(df["trip_distance"] >= 0.1) & (df["trip_distance"] <= 100)]

    # Passengers: 1 – 6
    if "passenger_count" in df.columns:
        df = df[(df["passenger_count"] >= 1) & (df["passenger_count"] <= 6)]

    log.info(f"Removed {before - len(df):,} outlier rows")
    return df


def _engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create derived columns useful for analytics."""
    if "tpep_pickup_datetime" in df.columns and "tpep_dropoff_datetime" in df.columns:
        df["trip_duration_minutes"] = (
            (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
            .dt.total_seconds()
            .div(60)
            .round(2)
        )
        df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
        df["pickup_day_of_week"] = df["tpep_pickup_datetime"].dt.day_name()
        df["pickup_month"] = df["tpep_pickup_datetime"].dt.month
        df["pickup_year"] = df["tpep_pickup_datetime"].dt.year

        # Filter trips with logical duration (1 min – 3 hrs)
        df = df[(df["trip_duration_minutes"] >= 1) & (df["trip_duration_minutes"] <= 180)]

    if "fare_amount" in df.columns and "trip_distance" in df.columns:
        df["fare_per_mile"] = (df["fare_amount"] / df["trip_distance"]).round(2)

    df["etl_loaded_at"] = pd.Timestamp.utcnow()

    return df


def _cast_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure consistent dtypes before loading to Snowflake."""
    int_cols = ["passenger_count", "pulocationid", "dolocationid",
                "pickup_hour", "pickup_month", "pickup_year"]
    float_cols = ["fare_amount", "trip_distance", "tip_amount",
                  "total_amount", "trip_duration_minutes", "fare_per_mile"]
    str_cols = ["pickup_day_of_week", "payment_type"]

    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].astype("Int64")

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(4)

    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)

    return df
