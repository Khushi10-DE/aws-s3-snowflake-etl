"""
tests/test_transform.py
-----------------------
Unit tests for transform.py using pytest.
Run with: pytest tests/ -v

Author: Khushbu Gohil
"""

import pandas as pd
import pytest
from src.transform import transform_taxi_data, _standardise_columns, _remove_outliers


# ── Fixtures ──────────────────────────────────────────────────────────────────
@pytest.fixture
def sample_df():
    """Minimal valid NYC Taxi DataFrame for testing."""
    return pd.DataFrame({
        "tpep_pickup_datetime":  ["2024-01-15 08:30:00", "2024-01-15 09:00:00", "2024-01-15 10:00:00"],
        "tpep_dropoff_datetime": ["2024-01-15 08:45:00", "2024-01-15 09:25:00", "2024-01-15 10:35:00"],
        "passenger_count":       [1, 2, 1],
        "trip_distance":         [2.5, 5.1, 3.8],
        "fare_amount":           [12.5, 22.0, 16.0],
        "tip_amount":            [2.5, 4.0, 3.0],
        "total_amount":          [15.0, 26.0, 19.0],
        "pulocationid":          [161, 234, 79],
        "dolocationid":          [230, 100, 161],
        "payment_type":          ["Credit card", "Credit card", "Cash"],
    })


# ── Column standardisation ─────────────────────────────────────────────────
def test_standardise_columns_lowercase():
    df = pd.DataFrame({"Column One": [1], "Column-Two": [2]})
    result = _standardise_columns(df)
    assert list(result.columns) == ["column_one", "column_two"]


# ── Outlier removal ────────────────────────────────────────────────────────
def test_remove_outliers_fare(sample_df):
    sample_df.loc[0, "fare_amount"] = 999  # inject outlier
    result = _remove_outliers(sample_df)
    assert (result["fare_amount"] <= 500).all()


def test_remove_outliers_distance(sample_df):
    sample_df.loc[0, "trip_distance"] = 200  # inject outlier
    result = _remove_outliers(sample_df)
    assert (result["trip_distance"] <= 100).all()


# ── Full transform ─────────────────────────────────────────────────────────
def test_transform_adds_duration(sample_df):
    result = transform_taxi_data(sample_df)
    assert "trip_duration_minutes" in result.columns
    assert (result["trip_duration_minutes"] > 0).all()


def test_transform_adds_pickup_hour(sample_df):
    result = transform_taxi_data(sample_df)
    assert "pickup_hour" in result.columns
    assert result["pickup_hour"].between(0, 23).all()


def test_transform_adds_etl_loaded_at(sample_df):
    result = transform_taxi_data(sample_df)
    assert "etl_loaded_at" in result.columns


def test_transform_no_nulls_in_key_cols(sample_df):
    result = transform_taxi_data(sample_df)
    for col in ["fare_amount", "trip_distance", "trip_duration_minutes"]:
        assert result[col].isnull().sum() == 0, f"Nulls found in {col}"


def test_transform_output_has_rows(sample_df):
    result = transform_taxi_data(sample_df)
    assert len(result) > 0
