-- sql/01_create_schema.sql
-- -------------------------
-- Run this once in Snowflake to set up the database and schema.
-- Warehouse: COMPUTE_WH | Database: NYC_TAXI_DB | Schema: RAW

-- Create database
CREATE DATABASE IF NOT EXISTS NYC_TAXI_DB;
USE DATABASE NYC_TAXI_DB;

-- Create schemas (medallion pattern)
CREATE SCHEMA IF NOT EXISTS RAW;      -- raw ingested data
CREATE SCHEMA IF NOT EXISTS STAGING;  -- cleaned / validated data
CREATE SCHEMA IF NOT EXISTS ANALYTICS; -- aggregated KPIs

-- Create raw table
CREATE TABLE IF NOT EXISTS RAW.YELLOW_TAXI_TRIPS (
    TPEP_PICKUP_DATETIME        TIMESTAMP_NTZ,
    TPEP_DROPOFF_DATETIME       TIMESTAMP_NTZ,
    PASSENGER_COUNT             NUMBER(2),
    TRIP_DISTANCE               FLOAT,
    PULOCATIONID                NUMBER(5),
    DOLOCATIONID                NUMBER(5),
    PAYMENT_TYPE                VARCHAR(50),
    FARE_AMOUNT                 FLOAT,
    TIP_AMOUNT                  FLOAT,
    TOTAL_AMOUNT                FLOAT,
    TRIP_DURATION_MINUTES       FLOAT,
    PICKUP_HOUR                 NUMBER(2),
    PICKUP_DAY_OF_WEEK          VARCHAR(20),
    PICKUP_MONTH                NUMBER(2),
    PICKUP_YEAR                 NUMBER(4),
    FARE_PER_MILE               FLOAT,
    ETL_LOADED_AT               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
