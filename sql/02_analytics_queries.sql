-- sql/02_analytics_queries.sql
-- -----------------------------
-- Sample analytical queries to validate the loaded data
-- and showcase insights from the NYC Taxi dataset.

USE DATABASE NYC_TAXI_DB;
USE SCHEMA RAW;

-- ── 1. Row count & load check ─────────────────────────────────────────────
SELECT
    COUNT(*)                        AS total_trips,
    MIN(ETL_LOADED_AT)              AS first_loaded,
    MAX(ETL_LOADED_AT)              AS last_loaded
FROM YELLOW_TAXI_TRIPS;


-- ── 2. Average fare, distance, and duration by hour ──────────────────────
SELECT
    PICKUP_HOUR,
    COUNT(*)                        AS total_trips,
    ROUND(AVG(FARE_AMOUNT), 2)      AS avg_fare,
    ROUND(AVG(TRIP_DISTANCE), 2)    AS avg_distance_miles,
    ROUND(AVG(TRIP_DURATION_MINUTES), 1) AS avg_duration_min
FROM YELLOW_TAXI_TRIPS
GROUP BY PICKUP_HOUR
ORDER BY PICKUP_HOUR;


-- ── 3. Busiest day of week ────────────────────────────────────────────────
SELECT
    PICKUP_DAY_OF_WEEK,
    COUNT(*)                        AS total_trips,
    ROUND(AVG(FARE_AMOUNT), 2)      AS avg_fare
FROM YELLOW_TAXI_TRIPS
GROUP BY PICKUP_DAY_OF_WEEK
ORDER BY total_trips DESC;


-- ── 4. Top 10 pickup locations ────────────────────────────────────────────
SELECT
    PULOCATIONID                    AS pickup_zone,
    COUNT(*)                        AS trips,
    ROUND(AVG(FARE_AMOUNT), 2)      AS avg_fare
FROM YELLOW_TAXI_TRIPS
GROUP BY PULOCATIONID
ORDER BY trips DESC
LIMIT 10;


-- ── 5. Payment type breakdown ─────────────────────────────────────────────
SELECT
    PAYMENT_TYPE,
    COUNT(*)                        AS trips,
    ROUND(AVG(TIP_AMOUNT), 2)       AS avg_tip,
    ROUND(SUM(TOTAL_AMOUNT), 2)     AS total_revenue
FROM YELLOW_TAXI_TRIPS
GROUP BY PAYMENT_TYPE
ORDER BY trips DESC;


-- ── 6. Revenue by month ───────────────────────────────────────────────────
SELECT
    PICKUP_YEAR,
    PICKUP_MONTH,
    COUNT(*)                        AS total_trips,
    ROUND(SUM(TOTAL_AMOUNT), 2)     AS total_revenue,
    ROUND(AVG(FARE_AMOUNT), 2)      AS avg_fare
FROM YELLOW_TAXI_TRIPS
GROUP BY PICKUP_YEAR, PICKUP_MONTH
ORDER BY PICKUP_YEAR, PICKUP_MONTH;
