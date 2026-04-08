-- snowflake_setup.sql
-- Run this once in Snowflake to set up the NZ Housing database
-- before triggering the Airflow DAG

-- Create database
CREATE DATABASE IF NOT EXISTS NZ_HOUSING;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS NZ_HOUSING.RAW;
CREATE SCHEMA IF NOT EXISTS NZ_HOUSING.STAGING;
CREATE SCHEMA IF NOT EXISTS NZ_HOUSING.MARTS;

-- Use RAW schema for table creation
USE DATABASE NZ_HOUSING;
USE SCHEMA RAW;

-- Building consents table
CREATE TABLE IF NOT EXISTS BUILDING_CONSENTS (
    period           VARCHAR,
    houses           NUMBER,
    apartments       NUMBER,
    retirement_units NUMBER,
    townhouses       NUMBER,
    all_dwellings    NUMBER,
    floor_area       FLOAT,
    value            FLOAT
);

-- HUD rental index table
CREATE TABLE IF NOT EXISTS HUD_RENTAL_INDEX (
    region              VARCHAR,
    annual_change       FLOAT,
    period              DATE,
    rental_price_index  FLOAT
);

-- Mortgage rates table
CREATE TABLE IF NOT EXISTS MORTGAGE_RATES (
    date      DATE,
    term      VARCHAR,
    rate_pct  FLOAT,
    series    VARCHAR
);
