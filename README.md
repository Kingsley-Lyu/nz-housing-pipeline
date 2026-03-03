# 🏠 NZ Housing Market Analytics Pipeline

A end-to-end data engineering project that ingests, transforms, and analyses New Zealand housing market data to uncover trends in property prices, affordability, and regional demand.

---

## 📌 Project Overview

New Zealand's housing market is one of the most discussed economic topics in the country. This project builds a production-style data pipeline that collects housing data from multiple NZ sources, loads it into a cloud data warehouse, and transforms it into analytical models ready for reporting.

This project demonstrates core data engineering skills including batch ingestion, data modelling, ELT transformations with dbt, orchestration with Airflow, and data quality testing.

---

## 🎯 Objectives

- Build a scalable batch pipeline ingesting NZ housing data monthly
- Model data using a star schema (fact + dimension tables) in Snowflake
- Apply data quality checks and transformation logic using dbt
- Orchestrate the pipeline end-to-end with Apache Airflow
- Deliver analytical outputs: median prices by region, affordability index, and supply trends

---

## 🗂️ Data Sources

## 📦 Data Sources

| # | Source | Data | Format | Frequency |
|---|---|---|---|---|
| 1 | Stats NZ | Building consents, rental indexes, population by region | CSV | Monthly |
| 2 | Trademe Property API | Residential listings, asking prices, days on market | API (JSON) | Weekly |
| 3 | REINZ / CoreLogic | Median sale prices, sales volumes by region | CSV / PDF | Monthly |
| 4 | Reserve Bank NZ | Interest rates, mortgage rates | CSV | Monthly |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    DATA SOURCES                         │
│   Stats NZ CSV   │   Trademe API   │   REINZ Reports   │
└────────┬─────────────────┬─────────────────┬────────────┘
         │                 │                 │
         ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────┐
│              INGESTION LAYER (Python)                   │
│         pandas + requests + boto3                       │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│              STORAGE LAYER (AWS S3)                     │
│         Raw files land in S3 Data Lake                  │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│            DATA WAREHOUSE (Snowflake)                   │
│   RAW schema → STAGING schema → MARTS schema            │
│         (Bronze)       (Silver)      (Gold)             │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│          TRANSFORMATION LAYER (dbt)                     │
│   Staging models → Intermediate → Fact & Dim tables     │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│         ORCHESTRATION (Apache Airflow)                  │
│       Monthly DAG: Ingest → Load → Transform → Test     │
└─────────────────────────────────────────────────────────┘
```

---

## 📊 Data Model (Star Schema)

```
                    ┌─────────────┐
                    │  DIM_TIME   │
                    │─────────────│
                    │ date_id (PK)│
                    │ year        │
                    │ month       │
                    │ quarter     │
                    │ is_covid_era│
                    └──────┬──────┘
                           │
┌─────────────┐    ┌───────┴──────────────┐
│ DIM_REGION  │    │  FACT_PROPERTY_SALES  │
│─────────────│    │──────────────────────│
│ region_id   ├────│ sale_id (PK)          │
│ region_name │    │ region_id (FK)        │
│ territorial │    │ date_id (FK)          │
│ island      │    │ median_price          │
└─────────────┘    │ num_sales             │
                   │ days_on_market        │
                   │ affordability_index   │
                   └──────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Tool |
|---|---|
| Language | Python 3.10+ |
| Storage | AWS S3 |
| Data Warehouse | Snowflake |
| Transformation | dbt (dbt-snowflake) |
| Orchestration | Apache Airflow |
| Data Quality | Great Expectations + dbt tests |
| Version Control | Git + GitHub |
| Infrastructure | Terraform (coming soon) |
| Containerisation | Docker |

---

## 📁 Project Structure

```
nz-housing-pipeline/
│
├── dags/                        # Airflow DAGs
│   └── housing_pipeline.py      # Main monthly pipeline DAG
│
├── ingestion/                   # Data ingestion scripts
│   ├── stats_nz.py              # Pull Stats NZ CSVs
│   ├── trademe.py               # Trademe Property API
│   └── reinz.py                 # REINZ report loader
│
├── dbt_project/                 # dbt project
│   ├── models/
│   │   ├── staging/             # Raw → cleaned models
│   │   ├── intermediate/        # Business logic
│   │   └── marts/               # Fact & dimension tables
│   ├── tests/                   # dbt data tests
│   └── dbt_project.yml
│
├── tests/                       # Python unit tests
├── docker-compose.yml           # Airflow local setup
├── requirements.txt
└── README.md
```

---