# 🏠 NZ Housing Market Analytics Pipeline

An end-to-end data engineering project that ingests, transforms, and analyses New Zealand housing market data to uncover trends in property prices, affordability, and regional demand.

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
- Deliver analytical outputs: affordability index, supply vs demand, and rate sensitivity analysis

---

## 🗂️ Data Sources

| # | Source | Data | Format | Frequency |
|---|---|---|---|---|
| 1 | Stats NZ | Building consents by dwelling type, HUD rental price index by region | XLSX | Monthly |
| 2 | Reserve Bank NZ (RBNZ) | Mortgage interest rates — special (B21), standard (B20), weighted avg (B30) | XLSX | Monthly |
| 3 | Trademe Property API | Residential listings, asking prices, days on market by region | API (JSON) | Weekly |

> **Note:** All 3 sources are aggregated to region + month grain before joining. They do not track individual properties — analysis is macro-level.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    DATA SOURCES                         │
│     Stats NZ XLSX    │   RBNZ XLSX    │  Trademe API    │
└────────┬─────────────────────┬───────────────┬──────────┘
         │                     │               │
         ▼                     ▼               ▼
┌─────────────────────────────────────────────────────────┐
│              INGESTION LAYER (Python)                   │
│         stats_nz.py  │  rbnz.py  │  trademe.py          │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│              STORAGE LAYER (AWS S3)                     │
│              Raw files land in S3 Data Lake             │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│            DATA WAREHOUSE (Snowflake)                   │
│   RAW schema → STAGING schema → MARTS schema            │
│      (Bronze)       (Silver)         (Gold)             │
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
                         └──────┬──────┘
                                │
          ┌─────────────────────┼──────────────────────┐
          │                     │                      │
          ▼                     ▼                      ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│   FACT_SUPPLY    │  │  FACT_MORTGAGE   │  │  FACT_LISTINGS   │
│──────────────────│  │──────────────────│  │──────────────────│
│ date_id (FK)     │  │ date_id (FK)     │  │ date_id (FK)     │
│ dwelling_type    │  │ term             │  │ region           │
│ consents_issued  │  │ rate_pct         │  │ avg_asking_price │
│ floor_area       │  │ series           │  │ num_listings     │
│ value            │  └──────────────────┘  │ days_on_market   │
└──────────────────┘                        └──────────────────┘
          │                                          │
          └──────────────┬───────────────────────────┘
                         │
                         ▼
              ┌─────────────────────┐
              │  MART_AFFORDABILITY │
              │─────────────────────│
              │ date_id             │
              │ region              │
              │ avg_asking_price    │
              │ one_year_rate       │
              │ consents_issued     │
              │ affordability_index │
              │ supply_gap          │
              └─────────────────────┘
```

---

## 🔗 How The 3 Sources Integrate

| Source | Role | Grain |
|---|---|---|
| Stats NZ | Supply side — how many homes are being built | National + monthly |
| RBNZ | Cost side — how much it costs to borrow | National + monthly |
| Trademe | Demand side — what's listed and at what price | Region + weekly |

**3 key analytical outputs:**
- **Affordability Index** — avg listing price vs current mortgage rate
- **Supply Gap** — consents issued vs active listings per region
- **Rate Sensitivity** — do listing volumes/prices move when rates change

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
│   ├── stats_nz.py              # Stats NZ building consents + HUD rental index
│   ├── rbnz.py                  # RBNZ mortgage rates (B20, B21, B30)
│   └── trademe.py               # Trademe Property API listings
│
├── dbt_project/                 # dbt project
│   ├── models/
│   │   ├── staging/             # Raw → cleaned models
│   │   ├── intermediate/        # Business logic
│   │   └── marts/               # Fact & dimension tables
│   ├── tests/                   # dbt data tests
│   └── dbt_project.yml
│
├── data/                        # Local raw data (gitignored)
│   └── raw/
│       ├── stats_nz/
│       └── rbnz/
│
├── tests/                       # Python unit tests
├── docker-compose.yml           # Airflow local setup
├── .env                         # API keys and credentials (gitignored)
├── .gitignore
├── requirements.txt
└── README.md
```

## 📈 Analytical Outputs

- **Affordability Index** — average listing price vs current 1-year mortgage rate
- **Supply Gap** — building consents issued vs active listings by region
- **Rate Sensitivity** — how listing volumes and prices respond to rate changes
- **Rental Trends** — annual rental price change by region over time

---

## 🗺️ Roadmap

- [x] Project setup and architecture design
- [x] Stats NZ ingestion (building consents + HUD rental index)
- [x] RBNZ ingestion (mortgage rates B20, B21, B30)
- [ ] Trademe API ingestion (pending API approval)
- [ ] Snowflake schema and table setup
- [ ] dbt staging models
- [ ] dbt mart models (fact + dims + affordability mart)
- [ ] dbt data quality tests
- [ ] Airflow DAG
- [ ] Dashboard / reporting layer
- [ ] Terraform infrastructure-as-code
- [ ] CI/CD with GitHub Actions

---

## 🙋 About

Built as a portfolio project to demonstrate data engineering skills relevant to the New Zealand job market. All data sources are publicly available or use free-tier APIs.

---

## 📄 License

MIT License — feel free to fork and adapt for your own learning.