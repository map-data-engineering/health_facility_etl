# HealthScape ETL Pipeline
> Health facility intelligence for Sub-Saharan Africa — feeding the HealthScape portal

---

## Overview

The HealthScape ETL Pipeline is a standardised, reproducible R framework that extracts, transforms, and loads health facility data from multiple Sub-Saharan African countries into a unified data lake. It produces two linked datasets — facility demographics and facility services — stored in a DuckDB database and consumed by the HealthScape portal.

**Current coverage:** Tanzania, Uganda, Zambia, Nigeria, Malawi
**Data lake size:** ~78,000 facilities | ~308,000 service records

---

## What the pipeline produces

```
healthscape.duckdb
├── health_facilities     — who each facility is
│   78,265 rows             name, location, type, ownership, admin hierarchy
│
└── facility_services     — what each facility offers
    307,954 rows            standardised service names across all countries
```

These two tables are joined via `uid ↔ facility_uid` and queried directly by HealthScape.

---

## Project Structure

```
health_facility_etl/
│
├── config/
│   ├── countries.yml             — column mappings per country
│   ├── schema.yml                — master facility schema (30 fields)
│   └── services_schema.yaml      — master services schema (19 fields)
│
├── crosswalks/
│   ├── tz_services_crosswalk.csv      — Tanzania service name → standard
│   ├── malawi_services_crosswalk.csv  — Malawi service name → standard
│   └── global_services_crosswalk.csv  — master crosswalk (all countries)
│
├── data/
│   ├── raw/
│   │   ├── tanzania/             — raw source files per country
│   │   ├── malawi/
│   │   ├── nigeria/
│   │   ├── uganda/
│   │   └── zambia/
│   │
│   └── processed/
│       ├── country_standardized/ — per-country standardised CSVs
│       │   ├── tanzania_standardized.csv
│       │   ├── tanzania_services_standardized.csv
│       │   └── ...
│       │
│       ├── global_master/
│       │   ├── global_health_facilities.csv   — merged facility demographics
│       │   ├── facilities_YYYY-MM.parquet     — parquet snapshot
│       │   └── healthscape.duckdb               — THE DATA LAKE
│       │
│       └── uid_registry.csv      — global unique ID registry
│
├── pipelines/
│   ├── run_global_pipeline.R     — MASTER ORCHESTRATOR (run this)
│   ├── run_country_pipeline.R    — generic country engine
│   ├── run_tanzania_pipeline.R   — Tanzania only
│   ├── run_malawi_pipeline.R     — Malawi only
│   ├── run_nigeria_pipeline.R    — Nigeria only
│   ├── run_uganda_pipeline.R     — Uganda only
│   └── run_zambia_pipeline.R     — Zambia only
│
├── R/
│   ├── standardise/
│   │   └── standardise_services.R  — unified services standardisation
│   └── ...
│
├── scripts/
│   ├── transformation/
│   ├── loading/
│   └── utils/
│
├── logs/
│   └── pipeline_log.csv
│
└── docs/
    └── session_notes/            — development session summaries
```

---

## ETL Workflow

The pipeline runs in 4 steps, all orchestrated by `run_global_pipeline.R`:

```
Step 1 — Country facility pipelines
          Raw CSV → standardised schema → per-country CSV
          Generates UIDs, validates coordinates, flags quality

Step 2 — Global merge
          All country CSVs → global_health_facilities.csv
          Schema enforced, missing columns filled as NA

Step 3 — Data lake export
          global_health_facilities.csv → healthscape.duckdb (health_facilities table)
          Also writes a dated Parquet snapshot

Step 4 — Services pipeline
          Raw service files → crosswalk translation → standardised CSV
          All countries merged → healthscape.duckdb (facility_services table)
```

### Facility schema (Step 1-3)

Each facility record contains 30 standardised fields including unique identifier (`uid`), facility name, type, ownership, operational status, administrative hierarchy (admin1–admin4), coordinates, coordinate validity flag, and data quality flag.

Field selection follows WHO guidance on Master Facility Lists (WHO, 2017).

### Services schema (Step 4)

Each service record contains 19 fields including a 3-level hierarchy:

```
service_domain   — 7 standard domains
                   (Clinical Services, Malaria Services,
                    HIV/AIDS Services, Diagnostic Services,
                    Reproductive & Maternal Health,
                    Community & Preventive Health,
                    Support Services)

service_group    — mid-level grouping (~25 groups)

service_name     — lowest subcategory (primary analysis field)
```

Analysis flags — `is_clinical`, `is_malaria_related`, `include_in_analysis` — allow filtering out commodities, equipment, and training records from service availability analysis.

---

## How to Run

**Always set working directory first:**
```r
setwd("path/to/health_facility_etl")
```

### Run everything (demographics + services, all countries)
```r
source("pipelines/run_global_pipeline.R")
run_global_pipeline()
```

### Run services pipeline only (skip re-running demographics)
```r
source("pipelines/run_global_pipeline.R")
run_global_pipeline(
  run_countries    = FALSE,
  export_data_lake = FALSE,
  run_services     = TRUE
)
```

### Run a single country only
```r
source("pipelines/run_tanzania_pipeline.R")
```

### Run specific countries in the global pipeline
```r
source("pipelines/run_global_pipeline.R")
run_global_pipeline(countries = c("tanzania", "malawi"))
```

---

## Querying the Data Lake

```r
library(DBI)
library(duckdb)

con <- dbConnect(duckdb(),
  "data/processed/global_master/healthscape.duckdb",
  read_only = TRUE)

# Facility counts by country
dbGetQuery(con, "
  SELECT country, COUNT(*) as facilities
  FROM health_facilities
  GROUP BY country
")

# Malaria service coverage
dbGetQuery(con, "
  SELECT hf.country, hf.facility_name,
         hf.latitude, hf.longitude,
         s.service_name
  FROM health_facilities hf
  JOIN facility_services s ON hf.uid = s.facility_uid
  WHERE s.is_malaria_related = TRUE
    AND s.include_in_analysis = TRUE
")

dbDisconnect(con, shutdown = TRUE)
```

---

## Adding a New Country

### Facility demographics
1. Place raw CSV in `data/raw/{country}/`
2. Add column mapping to `config/countries.yml`
3. Run `run_global_pipeline()` — picked up automatically

### Service data
1. Extract service data from country HFR portal
   (see `docs/session_notes/` for API reverse engineering method)
2. Build crosswalk: `crosswalks/{iso}_services_crosswalk.csv`
   Use `crosswalks/global_services_crosswalk.csv` as reference —
   only add service names not already in the master crosswalk
3. Add country config block to `R/standardise/standardise_services.R`
   under `SERVICE_CONFIG`
4. Add ISO code to `service_countries` vector in Step 4 of
   `pipelines/run_global_pipeline.R`
5. Run `run_global_pipeline(run_countries = FALSE, run_services = TRUE)`

---

## UID System

Every facility receives a stable global unique ID:

```
Format:   {ISO3}-{6-digit zero-padded}
Example:  TZA-000142, MWI-001234, NGA-051022

Registered in: data/processed/uid_registry.csv
Keyed on:      (country, facility_code)
```

Service records use a parallel format:
```
Format:   {ISO3}-SVC-{6-digit zero-padded}
Example:  TZA-SVC-000001, MWI-SVC-099013
```

UIDs are stable across pipeline runs — a facility keeps the same UID even if the pipeline is re-run.

---

## Crosswalks

Crosswalks are translation tables that map raw country-specific service names to the standard 3-level hierarchy. They solve the core cross-country comparability problem:

```
"mRDT - Rapid Diagnostic Tests"  (Tanzania)  ─┐
"Malaria rapid test"              (Malawi)     ─┼─► Malaria Services >
"MRDT available"                  (Malawi)     ─┘    Malaria Diagnosis (mRDT) >
                                                      Malaria RDT
```

The `global_services_crosswalk.csv` grows with each new country added and becomes the master reference for all SSA service terminology.

---

## Logging

Every pipeline step is logged in `logs/pipeline_log.csv`:

| Field | Description |
|---|---|
| timestamp | When the step ran |
| country | Which country |
| step | Pipeline step name |
| status | START / SUCCESS / FAILED / SKIPPED |
| message | Row counts, file paths, error messages |

---

## Known Data Issues

| Issue | Countries | Status |
|---|---|---|
| 190 facility codes in Malawi services not in HF registry | Malawi | Source gap — facility_uid = NULL for 1,968 rows |
| `iso` column is NA for Malawi in health_facilities | Malawi | Use `country` column for joins |
| Tanzania coordinates missing for some facilities | Tanzania | Flagged as `data_quality_flag = low` |

---

## Dependencies

```r
install.packages(c(
  "dplyr", "readr", "yaml", "DBI",
  "duckdb", "arrow", "sf", "lubridate"
))
```

---

## References

World Health Organization. *Master facility list resource package: guidance for countries wanting to strengthen their master facility list.* Geneva: WHO; 2017. Available from: https://www.who.int/publications/i/item/-9789241513302
