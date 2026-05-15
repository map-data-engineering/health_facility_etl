# AfyaScope — Health Facility ETL Pipeline

> **v2.0** — Sub-Saharan Africa Malaria-Endemic Countries

A standardized, reproducible ETL framework to collect, clean, harmonize,
and merge health facility data extracted directly from Ministries of Health
across Sub-Saharan Africa. The output powers the **AfyaScope** health
facility intelligence portal.

---

## Countries Covered

| Country   | Source                                    | Records  | Status   |
|-----------|-------------------------------------------|----------|----------|
| Tanzania  | MoH Tanzania Health Facility Registry    | ~13,075  | ✅ Active |
| Uganda    | Uganda MoH Master Facility List (MFL)    | ~8,508   | ✅ Active |
| Zambia    | Zambia MoH Master Facility List (MFL)    | ~3,731   | ✅ Active |
| Malawi    | Malawi Health Facility Registry (MHFR)   | ~1,929   | ✅ Active |
| Nigeria   | GRID3 / NHFR                             | ~51,022  | ✅ Active |
| Botswana  | MoH Botswana Facilities List             | TBD      | 🔄 Pending raw file |

---

## Project Structure

```
health_facility_etl/
├── config/
│   ├── countries.yml          # Country configs & column mappings
│   ├── schema.yml             # Master standardized schema (v2)
│   └── paths.yml              # Directory paths
│
├── data/
│   ├── raw/                   # Original source files (one folder per country)
│   └── processed/
│       ├── country_standardized/   # Per-country clean CSVs + QA reports
│       └── global_master/          # Merged global dataset
│
├── pipelines/
│   ├── run_country_pipeline.R      # Core orchestrator (all steps)
│   ├── run_global_pipeline.R       # Runs all countries → merge
│   ├── run_tanzania_pipeline.R     # Convenience: Tanzania only
│   ├── run_uganda_pipeline.R
│   ├── run_zambia_pipeline.R
│   ├── run_malawi_pipeline.R
│   └── run_nigeria_pipeline.R
│
├── scripts/
│   ├── extraction/
│   │   └── read_country_data.R     # CSV/XLSX reader with encoding handling
│   ├── transformation/
│   │   ├── standardize_columns.R   # Maps raw cols → schema; preserves all admin levels
│   │   ├── clean_coordinates.R     # Coerces lat/lon; flags invalid/missing
│   │   ├── validate_coordinates.R  # Within-country boundary check (requires sf)
│   │   ├── clean_dates.R           # Parses open dates; nulls epoch-zero & implausible
│   │   ├── enforce_schema_types.R  # Coerces all cols to schema types; adds missing as NA
│   │   └── flag_data_quality.R     # Assigns high/medium/low/unknown per record
│   └── loading/
│       └── save_standardized_data.R  # Saves CSV + per-country QA report
│
├── logs/
│   └── pipeline_log.csv        # Timestamped log of every pipeline step
│
└── docs/
    └── country_notes/          # Country-specific extraction notes
```

---

## ETL Pipeline Steps

```
RAW FILE
   │
   ▼
read_country_data()        — reads CSV/XLSX; handles latin1 encoding
   │
   ▼
standardize_columns()      — maps raw col names → standard names
                             preserves: admin1–4, zone, open_date_raw,
                             ownership_detail, catchment_population,
                             urban_rural_strata; normalises ownership
                             to 4 categories (Public/Private/FBO/Other)
   │
   ▼
clean_coordinates()        — coerces to numeric; flags missing/invalid/
                             swapped lat-lon as coordinate_valid = FALSE
   │
   ▼
validate_coordinates()     — (optional) within-country boundary check
                             requires sf + rnaturalearth
   │
   ▼
clean_dates()              — parses Stata %td, ISO 8601 formats;
                             nulls epoch-zero 1970-01-01 sentinel values;
                             produces open_date (Date) + open_year (int)
   │
   ▼
enforce_schema_types()     — coerces all schema columns to correct types;
                             adds missing schema cols as typed NA
   │
   ▼
flag_data_quality()        — assigns high/medium/low/unknown per record
                             based on coordinate validity + field completeness
   │
   ▼
save_standardized_data()   — writes CSV + QA report to processed/
```

---

## Standardized Schema (v2) — Key Fields

| Field                  | Type    | Description                                      |
|------------------------|---------|--------------------------------------------------|
| `facility_code`        | string  | National facility ID                             |
| `facility_name`        | string  | Official name                                    |
| `country`              | string  | Country name (Title Case)                        |
| `admin1`               | string  | Region / State / Province                        |
| `admin2`               | string  | District / LGA                                   |
| `admin3`               | string  | Ward / Sub-county                                |
| `admin4`               | string  | Village / Parish                                 |
| `zone`                 | string  | Zonal grouping (Tanzania, Zambia)                |
| `latitude`             | float   | Decimal degrees                                  |
| `longitude`            | float   | Decimal degrees                                  |
| `coordinate_valid`     | boolean | Passes range + boundary check                    |
| `facility_type`        | string  | Facility type from source                        |
| `ownership`            | string  | Public / Private / Faith-Based NGO / Other       |
| `ownership_detail`     | string  | Detailed ownership from source                   |
| `status`               | string  | Operating status                                 |
| `open_date`            | date    | Registration/opening date (NA if unknown)        |
| `open_year`            | integer | Year opened (cleaned)                            |
| `catchment_population` | integer | Estimated catchment population                   |
| `urban_rural_strata`   | string  | Urban/rural classification                       |
| `inpatient`            | boolean | Service available (to be joined from service data)|
| `outpatient`           | boolean | Service available                                |
| `maternity`            | boolean | Service available                                |
| `emergency`            | boolean | Service available                                |
| `laboratory`           | boolean | Service available                                |
| `malaria_services`     | boolean | Service available                                |
| `data_source`          | string  | Full source label                                |
| `data_quality_flag`    | string  | high / medium / low / unknown                    |

---

## How to Run

### Tanzania only (PoC)
```r
source("pipelines/run_tanzania_pipeline.R")
```

### Any single country
```r
library(yaml)
source("pipelines/run_country_pipeline.R")

countries_config <- read_yaml("config/countries.yml")
schema           <- load_schema("config/schema.yml")

run_country_pipeline("uganda", countries_config, schema)
```

### Full global pipeline
```r
source("pipelines/run_global_pipeline.R")
run_global_pipeline()
```

### Merge only (skip re-running country pipelines)
```r
run_global_pipeline(run_countries = FALSE)
```

### Specific subset of countries
```r
run_global_pipeline(countries = c("tanzania", "uganda", "zambia"))
```

---

## Adding a New Country

1. Place raw file in `data/raw/<country>/`
2. Add entry to `config/countries.yml` with column mappings
3. Run `run_global_pipeline(countries = c("<new_country>"))`

No other code changes needed.

---

## Plugging in Service Data

Service columns (`inpatient`, `outpatient`, `maternity`, `emergency`,
`laboratory`, `malaria_services`) are schema-defined but populated via
a separate join — they are not in the MoH registry files.

To join service data:
```r
library(dplyr)
services <- read_csv("data/raw/<country>/services.csv")  # your service file

df_standardized <- df_standardized %>%
  left_join(services, by = "facility_code")
```

The schema columns will then be populated at `enforce_schema_types()` time.

---

## Known Issues & Notes

- **Nigeria source**: GRID3 v2.0 is a second-party aggregator (not direct MoH).
  Data provenance is flagged in `data_source` column.
- **Tanzania open dates**: ~2,066 records had `01jan1970` (Unix epoch zero =
  missing date). These are cleaned to `NA` by `clean_dates.R`.
- **Missing coordinates**: ~7.7% of Tanzania facilities lack lat/lon.
  Records are retained with `coordinate_valid = NA` and `data_quality_flag = "low"`.
  Future work: geocode by ward centroid using Tanzania admin boundaries.
- **Botswana**: raw file not yet received. Config entry exists; pipeline will
  error gracefully until file is placed in `data/raw/botswana/`.
