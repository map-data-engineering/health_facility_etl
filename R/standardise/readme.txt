source("R/standardise/standardise_services.R")

# Run for existing countries
standardise_services("TZA")
standardise_services("MWI")

# Run for a new country (after adding config + crosswalk)
standardise_services("KEN")
standardise_services("MOZ")

#To add a new country — only 2 steps
1. Add a config block to SERVICE_CONFIG in the script - in standardize_services script
   (10 lines of config)

2. Build the crosswalk CSV for that country
   (use global_services_crosswalk.csv as starting point)

That's it — no new R script needed ever again.

RAW FILE
  + CROSSWALK (translation)
  + UID REGISTRY (facility linking)
  ↓
standardise_services("MWI")
  ↓
malawi_services_standardized.csv


***********************************************************************
Step 4 only produces per-country CSVs:
data/processed/country_standardized/
├── tanzania_services_standardized.csv    ← Step 4 output
├── malawi_services_standardized.csv      ← Step 4 output
├── kenya_services_standardized.csv       ← Step 4 output (future)
│
├── tanzania_standardized.csv             ← facilities (Step 2)
├── malawi_standardized.csv               ← facilities (Step 2)
Step 4 does NOT load to DuckDB — that's Step 5.

The full flow
Step 2 — standardise_services("TZA")
         standardise_services("MWI")
         → per-country CSVs saved to country_standardized/

Step 5 — load_services_to_duckdb.R
         reads ALL *_services_standardized.csv files
         merges them
         loads into facility_services table in DuckDB
         → this is your global services dataset

So the global dataset lives in DuckDB
No global_hf_services.CSV exists on disk
                    ↓
Instead it lives inside afyascope.duckdb
as the facility_services table

To get it as a CSV if ever needed:
dbGetQuery(con, "SELECT * FROM facility_services")
write.csv(...)

The parallel with facilities
Facilities pipeline:          Services pipeline:
────────────────────          ─────────────────
Step 2: per-country CSVs  =   Step 4: per-country CSVs
Step 3: merge → DuckDB    =   Step 5: merge → DuckDB

health_facilities table   =   facility_services table