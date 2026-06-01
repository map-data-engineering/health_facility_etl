# =============================================================
# run_botswana_pipeline.R — AfyaScope ETL
#
# Botswana single-country runner. Re-fetches raw data from the
# Botswana MFL public client API first, then runs the standard
# country pipeline + global merge + services standardisation.
#
# Usage (from repo root):
#   Rscript pipelines/run_botswana_pipeline.R
# =============================================================
library(yaml)
source("pipelines/run_country_pipeline.R")
source("pipelines/run_global_pipeline.R")

# ── Refresh raw Botswana data from the upstream API ─────────
# Writes data/raw/botswana/Facilities_List_Nov2026.csv and
# data/raw/botswana/botswana_services_raw.csv.
source("scripts/extraction/extract_botswana.R")

countries_config <- read_yaml("config/countries.yml")
schema           <- load_schema("config/schema.yml")

run_country_pipeline(
  country           = "botswana",
  countries_config  = countries_config,
  schema            = schema,
  validate_boundary = TRUE
)

run_global_pipeline(
    countries         = NULL,
    run_countries     = TRUE,
    validate_boundary = TRUE,
    export_data_lake  = TRUE,
    run_services      = TRUE
)
