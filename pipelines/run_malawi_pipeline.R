# =============================================================
# run_malawi_pipeline.R — AfyaScope ETL
# =============================================================
library(yaml)
source("pipelines/run_country_pipeline.R")

countries_config <- read_yaml("config/countries.yml")
schema           <- load_schema("config/schema.yml")

run_country_pipeline(
  country           = "malawi",
  countries_config  = countries_config,
  schema            = schema,
  validate_boundary = TRUE
)

run_global_pipeline (
    countries         = NULL,   # NULL = all countries in config
    run_countries     = TRUE,   # FALSE = skip to merge step
    validate_boundary = TRUE,   # uses GADM (cached); set FALSE to skip
    export_data_lake  = TRUE,   # FALSE = skip Parquet + DuckDB export
    run_services      = TRUE    # FALSE = skip service standardisation + DuckDB load
) 