# =============================================================
# run_uganda_pipeline.R — AfyaScope ETL
# =============================================================
library(yaml)
source("pipelines/run_country_pipeline.R")

countries_config <- read_yaml("config/countries.yml")
schema           <- load_schema("config/schema.yml")

run_country_pipeline(
  country           = "uganda",
  countries_config  = countries_config,
  schema            = schema,
  validate_boundary = TRUE
)
