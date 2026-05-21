# =============================================================
# run_country_pipeline.R
# AfyaScope ETL — Country Pipeline Orchestrator
#
# Runs the full ETL chain for a single country:
#   1. Extract  : read raw file
#   2. Transform: standardize columns → clean coords → clean dates
#                 → enforce schema types → flag data quality
#   3. Load     : save standardized CSV + QA report
#   4. Log      : every step success/failure
# =============================================================

library(yaml)
library(dplyr)

source("scripts/utils/logger.R")
source("scripts/extraction/read_country_data.R")
source("scripts/transformation/standardize_columns.R") #removed admn 3,4
source("scripts/transformation/clean_coordinates.R")
#source("scripts/transformation/validate_coordinates.R")
source("scripts/transformation/geocode_missing.R")
source("scripts/transformation/clean_dates.R")
source("scripts/transformation/enforce_schema_types.R")
source("scripts/transformation/flag_data_quality.R")
source("scripts/transformation/generate_facility_uid.R")
source("scripts/loading/save_standardized_data.R")

run_country_pipeline <- function(country, countries_config, schema,
                                 validate_boundary      = FALSE,
                                 geocode_missing_coords = FALSE) {

  cat(sprintf("\n══════════════════════════════════════\n"))
  cat(sprintf("  Running pipeline: %s\n", toupper(country)))
  cat(sprintf("══════════════════════════════════════\n"))

  log_message(country, "pipeline", "START")

  tryCatch({

    # 1. EXTRACT
    log_message(country, "extract", "START")
    df <- read_country_data(country, countries_config)
    log_message(country, "extract", "SUCCESS",
                paste("Raw rows:", nrow(df), "| Cols:", ncol(df)))
    cat(sprintf("  [extract]   %d rows, %d columns\n", nrow(df), ncol(df)))

    # 2. STANDARDIZE COLUMNS
    log_message(country, "standardize", "START")
    df <- standardize_columns(df, country, countries_config)
    log_message(country, "standardize", "SUCCESS",
                paste("Cols after standardize:", ncol(df)))

    # 3. CLEAN COORDINATES (range + country bounding box)
    log_message(country, "clean_coords", "START")
    df <- clean_coordinates(df, country)
    n_valid <- sum(df$coordinate_valid == TRUE, na.rm = TRUE)
    n_miss  <- sum(is.na(df$latitude) | is.na(df$longitude))
    log_message(country, "clean_coords", "SUCCESS",
                paste("Valid coords:", n_valid, "| Missing:", n_miss))

    # 4. GEOCODE MISSING COORDINATES (optional — requires httr2 + network)
    if (geocode_missing_coords) {
      log_message(country, "geocode_missing", "START")
      df <- geocode_missing(df, country)
      n_osm <- sum(df$coordinate_source == "OSM", na.rm = TRUE)
      log_message(country, "geocode_missing", "SUCCESS",
                  paste("OSM geocoded:", n_osm))
    }

    # 5. VALIDATE COORDINATES (within-country boundary — optional)
    if (validate_boundary) {
      log_message(country, "validate_coords", "START")
      df <- validate_coordinates(df, tools::toTitleCase(country))
      log_message(country, "validate_coords", "SUCCESS")
    }

    # 6. CLEAN DATES
    log_message(country, "clean_dates", "START")
    df <- clean_dates(df)
    n_dates <- sum(!is.na(df$open_date))
    log_message(country, "clean_dates", "SUCCESS",
                paste("Valid open_date:", n_dates))

    # 7. ENFORCE SCHEMA TYPES
    log_message(country, "schema_types", "START")
    df <- enforce_schema_types(df, schema)
    log_message(country, "schema_types", "SUCCESS")

    # 8. FLAG DATA QUALITY
    log_message(country, "quality_flag", "START")
    df <- flag_data_quality(df)
    log_message(country, "quality_flag", "SUCCESS",
                paste(capture.output(table(df$data_quality_flag)), collapse = " | "))

    # 9. GENERATE STABLE GLOBAL UIDs
    log_message(country, "generate_uid", "START")
    df <- generate_facility_uid(df, country)
    log_message(country, "generate_uid", "SUCCESS",
                paste("UIDs assigned:", sum(!is.na(df$uid))))

    # 10. SAVE
    log_message(country, "save", "START")
    save_standardized_data(df, country)
    log_message(country, "save", "SUCCESS",
                paste("Rows saved:", nrow(df)))

    log_message(country, "pipeline", "SUCCESS",
                paste("Total rows:", nrow(df)))
    cat(sprintf("  ✓ Pipeline complete: %d rows saved\n", nrow(df)))

    return(invisible(df))

  }, error = function(e) {
    log_message(country, "pipeline", "FAILED", e$message)
    cat(sprintf("  ✗ Pipeline FAILED: %s\n", e$message))
    return(invisible(NULL))
  })
}
