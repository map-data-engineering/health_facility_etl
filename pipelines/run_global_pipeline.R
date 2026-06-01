# =============================================================
# run_global_pipeline.R
# AfyaScope ETL — Global Pipeline Orchestrator
#
# Step 1 — Country pipelines (optional, skip with run_countries=FALSE)
# Step 2 — Merge standardized CSVs → global_health_facilities.csv
# Step 3 — Data lake export (Phase 2):
#             Parquet  : facilities_YYYY-MM.parquet
#             DuckDB   : afyascope.duckdb  table=health_facilities
# Step 4 — Services pipeline (optional, skip with run_services=FALSE):
#             Runs R/standardise/standardise_{country}_services.R for any
#             country in run_list that has a matching script.
#             DuckDB   : afyascope.duckdb  table=facility_services (overwrite)
#             To add a new country: create
#               R/standardise/standardise_{country}_services.R
#               crosswalks/{country}_services_crosswalk.csv
#
# Usage (from project root):
#   source("pipelines/run_global_pipeline.R")
#   run_global_pipeline()
#
# Facilities only (skip services):
#   run_global_pipeline(run_services = FALSE)
#
# Merge only (skip country pipelines):
#   run_global_pipeline(run_countries = FALSE)
#
# Skip data lake export:
#   run_global_pipeline(export_data_lake = FALSE)
# =============================================================

library(yaml)
library(dplyr)
library(readr)

source("pipelines/run_country_pipeline.R")
source("scripts/transformation/enforce_schema_types.R")
source("scripts/loading/export_data_lake.R")
source("scripts/utils/logger.R")

run_global_pipeline <- function(
    countries         = NULL,   # NULL = all countries in config
    run_countries     = TRUE,   # FALSE = skip to merge step
    validate_boundary = TRUE,   # uses GADM (cached); set FALSE to skip
    export_data_lake  = TRUE,   # FALSE = skip Parquet + DuckDB export
    run_services      = TRUE    # FALSE = skip service standardisation + DuckDB load
) {

  cat("\n╔══════════════════════════════════════╗\n")
  cat("║   AfyaScope Global Pipeline          ║\n")
  cat("╚══════════════════════════════════════╝\n\n")

  # Load config and schema
  countries_config <- read_yaml("config/countries.yml")
  schema           <- load_schema("config/schema.yml")

  # Determine which countries to run
  all_countries <- names(countries_config)
  run_list      <- if (!is.null(countries)) countries else all_countries

  invalid <- setdiff(run_list, all_countries)
  if (length(invalid) > 0)
    stop("Unknown countries in run list: ", paste(invalid, collapse = ", "))

  # ── Step 1: Country pipelines ─────────────────────────────
  if (run_countries) {
    cat("Step 1/3 — Running country pipelines\n")
    log_message("GLOBAL", "country_pipelines", "START",
                paste("Countries:", paste(run_list, collapse = ", ")))

    results <- list()
    for (country in run_list) {
      results[[country]] <- run_country_pipeline(
        country           = country,
        countries_config  = countries_config,
        schema            = schema,
        validate_boundary = validate_boundary
      )
    }

    failed <- names(Filter(is.null, results))
    if (length(failed) > 0) {
      warning("Pipelines failed for: ", paste(failed, collapse = ", "))
      log_message("GLOBAL", "country_pipelines", "PARTIAL",
                  paste("Failed:", paste(failed, collapse = ", ")))
    } else {
      log_message("GLOBAL", "country_pipelines", "SUCCESS",
                  paste(length(run_list), "countries completed"))
    }
  } else {
    cat("Step 1/3 — Skipping country pipelines (merge only)\n")
  }

  # ── Step 2: Merge all standardized CSVs ──────────────────
  cat("\nStep 2/3 — Merging standardized country files\n")
  log_message("GLOBAL", "merge", "START")

  input_folder  <- "data/processed/country_standardized"
  output_folder <- "data/processed/global_master"

  if (!dir.exists(output_folder)) dir.create(output_folder, recursive = TRUE)

  # Exclude services files (_services_standardized.csv) — those go to
  # facility_services DuckDB table in Step 4, not into health_facilities
  all_csvs  <- list.files(input_folder, pattern = "_standardized\\.csv$",
                          full.names = TRUE, recursive = FALSE)
  csv_files <- all_csvs[!grepl("_services_standardized\\.csv$",
                                basename(all_csvs))]

  if (length(csv_files) == 0)
    stop("No standardized country CSVs found in ", input_folder)

  cat(sprintf("  Found %d country file(s):\n", length(csv_files)))
  for (f in csv_files) cat(sprintf("    - %s\n", basename(f)))

  # Read, add missing schema cols, enforce types
  country_dfs <- lapply(csv_files, function(f) {

    df <- read_csv(f, show_col_types = FALSE,
                   locale = locale(encoding = "UTF-8"))

    # Add any schema columns missing from this file as typed NA
    for (col in names(schema)) {
      if (!col %in% names(df)) {
        type <- schema[[col]]$type
        df[[col]] <- switch(type,
          "string"  = NA_character_,
          "float"   = NA_real_,
          "integer" = NA_integer_,
          "boolean" = NA,
          "date"    = as.Date(NA),
          NA_character_
        )
      }
    }

    enforce_schema_types(df, schema)
  })

  # ── FIX: bind_rows with explicit country source ───────────
  # Extract country name from filename (not from .id row index)
  country_names <- sub("_standardized\\.csv$", "", basename(csv_files))

  global_df <- bind_rows(country_dfs)

  # Use country column from data itself (set during standardize_columns)
  # Verify it is populated correctly
  if (any(is.na(global_df$country))) {
    # Fallback: derive from filename for rows where country is NA
    country_label <- rep(tools::toTitleCase(country_names),
                         times = sapply(country_dfs, nrow))
    global_df$country[is.na(global_df$country)] <-
      country_label[is.na(global_df$country)]
  }

  # Add metadata
  global_df$extraction_date <- Sys.Date()

  # Save
  out_path <- file.path(output_folder, "global_health_facilities.csv")
  write_csv(global_df, out_path)

  # ── Summary ───────────────────────────────────────────────
  cat(sprintf("\n  ✓ Global dataset saved: %s\n", out_path))
  cat(sprintf("  ✓ Total rows: %d | Columns: %d\n", nrow(global_df), ncol(global_df)))
  cat("\n  Country breakdown:\n")
  country_counts <- table(global_df$country)
  for (nm in names(country_counts))
    cat(sprintf("    %-15s %d\n", nm, country_counts[[nm]]))

  cat("\n  Data quality distribution:\n")
  if ("data_quality_flag" %in% names(global_df)) {
    qf <- table(global_df$data_quality_flag)
    for (nm in names(qf))
      cat(sprintf("    %-10s %d (%.1f%%)\n",
                  nm, qf[[nm]], qf[[nm]] / nrow(global_df) * 100))
  }

  log_message("GLOBAL", "merge", "SUCCESS",
              paste("Total rows:", nrow(global_df),
                    "| Countries:", paste(names(country_counts), collapse = ", ")))

  # ── Step 3: Data lake export ──────────────────────────────
  lake_result <- NULL
  if (export_data_lake) {
    cat("\nStep 3/3 — Exporting to data lake (Parquet + DuckDB)\n")
    log_message("GLOBAL", "data_lake", "START")

    lake_result <- export_data_lake(global_df, output_folder)

    if (!is.null(lake_result)) {
      log_message("GLOBAL", "data_lake", "SUCCESS",
                  paste("Parquet:", basename(lake_result$parquet_path),
                        "| DuckDB: afyascope.duckdb"))

      # Print query results as a formatted table
      cat("\n  Query: facilities by country × quality flag\n")
      qr <- lake_result$query_result
      cat(sprintf("  %-15s %-18s %12s %16s\n",
                  "Country", "Quality Flag", "Facilities", "% of Country"))
      cat(sprintf("  %s\n", strrep("-", 65)))
      prev_country <- ""
      for (i in seq_len(nrow(qr))) {
        country_label <- if (qr$country[i] == prev_country) "" else qr$country[i]
        cat(sprintf("  %-15s %-18s %12s %15.1f%%\n",
                    country_label,
                    qr$data_quality_flag[i],
                    format(qr$n_facilities[i], big.mark = ","),
                    qr$pct_of_country[i]))
        prev_country <- qr$country[i]
      }
    } else {
      log_message("GLOBAL", "data_lake", "SKIPPED", "arrow/duckdb unavailable")
    }
  } else {
    cat("\nStep 3/3 — Skipping data lake export\n")
  }

  # ── Step 4: Services pipeline ─────────────────────────────────────────────
  svc_result <- NULL
  if (run_services) {
    cat("\nStep 4 — Services standardisation + DuckDB load\n")
    log_message("GLOBAL", "services", "START")

    svc_cols <- c("uid","facility_uid","country_iso","country_name",
                  "service_domain","service_group","service_name",
                  "source_service_id","source_type_id","source_category_id",
                  "source_service_name","is_clinical","is_malaria_related",
                  "include_in_analysis","extracted_date","source_url",
                  "pipeline_version","facility_code","needs_review")

    # svc_dfs <- list()
    # for (country in run_list) {
    #   svc_script <- file.path("R", "standardise",
    #                           sprintf("standardise_%s_services.R", country))
    #   if (!file.exists(svc_script)) {
    #     cat(sprintf("  [services] %s — no script found, skipping\n", country))
    #     next
    #   }
    #   source(svc_script)
    #   fn_name <- sprintf("standardise_%s_services", country)
    #   df <- tryCatch(
    #     do.call(fn_name, list()),
    #     error = function(e) {
    #       log_message("GLOBAL", paste("services", country), "FAILED", e$message)
    #       cat(sprintf("  [services] %s FAILED: %s\n", country, e$message))
    #       NULL
    #     }
    #   )
    #   if (!is.null(df)) {
    #     svc_dfs[[country]] <- df
    #     cat(sprintf("  [services] %s: %d rows\n", country, nrow(df)))
    #   }
    # }
    
    # ── load unified function ──────────────────────────────
    source("R/standardise/standardise_services.R")
    
    # Countries with service data — add new ISO codes here
    service_countries <- c("TZA", "MWI", "BWA")  # ← only change needed
    
    svc_dfs <- list()
    for (iso in service_countries) {
      
      # Skip if not in current run_list
      if (!iso %in% run_list &
          !is.null(countries)) next
      
      df <- tryCatch(
        standardise_services(iso),
        error = function(e) {
          log_message("GLOBAL", paste("services", iso),
                      "FAILED", e$message)
          cat(sprintf("  [services] %s FAILED: %s\n",
                      iso, e$message))
          NULL
        }
      )
      if (!is.null(df)) {
        svc_dfs[[iso]] <- df
        cat(sprintf("  [services] %s: %d rows\n",
                    iso, nrow(df)))
      }
    }

    #**
    if (length(svc_dfs) > 0) {
      if (!requireNamespace("duckdb", quietly = TRUE) ||
          !requireNamespace("DBI",    quietly = TRUE)) {
        message("  [services] duckdb/DBI not installed — skipping DuckDB load")
      } else {
        combined_svc <- dplyr::bind_rows(lapply(svc_dfs, function(df) {
          df_sel <- df[, intersect(svc_cols, names(df)), drop = FALSE]
          dplyr::mutate(df_sel,
            dplyr::across(c(source_service_id, source_type_id, source_category_id),
                          as.character))
        }))

        duckdb_path <- file.path(output_folder, "afyascope.duckdb")
        con_svc <- DBI::dbConnect(duckdb::duckdb(), duckdb_path, read_only = FALSE)
        on.exit(DBI::dbDisconnect(con_svc, shutdown = TRUE), add = TRUE)
        DBI::dbWriteTable(con_svc, "facility_services", combined_svc, overwrite = TRUE)
        n_svc <- DBI::dbGetQuery(con_svc,
                                 "SELECT COUNT(*) AS n FROM facility_services")$n

        cat(sprintf("  ✓ facility_services: %d rows in DuckDB\n", n_svc))
        log_message("GLOBAL", "services", "SUCCESS",
                    paste("facility_services rows:", n_svc,
                          "| countries:", paste(names(svc_dfs), collapse = ", ")))
        svc_result <- list(n_rows = n_svc, countries = names(svc_dfs))
      }
    } else {
      cat("  [services] No service scripts found for any country in run_list\n")
      log_message("GLOBAL", "services", "SKIPPED", "no service scripts found")
    }
  } else {
    cat("\nStep 4 — Skipping services pipeline\n")
  }

  cat("\n╔══════════════════════════════════════╗\n")
  cat("║   Global pipeline complete ✓         ║\n")
  cat("╚══════════════════════════════════════╝\n\n")

  return(invisible(list(data = global_df, lake = lake_result,
                        services = svc_result)))
}
