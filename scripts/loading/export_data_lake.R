# =============================================================
# export_data_lake.R
# AfyaScope ETL — Loading Layer (Phase 2)
#
# Converts the merged global data frame to two formats:
#
#   Parquet  →  data/processed/global_master/facilities_YYYY-MM.parquet
#               Columnar, compressed (~10x smaller than CSV).
#               Named by month so each run is naturally versioned.
#               Prior months are kept; current month is overwritten.
#
#   DuckDB   →  data/processed/global_master/afyascope.duckdb
#               Persistent analytical database.
#               Table 'health_facilities' is replaced on every run
#               (always mirrors the latest Parquet snapshot).
#               Query with any DuckDB-compatible client.
#
# Returns a named list: parquet_path, duckdb_path, query_result.
# Returns NULL silently if arrow/duckdb are not installed.
#
# Requires: arrow, duckdb, DBI
# =============================================================

library(DBI)

export_data_lake <- function(df,
                             output_folder = "data/processed/global_master") {

  if (!requireNamespace("arrow",  quietly = TRUE) ||
      !requireNamespace("duckdb", quietly = TRUE)) {
    message("  [data_lake] arrow/duckdb not available — skipping")
    message("  [data_lake] Install with: install.packages(c('arrow','duckdb'))")
    return(invisible(NULL))
  }

  if (!dir.exists(output_folder))
    dir.create(output_folder, recursive = TRUE)

  # ── 1. Write Parquet ────────────────────────────────────────
  parquet_name <- paste0("facilities_", format(Sys.Date(), "%Y-%m"), ".parquet")
  parquet_path <- file.path(output_folder, parquet_name)

  arrow::write_parquet(df, parquet_path)

  csv_size_mb     <- file.size(file.path(output_folder,
                                          "global_health_facilities.csv")) / 1e6
  parquet_size_mb <- file.size(parquet_path) / 1e6
  compression_pct <- round((1 - parquet_size_mb / csv_size_mb) * 100, 1)

  message(sprintf(
    "  [data_lake] Parquet: %s  (%.1f MB vs CSV %.1f MB — %.1f%% smaller)",
    parquet_name, parquet_size_mb, csv_size_mb, compression_pct
  ))

  # ── 2. Load into DuckDB ─────────────────────────────────────
  duckdb_path <- file.path(output_folder, "afyascope.duckdb")
  # DuckDB requires forward slashes even on Windows
  parquet_abs <- gsub("\\\\", "/", normalizePath(parquet_path, mustWork = TRUE))

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = duckdb_path, read_only = FALSE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, sprintf(
    "CREATE OR REPLACE TABLE health_facilities AS
     SELECT * FROM read_parquet('%s')",
    parquet_abs
  ))

  n_rows <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM health_facilities")$n
  message(sprintf(
    "  [data_lake] DuckDB '%s' → table 'health_facilities' (%d rows)",
    basename(duckdb_path), n_rows
  ))

  # ── 3. Sample query: facilities by country × quality flag ───
  query_sql <- "
    SELECT
      country,
      COALESCE(data_quality_flag, '(null)') AS data_quality_flag,
      COUNT(*)                               AS n_facilities,
      ROUND(
        COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (PARTITION BY country),
        1
      )                                      AS pct_of_country
    FROM  health_facilities
    GROUP BY country, data_quality_flag
    ORDER BY country, data_quality_flag
  "
  query_result <- DBI::dbGetQuery(con, query_sql)

  message("  [data_lake] Sample query complete")

  return(list(
    parquet_path = parquet_path,
    duckdb_path  = duckdb_path,
    query_result = query_result
  ))
}
