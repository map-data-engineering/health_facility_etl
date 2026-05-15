# =============================================================
# standardize_columns.R
# AfyaScope ETL — Transformation Layer
#
# Maps raw country columns → standardized schema column names.
# Preserves ALL available fields: admin2/3/4, zone, open_date,
# ownership_detail, catchment_population, urban_rural_strata.
# Adds country name and data_source from config.
# =============================================================

library(dplyr)

standardize_columns <- function(df, country_name, countries_config) {

  cfg <- countries_config[[country_name]]

  # ── helper: safe rename ──────────────────────────────────────
  # Renames raw_col → std_col only if raw_col exists and
  # the config key is not null. Silently skips if unavailable.
  safe_rename <- function(df, raw_col, std_col) {
    if (!is.null(raw_col) && raw_col %in% names(df)) {
      df <- df %>% rename(!!std_col := !!sym(raw_col))
    } else if (!is.null(raw_col) && !(raw_col %in% names(df))) {
      warning(paste0("[", country_name, "] Column '", raw_col,
                     "' not found in raw data for field '", std_col, "'"))
    }
    return(df)
  }

  # ── Required renames (will stop if missing) ──────────────────
  required <- list(
    list(raw = cfg$facility_name_column, std = "facility_name"),
    list(raw = cfg$facility_code_column, std = "facility_code"),
    list(raw = cfg$admin1_column,        std = "admin1"),
    list(raw = cfg$facility_type_column, std = "facility_type"),
    list(raw = cfg$latitude_column,      std = "latitude"),
    list(raw = cfg$longitude_column,     std = "longitude")
  )

  for (field in required) {
    if (is.null(field$raw) || !(field$raw %in% names(df))) {
      stop(paste0("[", country_name, "] Required raw column '", field$raw,
                  "' missing — cannot produce '", field$std, "'"))
    }
    df <- df %>% rename(!!field$std := !!sym(field$raw))
  }

  # ── Ownership detail: rename FIRST to avoid collision with the
  # normalised 'ownership' column created below.  Tanzania, for example,
  # has a raw column called "ownership" (the detail field) which would
  # otherwise be overwritten before it can be preserved.
  df <- safe_rename(df, cfg$ownership_detail_column,      "ownership_detail")

  # ── Ownership: broad category ─────────────────────────────────
  df <- safe_rename(df, cfg$facility_ownership_column, "ownership_raw")

  # Normalise to allowed schema values
  if ("ownership_raw" %in% names(df)) {
    df <- df %>%
      mutate(ownership = case_when(
        grepl("public|government|lga|moh|military|police|prison|parastatal",
              ownership_raw, ignore.case = TRUE)                          ~ "Public",
        grepl("faith|fbo|church|mission|religious|ngo",
              ownership_raw, ignore.case = TRUE)                          ~ "Faith-Based / NGO",
        grepl("private|profit|company|investor|business|clinic",
              ownership_raw, ignore.case = TRUE)                          ~ "Private",
        TRUE                                                              ~ "Other"
      )) %>%
      select(-ownership_raw)
  } else {
    df$ownership <- NA_character_
  }

  # ── Remaining optional field renames ─────────────────────────
  # (ownership_detail already handled above)
  df <- safe_rename(df, cfg$admin2_column,                "admin2")
  df <- safe_rename(df, cfg$admin3_column,                "admin3")
  df <- safe_rename(df, cfg$admin4_column,                "admin4")
  df <- safe_rename(df, cfg$zone_column,                  "zone")
  df <- safe_rename(df, cfg$operation_status_column,      "status")
  df <- safe_rename(df, cfg$open_date_column,             "open_date_raw")
  df <- safe_rename(df, cfg$catchment_population_column,  "catchment_population")
  df <- safe_rename(df, cfg$urban_rural_strata_column,    "urban_rural_strata")

  # ── Add constant fields from config ──────────────────────────
  df$country     <- tools::toTitleCase(country_name)
  df$data_source <- cfg$data_source

  return(df)
}
