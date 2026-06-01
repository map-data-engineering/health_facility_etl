# =============================================================
# standardise_services.R
# AfyaScope ETL — Generic Facility Services Standardisation
# Works for ANY country — driven entirely by config
#
# Usage:
#   source("R/standardise/standardise_services.R")
#
#   # Tanzania
#   standardise_services("TZA")
#
#   # Malawi
#   standardise_services("MWI")
#
#   # Future country
#   standardise_services("KEN")
# =============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
})

# ── Country config table ─────────────────────────────────────
# Add a new row here for each new country — nothing else changes
SERVICE_CONFIG <- list(
  
  TZA = list(
    country_name   = "Tanzania",
    raw_path       = "data/raw/tanzania/hf_services.csv",
    crosswalk_path = "crosswalks/tz_services_crosswalk.csv",
    # Join keys: columns in raw file that match crosswalk
    join_keys      = c("service_category" = "source_category",
                       "service_detail"   = "source_detail"),
    # How to populate standard source columns
    source_service_id   = NULL,           # not available in TZ
    source_type_id      = NULL,           # not available in TZ
    source_category_id  = "service_category",
    source_service_name = "service_detail",
    date_col            = NULL            # not available in TZ
  ),
  
  MWI = list(
    country_name   = "Malawi",
    raw_path       = "data/raw/malawi/malawi_hf_services_all.csv",
    crosswalk_path = "crosswalks/malawi_services_crosswalk.csv",
    # Join keys: single column join
    join_keys      = c("service_name" = "source_service_name"),
    # How to populate standard source columns
    source_service_id   = "service_id",
    source_type_id      = "service_type_id",
    source_category_id  = "service_category_id",
    source_service_name = "service_name",
    date_col            = "created_date"  # ISO datetime column
  ),

  BWA = list(
    country_name   = "Botswana",
    raw_path       = "data/raw/botswana/botswana_services_raw.csv",
    crosswalk_path = "crosswalks/bwa_services_crosswalk.csv",
    # Raw file produced by scripts/extraction/extract_botswana.R
    # (hits the Botswana MFL public client API). Two-key join
    # because the same service_name can appear under different
    # source categories (clinical_services / infrastructure / staff).
    join_keys      = c("service_category" = "source_category",
                       "service_name"     = "source_service_name"),
    source_service_id   = NULL,
    source_type_id      = NULL,
    source_category_id  = "service_category",
    source_service_name = "service_name",
    date_col            = NULL
  ),

  KEN = list(
    country_name   = "Kenya",
    raw_path       = "data/raw/kenya/kenya_services_raw.csv",
    crosswalk_path = "crosswalks/ken_services_crosswalk.csv",
    # Raw services file is the long-format (facility_code,
    # service_category, service_group, service_detail) extract
    # from the KMHFR. Two-key join because the same detail name
    # can appear under different categories. See
    # scripts/extraction/extract_kenya.R and
    # scripts/extraction/build_kenya_crosswalk.R.
    join_keys      = c("service_category" = "source_category",
                       "service_detail"   = "source_service_name"),
    source_service_id   = NULL,
    source_type_id      = NULL,
    source_category_id  = "service_category",
    source_service_name = "service_detail",
    date_col            = NULL
  )

  # ── Add future countries here ──────────────────────────────
  # OLD KENYA STUB (now replaced by the KEN block above):
  # KEN = list(
  #   country_name   = "Kenya",
  #   raw_path       = "data/raw/kenya/ken_services.csv",
  #   crosswalk_path = "crosswalks/ken_services_crosswalk.csv",
  #   join_keys      = c("service_name" = "source_service_name"),
  #   source_service_id   = NULL,
  #   source_type_id      = NULL,
  #   source_category_id  = NULL,
  #   source_service_name = "service_name",
  #   date_col            = NULL
  # )
)

# ── Main function ────────────────────────────────────────────
standardise_services <- function(
    country_iso,
    registry_path    = "data/processed/uid_registry.csv",
    output_dir       = "data/processed/country_standardized",
    pipeline_version = "2.0"
) {
  
  # Validate country
  cfg <- SERVICE_CONFIG[[country_iso]]
  if (is.null(cfg))
    stop(sprintf(
      "No service config found for '%s'. Add it to SERVICE_CONFIG in standardise_services.R",
      country_iso
    ))
  
  cat(sprintf("\n[%s services] Reading raw file: %s\n",
              country_iso, cfg$raw_path))
  raw <- read_csv(cfg$raw_path, show_col_types = FALSE,
                  locale = locale(encoding = "UTF-8"))
  # Force facility_code to character — the uid_registry stores it as
  # text and readr will otherwise infer purely-numeric codes (KEN)
  # as <double>, which breaks the registry join further down.
  if ("facility_code" %in% names(raw)) {
    raw$facility_code <- as.character(raw$facility_code)
  }
  cat(sprintf("[%s services] Raw rows: %d | Unique facilities: %d\n",
              country_iso, nrow(raw),
              length(unique(raw$facility_code))))
  
  # ── 1. Load and join crosswalk ───────────────────────────────
  cat(sprintf("[%s services] Joining crosswalk...\n", country_iso))
  
  cw <- read_csv(cfg$crosswalk_path, show_col_types = FALSE) %>%
    mutate(across(where(is.character), trimws))
  
  # Trim raw join key columns too
  join_raw_cols <- names(cfg$join_keys)
  raw <- raw %>%
    mutate(across(all_of(join_raw_cols), trimws))
  
  df <- raw %>%
    left_join(
      cw %>% select(
        all_of(unname(cfg$join_keys)),   # crosswalk side of join keys
        service_domain,
        service_group_std = service_group,
        service_name_std  = service_name,
        is_clinical, is_malaria_related,
        include_in_analysis, needs_review
      ),
      by = cfg$join_keys
    )
  
  n_unmatched <- sum(is.na(df$service_domain))
  if (n_unmatched > 0)
    warning(sprintf(
      "[%s services] %d rows unmatched in crosswalk",
      country_iso, n_unmatched
    ))
  
  # Override raw group/name with standard versions
  df <- df %>%
    mutate(
      service_group = service_group_std,
      service_name  = service_name_std
    ) %>%
    select(-service_group_std, -service_name_std)
  
  # ── 2. Join uid_registry for facility_uid ───────────────────
  cat(sprintf("[%s services] Joining facility UIDs...\n", country_iso))
  
  registry <- read_csv(registry_path, show_col_types = FALSE) %>%
    filter(country == country_iso) %>%
    select(facility_code, facility_uid = uid)
  
  if (nrow(registry) == 0)
    warning(sprintf(
      "[%s services] No UIDs in registry — run facilities pipeline first. facility_uid will be NA.",
      country_iso
    ))
  
  df <- df %>% left_join(registry, by = "facility_code")
  
  n_matched <- sum(!is.na(df$facility_uid))
  cat(sprintf("[%s services] facility_uid matched: %d / %d (%.1f%%)\n",
              country_iso, n_matched, nrow(df),
              n_matched / nrow(df) * 100))
  
  # ── 3. Generate service UIDs ─────────────────────────────────
  df <- df %>%
    mutate(uid = sprintf("%s-SVC-%06d", country_iso, row_number()))
  
  # ── 4. Build standard metadata columns ──────────────────────
  get_col <- function(data, col_name) {
    if (is.null(col_name)) return(NA_character_)
    as.character(data[[col_name]])
  }
  
  df <- df %>%
    mutate(
      country_iso         = country_iso,
      country_name        = cfg$country_name,
      source_service_id   = get_col(df, cfg$source_service_id),
      source_type_id      = get_col(df, cfg$source_type_id),
      source_category_id  = get_col(df, cfg$source_category_id),
      source_service_name = get_col(df, cfg$source_service_name),
      extracted_date      = if (!is.null(cfg$date_col)) {
        as.Date(sub("T.*", "",
                    df[[cfg$date_col]]), "%Y-%m-%d")
      } else {
        as.Date(NA)
      },
      source_url          = cfg$raw_path,
      pipeline_version    = pipeline_version
    )
  
  # ── 5. Select final columns ──────────────────────────────────
  out <- df %>%
    select(
      uid, facility_uid, country_iso, country_name,
      service_domain, service_group, service_name,
      source_service_id, source_type_id,
      source_category_id, source_service_name,
      is_clinical, is_malaria_related,
      include_in_analysis, needs_review,
      extracted_date, source_url, pipeline_version,
      facility_code
    )
  
  # ── 6. Save output ───────────────────────────────────────────
  if (!dir.exists(output_dir))
    dir.create(output_dir, recursive = TRUE)
  
  out_file <- tolower(paste0(cfg$country_name,
                             "_services_standardized.csv"))
  out_path <- file.path(output_dir, out_file)
  write_csv(out, out_path)
  cat(sprintf("[%s services] Saved: %s (%d rows)\n",
              country_iso, out_path, nrow(out)))
  
  # ── 7. QA report ────────────────────────────────────────────
  report_path <- file.path(output_dir,
                           tolower(paste0(cfg$country_name, "_services_report.txt")))
  
  dom_tbl  <- sort(table(out$service_domain), decreasing = TRUE)
  dom_lines <- mapply(function(nm, n)
    sprintf("  %-35s %d (%.1f%%)", nm, n, n/nrow(out)*100),
    names(dom_tbl), as.integer(dom_tbl))
  
  unmatched <- out %>%
    filter(is.na(service_domain)) %>%
    distinct(facility_code, source_service_name)
  
  unmatched_lines <- if (nrow(unmatched) == 0) {
    "  None"
  } else {
    head(sprintf("  %s | %s",
                 unmatched$facility_code,
                 unmatched$source_service_name), 50)
  }
  
  writeLines(c(
    sprintf("AfyaScope — %s Services Standardisation Report",
            cfg$country_name),
    sprintf("Generated       : %s", Sys.time()),
    sprintf("Pipeline version: %s", pipeline_version),
    "",
    "--- Row counts ---",
    sprintf("Total records        : %d", nrow(out)),
    sprintf("Unique facilities    : %d",
            length(unique(out$facility_code))),
    sprintf("Matched facility_uid : %d (%.1f%%)",
            n_matched, n_matched/nrow(out)*100),
    sprintf("Unmatched uid        : %d",
            sum(is.na(out$facility_uid))),
    sprintf("include_in_analysis  : %d",
            sum(out$include_in_analysis, na.rm=TRUE)),
    sprintf("needs_review TRUE    : %d",
            sum(out$needs_review, na.rm=TRUE)),
    "",
    "--- Domain distribution ---",
    dom_lines,
    "",
    "--- Unmatched crosswalk rows ---",
    unmatched_lines
  ), report_path)
  
  cat(sprintf("[%s services] Report: %s\n",
              country_iso, report_path))
  invisible(out)
}