# =============================================================
# standardise_tanzania_services.R
# AfyaScope ETL — Tanzania Facility Services Standardisation
#
# Reads:  data/raw/tanzania/hf_services.csv
#         crosswalks/tz_services_crosswalk.csv
#         data/processed/uid_registry.csv
#
# Writes: data/processed/country_standardized/
#           tanzania_services_standardized.csv
#           tanzania_services_report.txt
#
# Usage (from project root):
#   source("R/standardise/standardise_tanzania_services.R")
#   standardise_tanzania_services()
# =============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
})

standardise_tanzania_services <- function(
    raw_path       = "data/raw/tanzania/hf_services.csv",
    crosswalk_path = "crosswalks/tz_services_crosswalk.csv",
    registry_path  = "data/processed/uid_registry.csv",
    output_dir     = "data/processed/country_standardized",
    pipeline_version = "2.0"
) {

  country_iso  <- "TZA"
  country_name <- "Tanzania"

  cat(sprintf("\n[TZ services] Reading raw file: %s\n", raw_path))
  raw <- read_csv(raw_path, show_col_types = FALSE,
                  locale = locale(encoding = "UTF-8"))
  cat(sprintf("[TZ services] Raw rows: %d | Unique facilities: %d\n",
              nrow(raw), length(unique(raw$facility_code))))

  # ── 1. Join crosswalk ────────────────────────────────────────
  cat("[TZ services] Joining crosswalk...\n")
  cw <- read_csv(crosswalk_path, show_col_types = FALSE)

  # Join on (source_category, source_detail). A 2-key join is safe because
  # each TZ service_category has exactly one service_group, so the (category,
  # detail) pair is unique in the crosswalk. The crosswalk service_group column
  # is renamed std_group to avoid collision with raw$service_group (dplyr would
  # otherwise produce service_group.x / service_group.y).
  df <- raw %>%
    left_join(
      cw %>% select(source_category, source_detail,
                    service_domain,
                    std_group    = service_group,   # renamed to avoid collision
                    service_name,
                    is_clinical, is_malaria_related,
                    include_in_analysis, needs_review),
      by = c("service_category" = "source_category",
             "service_detail"   = "source_detail")
    ) %>%
    mutate(service_group = std_group) %>%   # override raw group with standard
    select(-std_group)

  n_unmatched <- sum(is.na(df$service_domain))
  if (n_unmatched > 0)
    warning(sprintf("[TZ services] %d rows unmatched in crosswalk — check for new service_categories", n_unmatched))

  # ── 2. Join uid_registry for facility_uid ───────────────────
  cat("[TZ services] Joining facility UIDs from registry...\n")
  registry <- read_csv(registry_path, show_col_types = FALSE) %>%
    filter(country == country_iso) %>%
    select(facility_code, facility_uid = uid)

  df <- df %>%
    left_join(registry, by = "facility_code")

  n_no_uid <- sum(is.na(df$facility_uid))
  pct_no_uid <- round(n_no_uid / nrow(df) * 100, 1)
  cat(sprintf("[TZ services] Matched facility UIDs: %d (%.1f%% unmatched)\n",
              nrow(df) - n_no_uid, pct_no_uid))

  # ── 3. Generate service UIDs ─────────────────────────────────
  cat("[TZ services] Generating service UIDs...\n")
  df <- df %>%
    mutate(
      uid = sprintf("%s-SVC-%06d", country_iso, row_number())
    )

  # ── 4. Add standard metadata columns ────────────────────────
  df <- df %>%
    mutate(
      country_iso      = country_iso,
      country_name     = country_name,
      source_service_id   = NA_character_,
      source_type_id      = NA_character_,
      source_category_id  = service_category,
      source_service_name = service_detail,
      extracted_date      = as.Date(NA),
      source_url          = raw_path,
      pipeline_version    = pipeline_version
    )

  # ── 5. Select and order final columns ───────────────────────
  out <- df %>%
    select(
      uid, facility_uid, country_iso, country_name,
      service_domain, service_group, service_name,
      source_service_id, source_type_id, source_category_id, source_service_name,
      is_clinical, is_malaria_related, include_in_analysis,
      extracted_date, source_url, pipeline_version,
      facility_code, needs_review
    )

  # ── 6. Save output ───────────────────────────────────────────
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

  out_path <- file.path(output_dir, "tanzania_services_standardized.csv")
  write_csv(out, out_path)
  cat(sprintf("[TZ services] Saved: %s (%d rows)\n", out_path, nrow(out)))

  # ── 7. Write QA report ───────────────────────────────────────
  report_path <- file.path(output_dir, "tanzania_services_report.txt")
  report_lines <- c(
    "AfyaScope — Tanzania Services Standardisation Report",
    sprintf("Generated: %s", Sys.time()),
    sprintf("Pipeline version: %s", pipeline_version),
    "",
    "--- Row counts ---",
    sprintf("Total service records  : %d", nrow(out)),
    sprintf("Unique facility_codes  : %d", length(unique(out$facility_code))),
    sprintf("Matched facility_uid   : %d (%.1f%%)",
            sum(!is.na(out$facility_uid)),
            sum(!is.na(out$facility_uid)) / nrow(out) * 100),
    sprintf("Unmatched facility_uid : %d", sum(is.na(out$facility_uid))),
    sprintf("include_in_analysis TRUE : %d", sum(out$include_in_analysis, na.rm=TRUE)),
    sprintf("needs_review TRUE        : %d", sum(out$needs_review, na.rm=TRUE)),
    "",
    "--- Domain distribution ---"
  )
  dom_tbl <- sort(table(out$service_domain), decreasing = TRUE)
  for (nm in names(dom_tbl))
    report_lines <- c(report_lines,
      sprintf("  %-35s %d (%.1f%%)", nm, dom_tbl[[nm]],
              dom_tbl[[nm]] / nrow(out) * 100))

  report_lines <- c(report_lines, "", "--- Unmatched crosswalk rows ---")
  unmatched <- out %>% filter(is.na(service_domain)) %>%
    distinct(facility_code, source_service_name)
  if (nrow(unmatched) == 0) {
    report_lines <- c(report_lines, "  None")
  } else {
    for (i in seq_len(nrow(unmatched)))
      report_lines <- c(report_lines,
        sprintf("  %s | %s", unmatched$facility_code[i],
                unmatched$source_service_name[i]))
  }

  writeLines(report_lines, report_path)
  cat(sprintf("[TZ services] Report: %s\n", report_path))

  invisible(out)
}
