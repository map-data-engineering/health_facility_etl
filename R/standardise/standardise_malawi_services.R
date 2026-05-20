# =============================================================
# standardise_malawi_services.R
# AfyaScope ETL — Malawi Facility Services Standardisation
#
# Reads:  data/raw/malawi/malawi_hf_services_all.csv
#         crosswalks/malawi_services_crosswalk.csv
#         data/processed/uid_registry.csv
#
# Writes: data/processed/country_standardized/
#           malawi_services_standardized.csv
#           malawi_services_report.txt
#
# NOTE on facility_uid:
#   Malawi facility UIDs (MWI-XXXXXX) are only available after
#   run_malawi_pipeline.R has been executed and uid_registry.csv
#   has been populated for country="MWI".
#   Until then, facility_uid will be NA for all Malawi records.
#
# Usage (from project root):
#   source("R/standardise/standardise_malawi_services.R")
#   standardise_malawi_services()
# =============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
})

standardise_malawi_services <- function(
    raw_path       = "data/raw/malawi/malawi_hf_services_all.csv",
    crosswalk_path = "crosswalks/malawi_services_crosswalk.csv",
    registry_path  = "data/processed/uid_registry.csv",
    output_dir     = "data/processed/country_standardized",
    pipeline_version = "2.0"
) {

  country_iso  <- "MWI"
  country_name <- "Malawi"

  cat(sprintf("\n[Malawi services] Reading raw file: %s\n", raw_path))
  raw <- read_csv(raw_path, show_col_types = FALSE,
                  locale = locale(encoding = "UTF-8"))
  cat(sprintf("[Malawi services] Raw rows: %d | Unique facilities: %d\n",
              nrow(raw), length(unique(raw$facility_code))))

  # ── 1. Join crosswalk (match on trimmed service_name) ────────
  cat("[Malawi services] Joining crosswalk...\n")
  cw <- read_csv(crosswalk_path, show_col_types = FALSE) %>%
    mutate(source_service_name_key = trimws(source_service_name))

  df <- raw %>%
    mutate(service_name_key = trimws(service_name)) %>%
    left_join(
      cw %>% select(
        service_name_key = source_service_name_key,
        service_domain, service_group, service_name_std = service_name,
        is_clinical, is_malaria_related,
        include_in_analysis, needs_review
      ),
      by = "service_name_key"
    )

  n_unmatched <- sum(is.na(df$service_domain))
  if (n_unmatched > 0)
    warning(sprintf(
      "[Malawi services] %d rows unmatched in crosswalk — new service names since crosswalk was built",
      n_unmatched
    ))

  # ── 2. Join uid_registry for facility_uid ────────────────────
  cat("[Malawi services] Joining facility UIDs from registry...\n")
  registry <- read_csv(registry_path, show_col_types = FALSE) %>%
    filter(country == country_iso) %>%
    select(facility_code, facility_uid = uid)

  n_mwi_uids <- nrow(registry)
  if (n_mwi_uids == 0) {
    message(paste0(
      "[Malawi services] WARNING: No MWI UIDs in registry. ",
      "Run run_malawi_pipeline.R first to populate. ",
      "facility_uid will be NA for all records."
    ))
  }

  df <- df %>%
    left_join(registry, by = "facility_code")

  n_matched_uid <- sum(!is.na(df$facility_uid))
  cat(sprintf("[Malawi services] Facility UIDs matched: %d / %d records (%.1f%%)\n",
              n_matched_uid, nrow(df),
              n_matched_uid / nrow(df) * 100))

  # ── 3. Generate service UIDs ──────────────────────────────────
  cat("[Malawi services] Generating service UIDs...\n")
  df <- df %>%
    mutate(
      uid = sprintf("%s-SVC-%06d", country_iso, row_number())
    )

  # ── 4. Add standard metadata columns ─────────────────────────
  df <- df %>%
    mutate(
      country_iso         = country_iso,
      country_name        = country_name,
      source_service_id   = as.character(service_id),
      source_type_id      = as.character(service_type_id),
      source_category_id  = as.character(service_category_id),
      source_service_name = trimws(service_name),
      service_name        = coalesce(service_name_std, trimws(service_name)),
      extracted_date      = as.Date(
                              sub("T.*", "", created_date),
                              format = "%Y-%m-%d"
                            ),
      source_url          = raw_path,
      pipeline_version    = pipeline_version
    )

  # ── 5. Select and order final columns ────────────────────────
  out <- df %>%
    select(
      uid, facility_uid, country_iso, country_name,
      service_domain, service_group, service_name,
      source_service_id, source_type_id, source_category_id, source_service_name,
      is_clinical, is_malaria_related, include_in_analysis,
      extracted_date, source_url, pipeline_version,
      facility_code, facility_name, needs_review
    )

  # ── 6. Save output ────────────────────────────────────────────
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

  out_path <- file.path(output_dir, "malawi_services_standardized.csv")
  write_csv(out, out_path)
  cat(sprintf("[Malawi services] Saved: %s (%d rows)\n", out_path, nrow(out)))

  # ── 7. Write QA report ───────────────────────────────────────
  report_path <- file.path(output_dir, "malawi_services_report.txt")
  report_lines <- c(
    "AfyaScope — Malawi Services Standardisation Report",
    sprintf("Generated: %s", Sys.time()),
    sprintf("Pipeline version: %s", pipeline_version),
    "",
    "--- Row counts ---",
    sprintf("Total service records   : %d", nrow(out)),
    sprintf("Unique facility_codes   : %d", length(unique(out$facility_code))),
    sprintf("Matched facility_uid    : %d (%.1f%%)",
            sum(!is.na(out$facility_uid)),
            sum(!is.na(out$facility_uid)) / nrow(out) * 100),
    sprintf("Unmatched facility_uid  : %d — run run_malawi_pipeline.R to populate",
            sum(is.na(out$facility_uid))),
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

  report_lines <- c(report_lines, "", "--- Unmatched crosswalk rows (new service names) ---")
  unmatched <- out %>% filter(is.na(service_domain)) %>%
    distinct(facility_code, source_service_name)
  if (nrow(unmatched) == 0) {
    report_lines <- c(report_lines, "  None")
  } else {
    for (i in seq_len(min(nrow(unmatched), 50)))
      report_lines <- c(report_lines,
        sprintf("  %s | %s", unmatched$facility_code[i],
                unmatched$source_service_name[i]))
    if (nrow(unmatched) > 50)
      report_lines <- c(report_lines,
        sprintf("  ... and %d more", nrow(unmatched) - 50))
  }

  writeLines(report_lines, report_path)
  cat(sprintf("[Malawi services] Report: %s\n", report_path))

  invisible(out)
}
