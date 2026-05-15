# =============================================================
# save_standardized_data.R
# AfyaScope ETL — Loading Layer
#
# Saves the standardized country dataframe to:
#   data/processed/country_standardized/<country>_standardized.csv
#
# Also writes a companion <country>_standardized_report.txt with
# row count, column list, and null summary for quick QA.
# =============================================================

library(readr)

save_standardized_data <- function(df, country_name) {

  out_dir <- "data/processed/country_standardized"
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

  out_path <- file.path(out_dir, paste0(country_name, "_standardized.csv"))
  write_csv(df, out_path)
  message(sprintf("  [save] Written: %s  (%d rows x %d cols)", out_path, nrow(df), ncol(df)))

  # ── QA report ──────────────────────────────────────────────
  report_path <- file.path(out_dir, paste0(country_name, "_standardized_report.txt"))

  null_pct <- sapply(df, function(x) round(mean(is.na(x)) * 100, 1))
  report_lines <- c(
    paste0("AfyaScope — Standardized Data Report"),
    paste0("Country      : ", tools::toTitleCase(country_name)),
    paste0("Generated    : ", Sys.time()),
    paste0("Rows         : ", nrow(df)),
    paste0("Columns      : ", ncol(df)),
    "",
    "Column completeness (% non-null):",
    paste0(sprintf("  %-35s %5.1f%%", names(null_pct), 100 - null_pct), collapse = "\n"),
    "",
    "Data quality flag distribution:",
    if ("data_quality_flag" %in% names(df))
      paste(capture.output(print(table(df$data_quality_flag))), collapse = "\n")
    else "  (not computed)"
  )

  writeLines(report_lines, report_path)
  message(sprintf("  [save] QA report: %s", report_path))
}
