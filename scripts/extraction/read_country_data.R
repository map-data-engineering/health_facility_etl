# =============================================================
# read_country_data.R
# AfyaScope ETL — Extraction Layer
#
# Reads raw country file (CSV or XLSX) using the path defined
# in countries.yml. Returns a raw data frame with no changes.
# =============================================================

library(readr)
library(readxl)

read_country_data <- function(country_name, countries_config) {

  cfg      <- countries_config[[country_name]]
  file_path <- cfg$raw_file

  if (is.null(file_path) || !file.exists(file_path)) {
    stop(paste0("Raw file not found for '", country_name, "': ", file_path))
  }

  if (grepl("\\.csv$", file_path, ignore.case = TRUE)) {
    df <- read_csv(file_path, show_col_types = FALSE,
                   locale = locale(encoding = "latin1"))

  } else if (grepl("\\.xlsx$", file_path, ignore.case = TRUE)) {
    df <- read_excel(file_path)

  } else {
    stop(paste0("Unsupported file type for: ", file_path))
  }

  # Normalise column names: trim whitespace
  names(df) <- trimws(names(df))

  return(df)
}
