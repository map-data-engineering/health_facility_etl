# =============================================================
# clean_dates.R
# AfyaScope ETL — Transformation Layer  (NEW)
#
# Parses open_date_raw → open_date (Date) and open_year (int).
#
# Handles known source formats:
#   - Tanzania HFR : "01jan1970", "12apr2025"  (Stata %td style)
#   - Uganda MFL   : ISO "YYYY-MM-DD" or "YYYY-MM-DDTHH:MM:SS"
#   - General ISO  : "YYYY-MM-DD"
#
# Epoch-zero dates (1970-01-01) from Tanzania are treated as
# unknown and set to NA — they represent missing data, not a
# real opening date.
#
# Implausible years (< 1900 or > current year) are also NA'd.
# =============================================================

library(dplyr)
library(lubridate)

clean_dates <- function(df) {

  current_year <- as.integer(format(Sys.Date(), "%Y"))

  if (!"open_date_raw" %in% names(df)) {
    # Country has no open date in source — create empty columns
    df$open_date <- as.Date(NA)
    df$open_year <- NA_integer_
    return(df)
  }

  df <- df %>%
    mutate(
      open_date = suppressWarnings({

        raw <- as.character(open_date_raw)

        # Try Stata %td format: ddmmmyyyy e.g. "01jan1970", "12apr2025"
        parsed_stata <- as.Date(raw, format = "%d%b%Y")

        # Try ISO format: "YYYY-MM-DD" or datetime "YYYY-MM-DDTHH:MM:SS"
        parsed_iso <- as.Date(substr(raw, 1, 10), format = "%Y-%m-%d")

        # Use stata if it worked, else iso
        result <- dplyr::if_else(!is.na(parsed_stata), parsed_stata, parsed_iso)

        # Null out epoch-zero (Tanzania missing-date sentinel)
        epoch_zero <- as.Date("1970-01-01")
        result <- dplyr::if_else(result == epoch_zero, as.Date(NA), result)

        result
      }),

      open_year = as.integer(format(open_date, "%Y")),

      # Null implausible years
      open_year = dplyr::if_else(
        !is.na(open_year) & (open_year < 1900 | open_year > current_year),
        NA_integer_,
        open_year
      ),
      open_date = dplyr::if_else(
        !is.na(open_year) | is.na(open_date),
        open_date,
        open_date
      )
    ) %>%
    select(-open_date_raw)

  n_na   <- sum(is.na(df$open_date))
  n_good <- sum(!is.na(df$open_date))
  message(sprintf("  [dates] open_date: %d valid, %d NA (unknown/epoch-zero/implausible)",
                  n_good, n_na))

  return(df)
}
