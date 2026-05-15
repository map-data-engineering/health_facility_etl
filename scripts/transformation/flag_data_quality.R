# =============================================================
# flag_data_quality.R
# AfyaScope ETL — Transformation Layer  (NEW)
#
# Assigns a data_quality_flag per record based on completeness
# of the most critical portal fields:
#
#   high    : coords valid + name + type + ownership + status
#   medium  : coords valid but some secondary fields missing
#   low     : coords missing/invalid OR name missing
#   unknown : cannot assess
# =============================================================

library(dplyr)

flag_data_quality <- function(df) {

  df <- df %>%
    mutate(
      data_quality_flag = case_when(

        # Low: no coordinates or no name  (evaluated before "unknown" so
        # a record with both missing name AND missing coords → "low", not "unknown")
        is.na(latitude) | is.na(longitude)     ~ "low",
        # Use == FALSE / == TRUE (vectorised); isFALSE()/isTRUE() are scalar-only
        # and always return FALSE inside a case_when column expression.
        coordinate_valid == FALSE              ~ "low",
        is.na(facility_name)                   ~ "low",

        # High: coords good + name + type + ownership all present
        !is.na(latitude) & !is.na(longitude) &
          coordinate_valid == TRUE &
          !is.na(facility_name) &
          !is.na(facility_type) &
          !is.na(ownership)                    ~ "high",

        # Medium: coords good but something else missing
        !is.na(latitude) & !is.na(longitude) &
          coordinate_valid == TRUE             ~ "medium",

        TRUE                                   ~ "unknown"
      )
    )

  flag_summary <- table(df$data_quality_flag)
  message("  [quality] flags: ", paste(names(flag_summary), flag_summary, sep = "=", collapse = ", "))

  return(df)
}
