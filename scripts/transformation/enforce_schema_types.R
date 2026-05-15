# =============================================================
# enforce_schema_types.R
# AfyaScope ETL — Transformation Layer
#
# Loads the schema from schema.yml.
# For every column defined in the schema:
#   - If present in df → coerce to schema type
#   - If absent       → add as NA with correct type
# Columns in df that are NOT in schema are preserved as-is
# (they are source-specific extras, not errors).
# =============================================================

library(yaml)
library(dplyr)

load_schema <- function(path = "config/schema.yml") {
  read_yaml(path)$standard_columns
}

enforce_schema_types <- function(df, schema) {

  for (col in names(schema)) {

    type <- schema[[col]]$type

    # Add column as NA if missing from df
    if (!col %in% names(df)) {
      df[[col]] <- switch(type,
        "string"  = NA_character_,
        "float"   = NA_real_,
        "integer" = NA_integer_,
        "boolean" = NA,
        "date"    = as.Date(NA),
        NA_character_
      )
    }

    # Coerce existing column to schema type
    df[[col]] <- switch(type,
      "string"  = as.character(df[[col]]),
      "float"   = suppressWarnings(as.numeric(df[[col]])),
      "integer" = suppressWarnings(as.integer(df[[col]])),
      "boolean" = {
        val <- df[[col]]
        if (is.logical(val)) val
        else as.logical(toupper(as.character(val)) %in% c("TRUE", "YES", "1", "Y"))
      },
      "date"    = {
        val <- df[[col]]
        if (inherits(val, "Date")) val else as.Date(as.character(val))
      },
      df[[col]]  # default: leave as-is
    )
  }

  return(df)
}
