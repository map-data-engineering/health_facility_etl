# =============================================================
# generate_facility_uid.R
# AfyaScope ETL — Transformation Layer
#
# Mints stable global facility UIDs of the form:
#   <ISO3>-<NNNNNN>   e.g.  TZA-000142  UGA-003891
#
# A uid_registry.csv lookup table persists UIDs across runs:
#   country | facility_code | uid | created_date
#
# Re-ingestion logic:
#   - Existing (country + facility_code) pair → reuse stored UID
#   - New pair → mint next available number for that country
#   - facility_code == NA → skip (UID is NA; logged as a warning)
#
# The uid column is inserted into the returned data frame and
# also written as the first schema column for portal use.
# =============================================================

library(dplyr)

.ISO3 <- c(
  tanzania = "TZA",
  uganda   = "UGA",
  zambia   = "ZMB",
  malawi   = "MWI",
  nigeria  = "NGA",
  botswana = "BWA",
  kenya    = "KEN",
  ethiopia = "ETH"
)

generate_facility_uid <- function(df, country_name,
                                  registry_path = "data/processed/uid_registry.csv") {

  iso3 <- .ISO3[tolower(country_name)]
  if (is.na(iso3)) {
    warning(sprintf("[generate_uid] Unknown country '%s' — UIDs not assigned", country_name))
    df$uid <- NA_character_
    return(df)
  }

  # ── Load or create registry ────────────────────────────────
  if (file.exists(registry_path)) {
    registry <- readr::read_csv(registry_path, show_col_types = FALSE,
                                col_types = readr::cols(
                                  country       = readr::col_character(),
                                  facility_code = readr::col_character(),
                                  uid           = readr::col_character(),
                                  created_date  = readr::col_date()
                                ))
  } else {
    registry <- data.frame(
      country       = character(),
      facility_code = character(),
      uid           = character(),
      created_date  = as.Date(character()),
      stringsAsFactors = FALSE
    )
  }

  # ── Determine next sequence number for this country ────────
  existing_for_country <- registry[registry$country == iso3, "uid", drop = TRUE]
  if (length(existing_for_country) > 0) {
    seq_nums    <- suppressWarnings(
      as.integer(sub(paste0("^", iso3, "-0*"), "", existing_for_country))
    )
    next_seq <- max(seq_nums, na.rm = TRUE) + 1L
  } else {
    next_seq <- 1L
  }

  # ── Assign UIDs ────────────────────────────────────────────
  n_reused  <- 0L
  n_minted  <- 0L
  n_skipped <- 0L

  uid_vec <- character(nrow(df))
  new_rows <- list()

  for (i in seq_len(nrow(df))) {
    fc <- df$facility_code[i]

    if (is.na(fc) || fc == "") {
      uid_vec[i] <- NA_character_
      n_skipped  <- n_skipped + 1L
      next
    }

    match_row <- registry[registry$country == iso3 &
                          registry$facility_code == fc, ]

    if (nrow(match_row) > 0) {
      uid_vec[i] <- match_row$uid[1]
      n_reused   <- n_reused + 1L
    } else {
      new_uid    <- sprintf("%s-%06d", iso3, next_seq)
      uid_vec[i] <- new_uid
      next_seq   <- next_seq + 1L
      n_minted   <- n_minted + 1L

      new_rows[[length(new_rows) + 1]] <- data.frame(
        country       = iso3,
        facility_code = as.character(fc),
        uid           = new_uid,
        created_date  = Sys.Date(),
        stringsAsFactors = FALSE
      )
    }
  }

  df$uid <- uid_vec

  # ── Persist updated registry ───────────────────────────────
  if (length(new_rows) > 0) {
    registry <- bind_rows(registry, bind_rows(new_rows))

    reg_dir <- dirname(registry_path)
    if (!dir.exists(reg_dir)) dir.create(reg_dir, recursive = TRUE)
    readr::write_csv(registry, registry_path)
  }

  if (n_skipped > 0)
    warning(sprintf("[generate_uid] %d rows have no facility_code — uid set to NA", n_skipped))

  message(sprintf(
    "  [uid] Minted: %d  |  Reused: %d  |  Skipped (no code): %d",
    n_minted, n_reused, n_skipped
  ))

  return(df)
}
