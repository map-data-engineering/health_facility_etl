# =============================================================
# extract_botswana.R
# AfyaScope ETL — Extraction Layer (Botswana)
#
# Source: Botswana Master Facility List
#   Public site (Angular SPA): https://healthfacilities.gov.bw
#   Backing client API:        https://mfldit.bitri-ist.co.bw/api/facility/client/v1/all
#
# The /all endpoint returns ~1,080 facilities in a single JSON
# payload with nested `facilityServices`, `knownYesNoInfrastructures`,
# `facilityInfrastructures` (mixed staff + equipment), and `personnel`
# arrays per facility.
#
# Writes two raw CSVs (gitignored, regenerated on demand):
#   data/raw/botswana/Facilities_List_Nov2026.csv
#       — columns match config/countries.yml's `botswana:` block.
#   data/raw/botswana/botswana_services_raw.csv
#       — one row per (facility, service item) pair, used by
#         standardise_services.R via the BWA crosswalk.
#
# Usage (from repo root):
#   Rscript scripts/extraction/extract_botswana.R
# =============================================================

suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(dplyr)
  library(purrr)
  library(readr)
})

`%||%` <- function(a, b) if (is.null(a) || length(a) == 0) b else a

api_url     <- "https://mfldit.bitri-ist.co.bw/api/facility/client/v1/all"
raw_dir     <- "data/raw/botswana"
fac_path    <- file.path(raw_dir, "Facilities_List_Nov2026.csv")
svc_path    <- file.path(raw_dir, "botswana_services_raw.csv")

dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)

message("Fetching ", api_url, " ...")
resp <- httr::GET(api_url, httr::timeout(180),
                  httr::add_headers(Accept = "application/json"))
httr::stop_for_status(resp)
facilities <- jsonlite::fromJSON(httr::content(resp, as = "text",
                                               encoding = "UTF-8"),
                                 simplifyVector = FALSE)
message("Got ", length(facilities), " facilities")

# ── 1. Build raw facility table (column names match countries.yml) ─
facility_rows <- purrr::map_dfr(facilities, function(f) {
  code <- if (nzchar(f$newFacilityCode %||% "")) f$newFacilityCode else f$id
  tibble::tibble(
    `Facility Name`         = f$facilityName        %||% NA_character_,
    `New Facility Code`     = code,
    `Old Facility Code`     = f$oldFacilityCode     %||% NA_character_,
    District                = f$district$name       %||% NA_character_,
    `Sub-District`          = f$physicalAddress$district$name %||% NA_character_,
    Constituency            = f$constituency$name   %||% NA_character_,
    `Service Delivery Type` = f$facilityType$name   %||% NA_character_,
    `Facility Owner`        = f$facilityOwner       %||% NA_character_,
    Latitude                = suppressWarnings(as.numeric(f$lat %||% NA)),
    Longitude               = suppressWarnings(as.numeric(f$lng %||% NA)),
    Telephone               = f$telephone           %||% NA_character_,
    `Urban or Rural`        = f$isUrbanOrRural      %||% NA_character_,
    `Always Open`           = isTRUE(f$isAlwaysOpen),
    `Facility Status`       = f$status              %||% NA_character_,
    `Publication Status`    = f$facilityStatus      %||% NA_character_,
    `Catchment Area`        = f$catchmentArea       %||% NA_character_,
    Updated                 = f$updated             %||% NA_character_
  )
})

readr::write_csv(facility_rows, fac_path, na = "")
message(sprintf("Wrote %d facilities -> %s", nrow(facility_rows), fac_path))

# ── 2. Build raw services table (long format, one row per item) ────
extract_services <- function(f) {
  code <- if (nzchar(f$newFacilityCode %||% "")) f$newFacilityCode else f$id

  clinical <- purrr::map_chr(f$facilityServices %||% list(),
                             ~ .x$name %||% NA_character_)
  clinical_rows <- if (length(clinical))
    tibble::tibble(
      facility_code    = code,
      service_category = "clinical_services",
      service_name     = clinical,
      quantity         = NA_integer_,
      is_available     = TRUE
    ) else NULL

  yn <- f$knownYesNoInfrastructures %||% list()
  yn_rows <- if (length(yn))
    tibble::tibble(
      facility_code    = code,
      service_category = "infrastructure",
      service_name     = purrr::map_chr(yn, ~ .x$type %||% NA_character_),
      quantity         = NA_integer_,
      is_available     = purrr::map_lgl(yn,
                                        ~ isTRUE(.x$isAvailable) || isTRUE(.x$available))
    ) else NULL

  infra <- f$facilityInfrastructures %||% list()
  infra_rows <- if (length(infra))
    purrr::map_dfr(infra, function(i) {
      tp <- i$facilityInfrastructureType
      if (is.null(tp$type)) return(NULL)
      is_staff <- isTRUE(tp$isStaff) || isTRUE(tp$staff)
      tibble::tibble(
        facility_code    = code,
        service_category = if (is_staff) "staff" else "infrastructure",
        service_name     = tp$type,
        quantity         = suppressWarnings(as.integer(i$quantity %||% 0L)),
        is_available     = isTRUE(tp$isAvailable) || isTRUE(tp$available)
      )
    }) else NULL

  dplyr::bind_rows(clinical_rows, yn_rows, infra_rows)
}

service_rows <- purrr::map_dfr(facilities, extract_services) |>
  dplyr::filter(!is.na(service_name), nzchar(service_name)) |>
  # Drop yes/no infrastructure rows where item is reported absent
  dplyr::filter(is.na(is_available) | is_available)

readr::write_csv(service_rows, svc_path, na = "")
message(sprintf("Wrote %d service rows for %d facilities -> %s",
                nrow(service_rows),
                dplyr::n_distinct(service_rows$facility_code),
                svc_path))

cat("\nCategory distribution:\n")
print(table(service_rows$service_category))
