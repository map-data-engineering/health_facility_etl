# =============================================================
# extract_ethiopia.R
# AfyaScope ETL — Extraction Layer (Ethiopia)
#
# Source
#   Ethiopia MoH Master Facility Registry, version 2 (MFR v2)
#     https://mfrv2.moh.gov.et/#/facility/all
#   The portal team supplies a CSV export from MFR v2's public
#   Facility-All page, ~64K rows with Region/Zone/Woreda and
#   four semicolon-joined service columns (Clinical / Diagnostic
#   / Support / 24Hr). Crucially the public export has NO GPS
#   coordinates.
#
# Coordinate strategy
#   MFR v2 internally carries lat/lng but the production API
#   (https://mfr.moh.gov.et/api/Facility/GetFacilities) requires
#   authentication (401 anonymous), so we can't query it.
#   Instead we join against HDX's "Ethiopian Health Facilities"
#   dataset — the WHO AFRO compilation of the same upstream
#   registry — which is openly licensed and has lat/lng for
#   ~40,525 facilities. The join key is the shared numeric
#   Facility ID. Match rate is ~63%.
#
#   Unmatched rows (mostly private clinics, drug stores,
#   pharmacies — outside WHO AFRO's traditional coverage scope)
#   are dropped downstream by the standard clean_coordinates
#   step because they have no GPS.
#
#   See scripts/extraction/geocode_ethiopia.R in the portal repo
#   for the same logic as a standalone script.
#
# Inputs (gitignored, supply locally):
#   data/raw/ethiopia/ethiopia_facilities_source.csv
#   data/raw/ethiopia/hdx_ethiopia_coords.csv
#
# Outputs:
#   data/raw/ethiopia/Ethiopia_HFR_2026.csv
#     — facility list with HDX-joined latitude / longitude.
#     — schema lines up with config/countries.yml ethiopia block.
#   data/raw/ethiopia/ethiopia_services_raw.csv
#     — long format: facility_code, service_category, service_name.
#
# Usage from repo root:
#   Rscript scripts/extraction/extract_ethiopia.R
# =============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tidyr)
  library(stringr)
})

raw_dir       <- "data/raw/ethiopia"
src_path      <- file.path(raw_dir, "ethiopia_facilities_source.csv")
hdx_path      <- file.path(raw_dir, "hdx_ethiopia_coords.csv")
fac_out_path  <- file.path(raw_dir, "Ethiopia_HFR_2026.csv")
svc_out_path  <- file.path(raw_dir, "ethiopia_services_raw.csv")

for (p in c(src_path, hdx_path)) {
  if (!file.exists(p)) stop("Missing raw file: ", p)
}

message("Reading source ...")
src <- read_csv(src_path, show_col_types = FALSE)
hdx <- read_csv(hdx_path, show_col_types = FALSE)

src$`Facility ID` <- as.character(src$`Facility ID`)
hdx$Id            <- as.character(hdx$Id)

message(sprintf("Source rows: %d (distinct IDs: %d)",
                nrow(src), n_distinct(src$`Facility ID`)))
message(sprintf("HDX rows:    %d (all geolocated)", nrow(hdx)))

# ── 1. Build the facility table ────────────────────────────
fac <- src |>
  left_join(
    hdx |> select(Id, Latitude, Longitude),
    by = c("Facility ID" = "Id")
  ) |>
  transmute(
    facility_code      = `Facility ID`,
    facility_name      = `Facility Name`,
    facility_type      = `Facility Type`,
    facility_ownership = Ownership,
    admin1             = Region,
    admin2             = Zone,
    admin3             = Woreda,
    latitude           = Latitude,
    longitude          = Longitude,
    status             = `Operational Status`
  ) |>
  distinct(facility_code, .keep_all = TRUE)

write_csv(fac, fac_out_path, na = "")
n_with_coord <- sum(!is.na(fac$latitude))
message(sprintf("Wrote %d facilities (%d with HDX coords, %.1f%%) -> %s",
                nrow(fac), n_with_coord,
                n_with_coord / nrow(fac) * 100, fac_out_path))

# ── 2. Build the long-format services table ────────────────
unnest_field <- function(df, col_name, category) {
  df |>
    select(facility_code = `Facility ID`, value = all_of(col_name)) |>
    filter(!is.na(value), value != "") |>
    mutate(value = str_split(value, ";")) |>
    tidyr::unnest(value) |>
    mutate(value = str_squish(value)) |>
    filter(value != "") |>
    mutate(service_category = category) |>
    select(facility_code, service_category, service_name = value)
}

services <- bind_rows(
  unnest_field(src, "Clinical Services",   "clinical_services"),
  unnest_field(src, "Support Services",    "support_services"),
  unnest_field(src, "Diagnostic Services", "diagnostic_services"),
  unnest_field(src, "24Hr Services",       "twenty_four_hour")
)

write_csv(services, svc_out_path, na = "")
message(sprintf("Wrote %d service rows (%d facilities, %d distinct names) -> %s",
                nrow(services),
                n_distinct(services$facility_code),
                n_distinct(services$service_name),
                svc_out_path))

cat("\nCategory distribution:\n")
print(table(services$service_category))
