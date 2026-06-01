# =============================================================
# extract_kenya.R
# AfyaScope ETL — Extraction Layer (Kenya)
#
# Source: Kenya Master Health Facility Registry
#   Public site: https://kmhfr.health.go.ke
#   Backing API: https://api.kmhfr.health.go.ke/api/v1/...
#
# Note on raw data
# ----------------
# KMHFR exposes a paginated public API but extracting the full
# registry + per-facility service profiles requires hundreds of
# authenticated calls. For this pass we use the pre-staged copy
# already extracted by the portal team:
#
#   data/raw/kenya/Kenya_KMHFR_2026.csv      — facility list
#   data/raw/kenya/kenya_services_raw.csv    — long-format services
#
# Both files are gitignored; ship them alongside this script when
# bootstrapping the ETL on a fresh machine. The TODO at the bottom
# captures the eventual API-driven refresh path.
#
# This script is a documentation stub for now — it sanity-checks
# that the expected files exist and reports shape.
# =============================================================

suppressPackageStartupMessages({
  library(readr)
})

raw_dir   <- "data/raw/kenya"
fac_path  <- file.path(raw_dir, "Kenya_KMHFR_2026.csv")
svc_path  <- file.path(raw_dir, "kenya_services_raw.csv")

for (p in c(fac_path, svc_path)) {
  if (!file.exists(p))
    stop("Missing raw file for Kenya: ", p,
         "\n  Copy it from the portal repo's data/Kenya/ folder, or",
         "\n  re-fetch from https://api.kmhfr.health.go.ke.")
}

fac <- readr::read_csv(fac_path, show_col_types = FALSE)
svc <- readr::read_csv(svc_path, show_col_types = FALSE)

message(sprintf("Kenya facilities: %d rows / %d columns",
                nrow(fac), ncol(fac)))
message(sprintf("Kenya services  : %d rows (%d facilities, %d categories)",
                nrow(svc),
                length(unique(svc$facility_code)),
                length(unique(svc$service_category))))

# TODO — replace this stub with an authenticated KMHFR fetch:
#   1. POST credentials to https://api.kmhfr.health.go.ke/api/v1/oauth/token
#   2. GET https://api.kmhfr.health.go.ke/api/v1/facilities/?page=...
#      (paginate; ~17K facilities)
#   3. For each facility GET .../<uuid>/services/
#   4. Map facility uuid -> facility_code via the facility payload
#   5. Write Kenya_KMHFR_<YYYY>.csv + kenya_services_raw.csv
