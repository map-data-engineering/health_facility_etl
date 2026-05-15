# =============================================================
# validate_coordinates.R
# AfyaScope ETL — Transformation Layer
#
# Performs within-country boundary check using GADM level-0
# shapefiles via the geodata package. GADM files are downloaded
# once and cached in data/cache/gadm/ for all subsequent runs.
#
# Updates coordinate_valid: TRUE = inside country polygon,
#                           FALSE = outside,
#                           NA    = coordinates unavailable.
#
# Requires: sf, terra, geodata  (skips gracefully if absent)
# Run AFTER clean_coordinates.R (which does range checks).
# =============================================================

library(dplyr)

# ISO 3166-1 alpha-3 codes for supported countries
.ISO3_LOOKUP <- c(
  tanzania = "TZA",
  uganda   = "UGA",
  zambia   = "ZMB",
  malawi   = "MWI",
  nigeria  = "NGA",
  botswana = "BWA"
)

validate_coordinates <- function(df, country_name,
                                 cache_dir = "data/cache/gadm") {

  if (!requireNamespace("sf",      quietly = TRUE) ||
      !requireNamespace("terra",   quietly = TRUE) ||
      !requireNamespace("geodata", quietly = TRUE)) {
    message("  [validate_coords] sf/terra/geodata not available — skipping boundary check")
    message("  [validate_coords] Install with: install.packages(c('sf','terra','geodata'))")
    return(df)
  }

  iso3 <- .ISO3_LOOKUP[tolower(country_name)]
  if (is.na(iso3)) {
    message(sprintf("  [validate_coords] No ISO3 code for '%s' — skipping", country_name))
    return(df)
  }

  has_coords <- !is.na(df$latitude) & !is.na(df$longitude) &
                !isFALSE(df$coordinate_valid)

  if (sum(has_coords) == 0) {
    message("  [validate_coords] No rows with coordinates to validate")
    return(df)
  }

  tryCatch({
    if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)

    # geodata::gadm() downloads once and reads from cache on all subsequent runs
    gadm_spat    <- geodata::gadm(country = iso3, level = 0, path = cache_dir)
    country_shape <- sf::st_as_sf(gadm_spat)

    df_sf <- df[has_coords, ] %>%
      sf::st_as_sf(coords = c("longitude", "latitude"), crs = 4326)

    inside <- as.vector(sf::st_within(df_sf, country_shape, sparse = FALSE))

    df$coordinate_valid[has_coords] <- inside
    # Only NA-out records with genuinely missing coordinates; records that
    # were already flagged FALSE by the bbox check in clean_coordinates must
    # keep their FALSE so flag_data_quality assigns "low" correctly.
    is_missing <- is.na(df$latitude) | is.na(df$longitude)
    df$coordinate_valid[is_missing] <- NA

    n_outside <- sum(!inside, na.rm = TRUE)
    if (n_outside > 0)
      message(sprintf("  [validate_coords] %d points fall outside %s boundary (flagged FALSE)",
                      n_outside, country_name))
    else
      message(sprintf("  [validate_coords] All %d tested points inside %s boundary",
                      sum(has_coords), country_name))

  }, error = function(e) {
    message("  [validate_coords] Boundary check failed: ", e$message)
  })

  return(df)
}
