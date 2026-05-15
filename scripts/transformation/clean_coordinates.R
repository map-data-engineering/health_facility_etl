# =============================================================
# clean_coordinates.R
# AfyaScope ETL — Transformation Layer
#
# 1. Coerces lat/lon to numeric
# 2. Flags implausible values: global range + country bounding box
# 3. Adds coordinate_valid column (basic range + bbox check)
#    Full within-country polygon validation is done separately via
#    validate_coordinates.R which requires the sf/geodata packages.
# =============================================================

library(dplyr)

# Country bounding boxes: generous but realistic extents.
# Points outside the bbox are almost certainly swapped or otherwise wrong.
.COUNTRY_BBOX <- list(
  tanzania = list(lat_min = -12, lat_max =  0,  lon_min = 28, lon_max = 41),
  uganda   = list(lat_min =  -2, lat_max =  5,  lon_min = 29, lon_max = 36),
  zambia   = list(lat_min = -19, lat_max = -7,  lon_min = 21, lon_max = 35),
  malawi   = list(lat_min = -18, lat_max = -8,  lon_min = 32, lon_max = 37),
  nigeria  = list(lat_min =   3, lat_max = 15,  lon_min =  2, lon_max = 16),
  botswana = list(lat_min = -28, lat_max = -17, lon_min = 19, lon_max = 30)
)

clean_coordinates <- function(df, country = NULL) {

  df <- df %>%
    mutate(
      latitude  = suppressWarnings(as.numeric(latitude)),
      longitude = suppressWarnings(as.numeric(longitude))
    )

  bbox <- if (!is.null(country)) .COUNTRY_BBOX[[tolower(country)]] else NULL

  df <- df %>%
    mutate(
      coordinate_valid = case_when(
        is.na(latitude) | is.na(longitude)   ~ NA,
        latitude  < -90  | latitude  > 90    ~ FALSE,
        longitude < -180 | longitude > 180   ~ FALSE,
        # Country-aware bounding box: catches swapped and grossly wrong coords.
        # Falls through to TRUE (= passes range check) when no bbox is available.
        !is.null(bbox) & (
          latitude  < bbox$lat_min | latitude  > bbox$lat_max |
          longitude < bbox$lon_min | longitude > bbox$lon_max
        )                                    ~ FALSE,
        TRUE                                 ~ TRUE
      )
    )

  n_missing <- sum(is.na(df$latitude) | is.na(df$longitude))
  n_invalid <- sum(df$coordinate_valid == FALSE, na.rm = TRUE)

  if (n_missing > 0)
    message(sprintf("  [coordinates] %d records missing lat/lon (kept, flagged)", n_missing))
  if (n_invalid > 0)
    message(sprintf("  [coordinates] %d records have implausible coordinates (flagged FALSE)", n_invalid))

  return(df)
}
