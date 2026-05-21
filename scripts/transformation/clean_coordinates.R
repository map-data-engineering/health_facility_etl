# =============================================================
# clean_coordinates.R
# HealthScape ETL — Transformation Layer
#
# 1. Coerces lat/lon to numeric
# 2. Removes records with missing coordinates
# 3. Flags implausible values: global range + country bounding box
# 4. Adds coordinate_valid column (basic range + bbox check)
# 5. Validates points against country shapefile via malariaAtlas::getShp()
#    and retains only points falling within the country boundary
# =============================================================

library(dplyr)
library(sf)
library(malariaAtlas)

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
  
  # ------------------------------------------------------------------
  # Step 1: Coerce to numeric
  # ------------------------------------------------------------------
  df <- df %>%
    mutate(
      latitude  = suppressWarnings(as.numeric(latitude)),
      longitude = suppressWarnings(as.numeric(longitude))
    )
  
  # ------------------------------------------------------------------
  # Step 2: Remove records with missing coordinates
  # ------------------------------------------------------------------
  n_missing <- sum(is.na(df$latitude) | is.na(df$longitude))
  if (n_missing > 0) {
    message(sprintf("  [coordinates] %d records with missing lat/lon removed", n_missing))
    df <- df %>% filter(!is.na(latitude), !is.na(longitude))
  }
  
  # ------------------------------------------------------------------
  # Step 3: Bounding box validation
  # ------------------------------------------------------------------
  bbox <- if (!is.null(country)) .COUNTRY_BBOX[[tolower(country)]] else NULL
  
  df <- df %>%
    mutate(
      coordinate_valid = case_when(
        latitude  < -90  | latitude  > 90    ~ FALSE,
        longitude < -180 | longitude > 180   ~ FALSE,
        !is.null(bbox) & (
          latitude  < bbox$lat_min | latitude  > bbox$lat_max |
            longitude < bbox$lon_min | longitude > bbox$lon_max
        )                                    ~ FALSE,
        TRUE                                 ~ TRUE
      )
    )
  
  n_invalid <- sum(df$coordinate_valid == FALSE, na.rm = TRUE)
  if (n_invalid > 0) {
    message(sprintf("  [coordinates] %d records have implausible coordinates (flagged FALSE)", n_invalid))
  }
  
  # ------------------------------------------------------------------
  # Step 4: Polygon validation via malariaAtlas::getShp()
  #         Capitalise first letter to match malariaAtlas expectations
  #         e.g. "tanzania" -> "Tanzania"
  # ------------------------------------------------------------------
  if (!is.null(country)) {
    
    country_label <- paste0(toupper(substring(country, 1, 1)),
                            tolower(substring(country, 2)))
    
    message(sprintf("  [coordinates] Fetching shapefile for %s via malariaAtlas ...", country_label))
    
    country_shp <- malariaAtlas::getShp(country = country_label, admin_level = "admin0")
    country_shp <- st_as_sf(country_shp)
    country_shp <- st_make_valid(country_shp)
    
    # Convert df to sf, filter to valid-flagged points only before spatial check
    df_sf <- df %>%
      filter(coordinate_valid == TRUE) %>%
      st_as_sf(coords = c("longitude", "latitude"), crs = 4326, remove = FALSE)
    
    # Ensure matching CRS
    country_shp <- st_transform(country_shp, crs = st_crs(df_sf))
    
    # Spatial filter: keep only points within country boundary
    df_inside  <- df_sf[country_shp, ]
    n_outside  <- nrow(df_sf) - nrow(df_inside)
    
    if (n_outside > 0) {
      message(sprintf("  [coordinates] %d records fall outside %s boundary and were removed",
                      n_outside, country_label))
    }
    
    # Drop sf geometry, return plain dataframe
    df_inside <- st_drop_geometry(df_inside)
    
    # Bind back the coordinate_valid == FALSE records if you want to retain them,
    # or drop entirely — here we drop since they failed validation
    df <- df_inside
    
  }
  
  return(df)
}