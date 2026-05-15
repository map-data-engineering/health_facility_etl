# =============================================================
# geocode_missing.R
# AfyaScope ETL — Transformation Layer
#
# OSM Nominatim fallback geocoder for facilities with missing
# coordinates.  Runs AFTER clean_coordinates.R.
#
# Behaviour:
#   - Queries Nominatim once per facility with missing lat/lon
#   - Rate-limits to 1 request / second (Nominatim policy)
#   - Caches all API responses in data/cache/geocode/geocode_cache.csv
#     so re-runs skip already-queried facilities
#   - On success : sets latitude, longitude, coordinate_source = "OSM"
#                  coordinate_valid left NA (validate_coordinates.R checks it)
#   - On failure : writes the row to geocode_failures.csv and leaves
#                  latitude/longitude as NA
#
# Requires: httr2, jsonlite  (skips gracefully if absent)
# =============================================================

library(dplyr)

geocode_missing <- function(df, country_name,
                            cache_dir     = "data/cache/geocode",
                            failures_file = "data/processed/geocode_failures.csv") {

  if (!requireNamespace("httr2",    quietly = TRUE) ||
      !requireNamespace("jsonlite", quietly = TRUE)) {
    message("  [geocode] httr2/jsonlite not available — skipping geocoding")
    message("  [geocode] Install with: install.packages(c('httr2','jsonlite'))")
    return(df)
  }

  missing_mask <- is.na(df$latitude) | is.na(df$longitude)
  n_missing    <- sum(missing_mask)

  if (n_missing == 0) {
    message("  [geocode] No missing coordinates — nothing to geocode")
    return(df)
  }
  message(sprintf("  [geocode] %d facilities with missing coordinates", n_missing))

  # ── Load / create cache ────────────────────────────────────
  if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)
  cache_file <- file.path(cache_dir, "geocode_cache.csv")

  if (file.exists(cache_file)) {
    cache <- readr::read_csv(cache_file, show_col_types = FALSE)
  } else {
    cache <- data.frame(
      query        = character(),
      result_lat   = numeric(),
      result_lon   = numeric(),
      result_type  = character(),
      stringsAsFactors = FALSE
    )
  }

  # ── Helpers ────────────────────────────────────────────────
  make_query <- function(name, admin1, country) {
    parts <- na.omit(c(trimws(name), trimws(admin1), trimws(country)))
    paste(parts, collapse = ", ")
  }

  nominatim_lookup <- function(query) {
    tryCatch({
      resp <- httr2::request("https://nominatim.openstreetmap.org/search") |>
        httr2::req_headers(
          `User-Agent` = "AfyaScope-ETL/2.0 (myallachristina@gmail.com)"
        ) |>
        httr2::req_url_query(
          q      = query,
          format = "json",
          limit  = 1
        ) |>
        httr2::req_timeout(10) |>
        httr2::req_perform()

      results <- httr2::resp_body_json(resp, simplifyVector = TRUE)

      if (length(results) == 0) return(NULL)

      list(
        lat  = as.numeric(results[[1]]$lat),
        lon  = as.numeric(results[[1]]$lon),
        type = as.character(results[[1]]$type)
      )
    }, error = function(e) {
      message("    [geocode] API error for query '", query, "': ", e$message)
      NULL
    })
  }

  # ── Process each missing record ────────────────────────────
  failures    <- list()
  n_geocoded  <- 0
  n_cached    <- 0
  n_failed    <- 0

  missing_idx <- which(missing_mask)

  for (i in missing_idx) {
    row <- df[i, ]

    admin1_val <- if ("admin1" %in% names(row)) row$admin1 else NA_character_
    query      <- make_query(row$facility_name, admin1_val, country_name)

    # Check cache first
    hit <- cache[cache$query == query, ]
    if (nrow(hit) > 0) {
      if (!is.na(hit$result_lat[1])) {
        df$latitude[i]          <- hit$result_lat[1]
        df$longitude[i]         <- hit$result_lon[1]
        df$coordinate_source[i] <- "OSM"
        n_cached <- n_cached + 1
      } else {
        n_failed <- n_failed + 1
        failures[[length(failures) + 1]] <- row
      }
      next
    }

    # API call + rate limit
    Sys.sleep(1)
    result <- nominatim_lookup(query)

    if (!is.null(result)) {
      df$latitude[i]          <- result$lat
      df$longitude[i]         <- result$lon
      df$coordinate_source[i] <- "OSM"
      n_geocoded <- n_geocoded + 1

      cache <- rbind(cache, data.frame(
        query       = query,
        result_lat  = result$lat,
        result_lon  = result$lon,
        result_type = result$type,
        stringsAsFactors = FALSE
      ))
    } else {
      n_failed <- n_failed + 1
      failures[[length(failures) + 1]] <- row

      cache <- rbind(cache, data.frame(
        query       = query,
        result_lat  = NA_real_,
        result_lon  = NA_real_,
        result_type = NA_character_,
        stringsAsFactors = FALSE
      ))
    }
  }

  # ── Persist cache ──────────────────────────────────────────
  readr::write_csv(cache, cache_file)

  # ── Write failures ─────────────────────────────────────────
  if (length(failures) > 0) {
    fail_df <- bind_rows(failures)
    fail_df$country       <- tools::toTitleCase(country_name)
    fail_df$geocode_query <- vapply(
      seq_len(nrow(fail_df)),
      function(j) make_query(
        fail_df$facility_name[j],
        if ("admin1" %in% names(fail_df)) fail_df$admin1[j] else NA,
        country_name
      ),
      character(1)
    )

    out_dir <- dirname(failures_file)
    if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

    append_header <- !file.exists(failures_file)
    readr::write_csv(fail_df, failures_file, append = !append_header)
    message(sprintf("  [geocode] %d failures written to %s", n_failed, failures_file))
  }

  message(sprintf(
    "  [geocode] Results: %d geocoded via API, %d from cache, %d failed",
    n_geocoded, n_cached, n_failed
  ))

  return(df)
}
