# =============================================================
# install_packages.R
# AfyaScope ETL — one-time dependency installer
#
# Run from the project root:
#   source("install_packages.R")
#   or: Rscript install_packages.R
# =============================================================

pkgs <- c(
  # Core pipeline (likely already present)
  "dplyr", "readr", "readxl", "yaml", "lubridate",
  # Geospatial — boundary validation
  "sf", "terra", "geodata",
  # Geocoding fallback
  "httr2", "jsonlite",
  # Phase 2 data lake
  "arrow", "duckdb", "DBI"
)

already   <- pkgs[pkgs %in% rownames(installed.packages())]
to_install <- setdiff(pkgs, already)

if (length(already) > 0)
  message("Already installed: ", paste(already, collapse = ", "))

if (length(to_install) == 0) {
  message("All packages are already installed.")
} else {
  message("Installing: ", paste(to_install, collapse = ", "))
  install.packages(to_install, repos = "https://cloud.r-project.org", dependencies = TRUE)
}

# Verify everything loads
message("\nVerifying installs...")
failed <- character()
for (p in pkgs) {
  ok <- requireNamespace(p, quietly = TRUE)
  cat(sprintf("  %-12s %s\n", p, if (ok) "OK" else "FAILED"))
  if (!ok) failed <- c(failed, p)
}

if (length(failed) > 0) {
  warning("These packages failed to load: ", paste(failed, collapse = ", "))
} else {
  message("\nAll packages ready.")
}
