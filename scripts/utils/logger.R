# =============================================================
# logger.R
# HealthScape ETL — Utilities
#
# Appends a structured log entry to logs/pipeline_log.csv.
# Creates the log file with header if it does not exist.
# =============================================================

log_message <- function(country, step, status, message = "") {

  log_entry <- data.frame(
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    country   = country,
    step      = step,
    status    = status,
    message   = message,
    stringsAsFactors = FALSE
  )

  log_dir  <- "logs"
  log_file <- file.path(log_dir, "pipeline_log.csv")

  if (!dir.exists(log_dir)) dir.create(log_dir, recursive = TRUE)

  write.table(
    log_entry,
    file      = log_file,
    sep       = ",",
    row.names = FALSE,
    col.names = !file.exists(log_file),   # header only on first write
    append    = file.exists(log_file),
    quote     = TRUE
  )
}
