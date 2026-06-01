# =============================================================
# build_kenya_crosswalk.R
# AfyaScope ETL — generates crosswalks/ken_services_crosswalk.csv
#
# The KMHFR service taxonomy already has a (category > group >
# detail) shape — group always equals category, so the mapping
# from raw to canonical is driven primarily by category.
#
# This script reads the distinct (category, group, detail) triples
# from the raw services file, applies a category-level rule table
# below, and writes the crosswalk CSV the standardisation step
# expects.
#
# Run from repo root:
#   Rscript scripts/extraction/build_kenya_crosswalk.R
# =============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tibble)
  library(stringr)
  library(purrr)
})

raw_path <- "data/raw/kenya/kenya_services_raw.csv"
out_path <- "crosswalks/ken_services_crosswalk.csv"

# ── Category-level mapping rules ────────────────────────────
# Each entry: category (verbatim, ALL CAPS as in source) →
#   list(domain, group, is_clinical, is_malaria_related, include)
RULES <- tibble::tribble(
  ~source_category,                                   ~service_domain,                       ~service_group,                              ~is_clinical, ~is_malaria_related, ~include_in_analysis,
  "ACCIDENT AND EMERGENCY CASUALTY SERVICES",         "Clinical Services",                   "Emergency Services",                         TRUE,         FALSE,                TRUE,
  "AMBULATORY SERVICES",                              "Clinical Services",                   "General Clinical Services",                  TRUE,         FALSE,                TRUE,
  "ANTENATAL CARE",                                   "Reproductive & Maternal Health",      "Reproductive & Maternal Health Services",    TRUE,         FALSE,                TRUE,
  "BLOOD SERVICES",                                   "Diagnostic Services",                 "Diagnostic Services",                        TRUE,         FALSE,                TRUE,
  "CANCER SCREENING",                                 "Diagnostic Services",                 "Diagnostic Services",                        TRUE,         FALSE,                TRUE,
  "CENTRAL STERILE SERVICES DEPARTMENT",              "Support Services",                    "Equipment and Infrastructure",               FALSE,        FALSE,                FALSE,
  "CURATIVE SERVICES",                                "Clinical Services",                   "General Clinical Services",                  TRUE,         FALSE,                TRUE,
  "DENTAL SERVICES",                                  "Clinical Services",                   "General Clinical Services",                  TRUE,         FALSE,                TRUE,
  "EMERGENCY PREPAREDNESS",                           "Clinical Services",                   "Emergency Services",                         TRUE,         FALSE,                TRUE,
  "FAMILY PLANNING",                                  "Reproductive & Maternal Health",      "Family Planning",                            TRUE,         FALSE,                TRUE,
  "FORENSIC SERVICES",                                "Community & Preventive Health",       "Health Promotion and Disease Prevention",    FALSE,        FALSE,                FALSE,
  "HIGH DEPENDENCY SERVICES",                         "Clinical Services",                   "General Inpatient (IPD)",                    TRUE,         FALSE,                TRUE,
  "HIV TREATMENT",                                    "HIV/AIDS Services",                   "HIV/AIDS Care and Treatment",                TRUE,         FALSE,                TRUE,
  "HIV/AIDS PREVENTION AND CARE SERVICES",            "HIV/AIDS Services",                   "HIV/AIDS Services",                          TRUE,         FALSE,                TRUE,
  "HIV/AIDS TREATMENT SERVICES",                      "HIV/AIDS Services",                   "HIV/AIDS Care and Treatment",                TRUE,         FALSE,                TRUE,
  "HOSPICE SERVICE",                                  "Clinical Services",                   "General Clinical Services",                  TRUE,         FALSE,                TRUE,
  "ICU SERVICES",                                     "Clinical Services",                   "General Inpatient (IPD)",                    TRUE,         FALSE,                TRUE,
  "IMMUNISATION",                                     "Community & Preventive Health",       "Immunisation/Vaccination",                   TRUE,         FALSE,                TRUE,
  "INPATIENT SERVICES",                               "Clinical Services",                   "General Inpatient (IPD)",                    TRUE,         FALSE,                TRUE,
  "INTEGRATED MANAGEMENT OF CHILDHOOD ILLNESS",       "Community & Preventive Health",       "Growth Monitoring & Nutrition",              TRUE,         FALSE,                TRUE,
  "INTERGRATED IMMUNIZATION",                         "Community & Preventive Health",       "Immunisation/Vaccination",                   TRUE,         FALSE,                TRUE,
  "LABORATORY SERVICES",                              "Diagnostic Services",                 "Diagnostic Services",                        TRUE,         FALSE,                TRUE,
  "LEPROSY DIAGNOSIS",                                "Clinical Services",                   "General Clinical Services",                  TRUE,         FALSE,                TRUE,
  "LEPROSY TREATMENT",                                "Clinical Services",                   "General Clinical Services",                  TRUE,         FALSE,                TRUE,
  "MATERNITY SERVICES",                               "Reproductive & Maternal Health",      "Reproductive & Maternal Health Services",    TRUE,         FALSE,                TRUE,
  "MENTAL HEALTH SERVICES",                           "Clinical Services",                   "General Clinical Services",                  TRUE,         FALSE,                TRUE,
  "MORTUARY SERVICES",                                "Support Services",                    "Support Services",                           FALSE,        FALSE,                FALSE,
  "NEWBORN CARE SERVICE",                             "Reproductive & Maternal Health",      "Reproductive & Maternal Health Services",    TRUE,         FALSE,                TRUE,
  "NHIF ACCREDITATION STATUS",                        "Support Services",                    "Support Services",                           FALSE,        FALSE,                FALSE,
  "NUTRITION SERVICES",                               "Community & Preventive Health",       "Growth Monitoring & Nutrition",              TRUE,         FALSE,                TRUE,
  "OCCUPATIONAL THERAPY",                             "Clinical Services",                   "General Clinical Services",                  TRUE,         FALSE,                TRUE,
  "ONCOLOGY SERVICES",                                "Clinical Services",                   "General Clinical Services",                  TRUE,         FALSE,                TRUE,
  "OPHTHALMIC SERVICES",                              "Clinical Services",                   "General Clinical Services",                  TRUE,         FALSE,                TRUE,
  "OPHTHALMIC SERVICES1",                             "Clinical Services",                   "General Clinical Services",                  TRUE,         FALSE,                TRUE,
  "ORGAN TRANSPLANT",                                 "Clinical Services",                   "Surgical Services",                          TRUE,         FALSE,                TRUE,
  "ORTHOPAEDIC TECHNOLOGY SERVICES",                  "Clinical Services",                   "General Clinical Services",                  TRUE,         FALSE,                TRUE,
  "PHARMACY SERVICES",                                "Support Services",                    "Therapeutics and Pharmacy",                  FALSE,        FALSE,                FALSE,
  "PHYSIO THERAPY",                                   "Clinical Services",                   "General Clinical Services",                  TRUE,         FALSE,                TRUE,
  "POSTNATAL CARE SERVICES",                          "Reproductive & Maternal Health",      "Reproductive & Maternal Health Services",    TRUE,         FALSE,                TRUE,
  "RADIOLOGY AND IMAGING",                            "Diagnostic Services",                 "Radiology Services",                         TRUE,         FALSE,                TRUE,
  "REHABILITATION SERVICES",                          "Clinical Services",                   "General Clinical Services",                  TRUE,         FALSE,                TRUE,
  "RENAL SERVICES",                                   "Clinical Services",                   "General Clinical Services",                  TRUE,         FALSE,                TRUE,
  "SERVICES FOR GENDER BASED VIOLENCE SURVIVORS",     "Community & Preventive Health",       "Health Promotion and Disease Prevention",    TRUE,         FALSE,                TRUE,
  "SPECIALIZED IN-PATIENT SERVICES",                  "Clinical Services",                   "General Inpatient (IPD)",                    TRUE,         FALSE,                TRUE,
  "SPECIALIZED OUTPATIENTS CLINIC",                   "Clinical Services",                   "General Clinical Services",                  TRUE,         FALSE,                TRUE,
  "STAND ALONE FACILITY REGULATION",                  "Support Services",                    "Support Services",                           FALSE,        FALSE,                FALSE,
  "THEATRE SERVICES",                                 "Clinical Services",                   "Surgical Services",                          TRUE,         FALSE,                TRUE,
  "TUBERCULOSIS DIAGNOSIS",                           "Clinical Services",                   "TB Services",                                TRUE,         FALSE,                TRUE,
  "TUBERCULOSIS TREATMENTS",                          "Clinical Services",                   "TB Services",                                TRUE,         FALSE,                TRUE,
  "YOUTH FRIENDLY SERVICES",                          "Community & Preventive Health",       "Health Promotion and Disease Prevention",    TRUE,         FALSE,                TRUE
)

# ── Pull distinct triples from the raw services file ────────
message("Reading ", raw_path, " ...")
raw <- readr::read_csv(raw_path, show_col_types = FALSE)
message(sprintf("Raw rows: %d", nrow(raw)))

distinct_triples <- raw |>
  dplyr::filter(!is.na(service_category), !is.na(service_detail)) |>
  dplyr::distinct(service_category, service_detail) |>
  dplyr::arrange(service_category, service_detail)
message(sprintf("Distinct (category, detail) pairs: %d",
                nrow(distinct_triples)))

# ── Detect malaria-related details (regardless of category) ─
# Kenya has no explicit Malaria Services category; check details
# for explicit malaria-treatment / diagnosis wording.
is_malaria <- function(detail) {
  stringr::str_detect(tolower(detail),
                      "malaria|mrdt|rdt for malaria|act ")
}

# ── Join + emit crosswalk ───────────────────────────────────
cw <- distinct_triples |>
  dplyr::left_join(RULES, by = c("service_category" = "source_category")) |>
  dplyr::mutate(
    source_category      = service_category,
    source_service_name  = service_detail,
    service_name         = service_detail,           # keep raw detail as canonical name
    needs_review         = is.na(service_domain),
    # Bump any malaria-flagged details
    is_malaria_related   = is_malaria_related | is_malaria(service_detail),
    service_domain       = dplyr::if_else(is_malaria(service_detail),
                                          "Malaria Services", service_domain),
    service_group        = dplyr::if_else(is_malaria(service_detail),
                                          "Malaria Diagnosis and Treatment",
                                          service_group)
  ) |>
  dplyr::select(
    source_category, source_service_name,
    service_domain, service_group, service_name,
    is_clinical, is_malaria_related, include_in_analysis, needs_review
  )

unmatched <- cw |> dplyr::filter(is.na(service_domain))
if (nrow(unmatched) > 0) {
  message("UNMATCHED categories (need rule):")
  print(unmatched |> dplyr::distinct(source_category))
}

readr::write_csv(cw, out_path)
message(sprintf("Wrote %d crosswalk rows -> %s", nrow(cw), out_path))

cat("\nDomain distribution:\n")
print(cw |> dplyr::count(service_domain, sort = TRUE))
cat("\nFlag distribution:\n")
print(cw |> dplyr::count(include_in_analysis))
