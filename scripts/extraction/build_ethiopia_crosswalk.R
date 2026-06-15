# =============================================================
# build_ethiopia_crosswalk.R
# AfyaScope ETL — generates crosswalks/eth_services_crosswalk.csv
#
# Ethiopia's source vocabulary is denser than Kenya/Tanzania's
# (descriptive verbose names like "Integrated Management of newborn
# and Childhood Illness/integrated community case management
# (IMNCI/ICCM)"). We use:
#   1. Category-level defaults (clinical / diagnostic / support /
#      24hr) for the rough domain;
#   2. Name-pattern overrides for items that belong in a different
#      domain than their category suggests (e.g. "ANC" in
#      clinical_services -> Reproductive & Maternal Health).
#
# Run from repo root:
#   Rscript scripts/extraction/build_ethiopia_crosswalk.R
# =============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

raw_path <- "data/raw/ethiopia/ethiopia_services_raw.csv"
out_path <- "crosswalks/eth_services_crosswalk.csv"

# ── Category defaults (applied first) ────────────────────────
CAT_DEFAULTS <- tibble::tribble(
  ~source_category,       ~service_domain,       ~service_group,                ~is_clinical, ~include_in_analysis,
  "clinical_services",    "Clinical Services",   "General Clinical Services",    TRUE,         TRUE,
  "diagnostic_services",  "Diagnostic Services", "Diagnostic Services",          TRUE,         TRUE,
  "support_services",     "Support Services",    "Support Services",             FALSE,        FALSE,
  "twenty_four_hour",     "Support Services",    "Service Hours",                FALSE,        FALSE
)

# ── Name-pattern overrides (case-insensitive) ────────────────
# Each rule: regex over service_name -> (domain, group).
# Applied after category defaults; first matching rule wins.
NAME_RULES <- tibble::tribble(
  ~pattern,                                              ~service_domain,                       ~service_group,
  # ── Reproductive & Maternal Health
  "^anc\\b|antenatal|^pnc\\b|post.?partum|postnatal",     "Reproductive & Maternal Health",      "Reproductive & Maternal Health Services",
  "delivery|cemoc|obstetric|maternal\\s?health|maternity|newborn|neonatal|essential new born", "Reproductive & Maternal Health", "Reproductive & Maternal Health Services",
  "family planning|pregnancy planning|pre conception",   "Reproductive & Maternal Health",      "Family Planning",
  "abortion",                                            "Reproductive & Maternal Health",      "Reproductive & Maternal Health Services",
  "adolescent|youth",                                    "Reproductive & Maternal Health",      "Reproductive & Maternal Health Services",
  "cervical cancer",                                     "Reproductive & Maternal Health",      "Reproductive & Maternal Health Services",
  "mch\\b|reproductive, maternal",                       "Reproductive & Maternal Health",      "Reproductive & Maternal Health Services",

  # ── HIV / AIDS
  "hiv|aids|viral load|cd4 count|prep|art\\b",           "HIV/AIDS Services",                   "HIV/AIDS Services",

  # ── TB
  "\\btb\\b|tuberculosis|afb stain",                     "Clinical Services",                   "TB Services",

  # ── Malaria
  "malaria|mrdt",                                        "Malaria Services",                    "Malaria Diagnosis and Treatment",

  # ── Immunisation
  "immuniz|immunis|vaccin",                              "Community & Preventive Health",       "Immunisation/Vaccination",

  # ── Growth / Nutrition / IMNCI
  "imnci|iccm|growth monitor|otp\\b|outpatient therapeutic|sick baby|under five|malnutrition|imci", "Community & Preventive Health", "Growth Monitoring & Nutrition",

  # ── Health promotion / community / consultation
  "health promotion|healthy living|infant feeding|community.based|venerology|sti\\b|ntd\\b", "Community & Preventive Health", "Health Promotion and Disease Prevention",

  # ── Pharmacy / therapeutics
  "pharma|medicine preparation|compounding",             "Support Services",                    "Therapeutics and Pharmacy",

  # ── Surgical
  "surger|surgical|anesth|anaesth|exodontia|chemotherapy|lithotripsy|transplant|interventional|endoscop|stent|cathet|colonoscop|urethroscop", "Clinical Services", "Surgical Services",

  # ── Emergency
  "emergency|first aid|ambulance|24 hours emergency|24 hours ambulance|cardiac emergency|emergency gynec|emergency nephr|emergency oral", "Clinical Services", "Emergency Services",

  # ── Inpatient / ICU
  "icu|inpatient|admission|nicu|cardiac icu|recovery",   "Clinical Services",                   "General Inpatient (IPD)",

  # ── ENT, Dental, Eye, Mental
  "ent\\b|orl\\b|dental|dentistr|eye|ophthalm|optometry|visual|slit lamp|keratometry|colour test", "Clinical Services", "ENT Services",
  "mental|psychiatr|psycholog|ect\\b|substance abuse|drug dependency",  "Clinical Services", "General Clinical Services",

  # ── Radiology (in diagnostic category)
  "x.?ray|ultrasound|mri|magnetic resonance|computer tomograph|\\bct\\b|fluoroscop|mammograph|nuclear medicine|echocardiograph|radio|imaging",
                                                         "Diagnostic Services",                 "Radiology Services",

  # ── Laboratory specifics
  "bacteriolog|microbiolog|mycolog|fungal culture|parasitolog|virolog|serolog|hematolog|haematolog|chemistr|coagulation|cd4|viral load|biochem|fertility tests|liver function|renal function|lipid profile|thyroid|cytology|pathology|biopsy|histo|electrolyte|electrophoresis|reticulocyte|semen|tumor markers|gram stain|esr|coomb|drug test|allergy|covid|diabetic tests|peripheral morphology",
                                                         "Diagnostic Services",                 "Laboratory Services",

  # ── Blood
  "blood transfus|blood group|24 hours blood",           "Diagnostic Services",                 "Diagnostic Services",

  # ── Rehab / physio
  "physiotherap|physical therapy|recreational therapy|vocational rehab|rehabil|occupational therapy|speech therapy", "Clinical Services", "General Clinical Services",

  # ── Dermatology / oncology
  "dermatolog|oncolog|skin|cancer|radiotherapy|brach therapy|laser therapy",
                                                         "Clinical Services",                   "General Clinical Services",

  # ── Diagnostic — fallthrough imaging-ish
  "stress testing|electro.cautery|nerve conduction|electro encephal|eeg",
                                                         "Diagnostic Services",                 "Diagnostic Services",

  # ── Support and 24hr
  "morgue|autopsy|care after death",                     "Support Services",                    "Support Services",
  "24 hours|24 hour ",                                   "Support Services",                    "Service Hours",
  "waste management|infection prevention|housekeep|laundry|food and diary|social work", "Support Services", "Support Services",

  # ── Error catch
  "^error$|^others$",                                    "Support Services",                    "Support Services"
)

apply_rules <- function(category, name) {
  cat_default <- CAT_DEFAULTS |> dplyr::filter(source_category == category)
  domain <- cat_default$service_domain[1]
  group  <- cat_default$service_group[1]
  is_clin <- cat_default$is_clinical[1]
  inc <- cat_default$include_in_analysis[1]

  # Apply first matching name rule
  nm <- tolower(name)
  for (i in seq_len(nrow(NAME_RULES))) {
    if (stringr::str_detect(nm, stringr::regex(NAME_RULES$pattern[i], ignore_case = TRUE))) {
      domain <- NAME_RULES$service_domain[i]
      group  <- NAME_RULES$service_group[i]
      # Refine is_clinical / include based on domain
      if (domain == "Support Services") {
        is_clin <- FALSE
        inc     <- FALSE
      } else if (domain == "Diagnostic Services" || domain == "Clinical Services" ||
                 domain == "Reproductive & Maternal Health" || domain == "HIV/AIDS Services" ||
                 domain == "Malaria Services") {
        is_clin <- TRUE
        inc     <- TRUE
      } else {
        is_clin <- TRUE
        inc     <- TRUE
      }
      break
    }
  }

  list(domain = domain, group = group,
       is_clinical = is_clin, include = inc)
}

# ── Build crosswalk ──────────────────────────────────────────
message("Reading ", raw_path, " ...")
raw <- read_csv(raw_path, show_col_types = FALSE)

distinct_pairs <- raw |>
  dplyr::distinct(service_category, service_name) |>
  dplyr::arrange(service_category, service_name)
message(sprintf("Distinct (category, name) pairs: %d",
                nrow(distinct_pairs)))

cw <- distinct_pairs |>
  dplyr::rowwise() |>
  dplyr::mutate(
    .map = list(apply_rules(service_category, service_name)),
    service_domain      = .map$domain,
    service_group       = .map$group,
    is_clinical         = .map$is_clinical,
    include_in_analysis = .map$include
  ) |>
  dplyr::ungroup() |>
  dplyr::mutate(
    source_category      = service_category,
    source_service_name  = service_name,
    # Treat "Error" / "Others" rows as review-needed
    needs_review         = grepl("^error|^others$", service_name, ignore.case = TRUE),
    is_malaria_related   = grepl("malaria|mrdt", service_name, ignore.case = TRUE)
  ) |>
  dplyr::select(
    source_category, source_service_name,
    service_domain, service_group, service_name,
    is_clinical, is_malaria_related, include_in_analysis, needs_review
  )

write_csv(cw, out_path)
message(sprintf("Wrote %d crosswalk rows -> %s", nrow(cw), out_path))

cat("\nDomain distribution:\n")
print(cw |> dplyr::count(service_domain, sort = TRUE))
cat("\nFlag distribution:\n")
print(cw |> dplyr::count(include_in_analysis))
cat("\nReview-needed rows:\n")
print(cw |> dplyr::filter(needs_review))
