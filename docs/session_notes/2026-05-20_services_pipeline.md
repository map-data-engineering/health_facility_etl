# Session Notes — 2026-05-20: Facility Services Pipeline Extension

## Summary

Extended AfyaScope ETL v2.0 to capture facility-level service data for Tanzania and Malawi. A new `facility_services` DuckDB table was added alongside the existing `health_facilities` table. The pipeline now runs as 4 steps: country pipelines → facility merge → data lake export → services standardisation + DuckDB load.

---

## New files created

| File | Purpose |
|------|---------|
| `schemas/services_schema.yaml` | 19-field standard schema, 7 permitted service_domains |
| `crosswalks/tz_services_crosswalk.csv` | 173-row TZ lookup (source_category × source_detail → standard hierarchy) |
| `crosswalks/malawi_services_crosswalk.csv` | 385-row Malawi lookup (source_service_name → standard hierarchy) |
| `crosswalks/global_services_crosswalk.csv` | 555-row union of TZ + Malawi crosswalks with country column |
| `R/standardise/standardise_tanzania_services.R` | Standardisation function for Tanzania HF services |
| `R/standardise/standardise_malawi_services.R` | Standardisation function for Malawi HF services |

## Files modified

| File | Change |
|------|--------|
| `pipelines/run_global_pipeline.R` | Added `run_services` param; Step 4 services loop; fixed `_services_standardized.csv` exclusion from facility merge |

---

## Design decisions

### Service domain hierarchy (7 domains)
1. Clinical Services
2. Reproductive & Maternal Health
3. HIV/AIDS Services
4. Malaria Services
5. Diagnostic Services
6. Community & Preventive Health
7. Support Services

### Tanzania `growth_nutrition` overrides
The following `service_detail` values fall under `service_category=growth_nutrition` in the TZ source but are remapped to **Reproductive & Maternal Health** in the crosswalk:
- Antenatal Care / ANC variants
- Family Planning and all FP subtypes (FP-INV, FP-NONINV, etc.)
- CEmOC, BEmOC
- Postnatal Care
- Emergency Contraception
- Cervical Cancer (screening)
- Maternal and Newborn Care
- Post-Abortion Care

Vaccination and Immunisation items remain in **Community & Preventive Health**.

### Malawi commodities/supplies
Any service_name that is clearly a drug, consumable, equipment, or staff training record:
- `include_in_analysis = FALSE`
- `is_clinical = FALSE`
- `service_domain = "Support Services"`

### Service UID format
`{ISO3}-SVC-{zero-padded row number to 6 digits}`  
e.g. `TZA-SVC-000001`, `MWI-SVC-000001`

### Column naming note
`health_facilities` uses columns `country` (full name) and `iso` (ISO3 code).  
`facility_services` uses `country_name` and `country_iso`.  
**Joins between the two tables must always go through `facility_uid = uid`, never through country columns.**

---

## Final DuckDB state (after session)

| Table | Rows |
|-------|------|
| `health_facilities` | 78,265 |
| `facility_services` | 307,954 |

### `health_facilities` breakdown
| Country | Rows |
|---------|------|
| Tanzania | 13,075 |
| Uganda | 8,508 |
| Zambia | 3,731 |
| Nigeria | 51,022 |
| Malawi | 1,929 |

### `facility_services` breakdown
| Country | Rows |
|---------|------|
| Tanzania | 208,941 |
| Malawi | 99,013 |

---

## Key issues resolved

### 1. Tanzania `service_group` column shadowing
Joining the TZ crosswalk on `(service_category, service_group, service_detail)` caused dplyr to keep the raw `service_group` as join key and shadow the crosswalk's standard `service_group`. **Fix:** 2-key join on `(source_category, source_detail)` only; rename crosswalk's `service_group` to `std_group` before join; `mutate(service_group = std_group)` after.

### 2. `bind_rows` type mismatch on `source_category_id`
TZ stores `source_category_id` as character slug (e.g. `"general_services"`); Malawi stores it as numeric double (e.g. `1`). **Fix:** Cast `source_service_id`, `source_type_id`, `source_category_id` all to `as.character()` before `bind_rows`.

### 3. Services CSVs included in facility merge
`_standardized\.csv` pattern matched both `tanzania_standardized.csv` and `tanzania_services_standardized.csv`. **Fix:** Added exclusion filter in Step 2 of `run_global_pipeline.R`.

### 4. Malawi `facility_uid` all NULL on first run
Malawi facilities pipeline had never been run, so `uid_registry.csv` had no MWI rows. **Fix:** Ran `run_country_pipeline("malawi", ...)`, then re-ran Malawi services standardisation. Result: **98.0% match (97,045 / 99,013 records)**.

### 5. 190 Malawi facility_codes not in HF registry
1,968 service records reference facility_codes that do not exist in the Malawi HFR. These are facilities present in the services survey but absent from the HFR registry. **Source data gap, not a pipeline error.** These records have `facility_uid = NA`.

---

## Known pending issues

### Tanzania script naming
`run_global_pipeline.R` looks for `standardise_tanzania_services.R` but the script was originally named `standardise_tz_services.R`. Tanzania services were silently skipped in the Step 4 services loop. **Fix applied this session:** Renamed to `standardise_tanzania_services.R` with function `standardise_tanzania_services()`.

---

## Next steps

- **Connect `afyascope.duckdb` to HealthScape Quarto portal** — `facility_services` table is ready for dashboards; join on `facility_uid = uid`.
- **Run Malawi HFR extraction for remaining countries** (Uganda, Zambia, Botswana, Nigeria) as service data becomes available.
- **Add service scripts for remaining countries**: Each needs `crosswalks/{country}_services_crosswalk.csv` and `R/standardise/standardise_{country}_services.R`.
- **Investigate 190 orphan Malawi facility_codes** — Cross-reference with HMIS register to determine if these are private/informal facilities excluded from the national HFR.
