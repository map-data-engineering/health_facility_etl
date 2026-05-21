library(DBI)
library(duckdb)

# Connect
con <- dbConnect(duckdb(), 
                 "data/processed/global_master/afyascope.duckdb",
                 read_only = TRUE)

# Check what tables exist
dbListTables(con)

# Read facility services
services <- dbGetQuery(con, "SELECT * FROM facility_services")

# Quick look
nrow(services)
head(services)
glimpse(services)


# Row counts by country
dbGetQuery(con, "
  SELECT country_name, COUNT(*) as records
  FROM facility_services
  GROUP BY country_name
")

# Domain distribution
dbGetQuery(con, "
  SELECT service_domain, COUNT(*) as n
  FROM facility_services
  WHERE include_in_analysis = TRUE
  GROUP BY service_domain
  ORDER BY n DESC
")

# Malaria services only
dbGetQuery(con, "
  SELECT country_name, service_group, 
         service_name, COUNT(*) as facilities
  FROM facility_services
  WHERE is_malaria_related = TRUE
    AND include_in_analysis = TRUE
  GROUP BY country_name, service_group, service_name
  ORDER BY country_name, facilities DESC
")

# Join services with facility demographics
dbGetQuery(con, "
  SELECT 
    hf.facility_name,
    hf.latitude,
    hf.longitude,
    hf.country,
    s.service_name,
    s.service_domain
  FROM health_facilities hf
  JOIN facility_services s 
    ON hf.uid = s.facility_uid
  WHERE s.is_malaria_related = TRUE
  LIMIT 20
") 
# Disconnect when done
dbDisconnect(con, shutdown = TRUE)