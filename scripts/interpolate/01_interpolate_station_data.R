# !usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2022-05-30

# Description: A script that given a station ID and a date-range, interpolates
# any missing data in it's record using nearby station information. The interpolation
# utilizes an 80-20 weighted average of values, with 80% weight based on the
# correlation between representative historical records and 20% being based on
# the distance between the station

# ==== Libraries ====
library(readr)
library(dplyr)
library(purrr)
library(stringr)
library(lubridate)
library(sf)
library(DBI)
library(RPostgres)
source('scripts/interpolate/help_funcs.R')

# ==== User defined options ====

# Dates
start_date <- '2021-09-21'
end_date <- '2022-05-25'

# Path where output files will be stored
out_path <- 'output/interpolation'

# ==== Completing heavy, one-time operations ====

# Connecting to database
conn <- dbConnect(drv = RPostgres::Postgres(),
                  dbname = 'gws',
                  host = '10.0.1.83',
                  port = '5432',
                  user = 'matt', 
                  password = 'Gws2005')

# Getting the representative year table from postgres
rep_year_db <- dbGetQuery(conn, 'select * from climate.precip_representative_year') %>% 
  select(index, ec_station_id, mean_total_precip)

# Reading current daily data for all stations within the given date range
query <- format_simple_query('ecclimate', 'daily', 'datetime', start_date, end_date)
refdat <- dbGetQuery(conn, query)

# Metadata file
stations <- read_csv('data/station_list_BC.csv') %>% 
  st_as_sf(coords = c(8,7), crs = 4326) %>% 
  mutate(across(contains('Year'), as.integer)) %>% 
  # Selecting only "active" stations for interpolation
  mutate(active = `DLY Last Year` >= 2021) %>% 
  # Filtering only stations with precip data (only stations that have a
  # representative year)
  filter(`Station ID` %in% unique(rep_year_db$ec_station_id))

# Getting the station IDs of all the active stations
active_ids <- stations$`Station ID`

# ==== Setting station ID being interpolated ====

# ID of station being interpolated
stat_id <- 51318

# ==== Building weights table ====

# Calculating distance weights --------

# Getting station data as af
stat <- stations %>% filter(`Station ID` == stat_id)

# Calculating distances between the station of interest and all active stations
dist <- as_tibble(
  list(
    'active_station' = pull(filter(stations, `Station ID` != stat_id), 'Station ID'),
    'distance' = st_distance(stat, 
                             # Not including the station itself, if relevant
                             filter(stations, `Station ID` != stat_id),
                             by_element = T) %>% 
      units::drop_units()
))

# Standardizing to 0/1, and subtracting from 1 so that closer areas are
# assigned higher weights
dist <- dist %>% 
  mutate(distance = 1 - ((distance - min(distance))/(max(distance) - min(distance))))

# Calculating precipitation correlation weights --------

# Getting the representative year for the current station, arranged by date
ryear <- rep_year_db %>% 
  filter(ec_station_id == stat_id) %>% 
  arrange(index)

# Removing leap year date if present, so there are no correlation issues
if(nrow(ryear) == 366) ryear <- ryear %>% filter(index != '2-29')

# The vector of rainfall values for this year
ppVals <- as.numeric(ryear$mean_total_precip)

# For each of the remaining active stations
precip <- map_dfr(active_ids[active_ids != stat_id], ~{
  print(.x)
  # Getting the rep year data for this station arranged by index
  rep_year <- rep_year_db %>% 
    filter(ec_station_id == .x) %>% 
    arrange(index)
  
  # If there are none, skipping
  if(nrow(rep_year) == 0) {
    print('No rep year database available for this station')
    return(
      list('base_station' = stat_id, 
           'active_station' = .x, 
           'corr_weight' = NA_real_, 
           'rep_year_completeness' = NA_real_)
      )
    # stop('Error: No representative year data available for this station')
  }
  
  # Removing leap year data
  if(nrow(rep_year) == 366) rep_year <- rep_year %>% filter(index != '2-29')
  
  # Getting rainfall values
  ppStation <- as.numeric(rep_year$mean_total_precip)
  
  # Getting a completeness factor that will be used to weight the resulting
  # correlation
  completeness <- 1 - (sum(is.na(ppStation))/length(ppStation))
  
  # If there is no data at all for precip:
  if (completeness == 0) {
    # Setting the correlation weight to also be 0
    pwcor <- completeness
  }else{
    # Calculating the correlation
    pwcor <- cor(ppVals, ppStation, 
                 use = 'pairwise.complete.obs', 
                 method = 'pearson')
    
    # Weighting correlation by completeness
    pwcor <- pwcor*completeness
  }
  
  # Creating an output list that will be combined to a dataframe
  list('base_station' = stat_id, 
       'active_station' = .x, 
       'corr_weight' = pwcor, 
       'rep_year_completeness' = completeness)
})

# Removing rows where precipitation weights are NA, indicating a lack of
# representation year data or an error in the calculation
precip <- precip %>% filter(!is.na(corr_weight))

# ==== Joining distance and precip weights for the final table ====

# Left joining precip onto the distance table, to ensure that only the closest
# 100 stations are selected
weight_table <- dist %>% 
  rename('dist_weight' = 'distance') %>% 
  left_join(precip, by = 'active_station') %>% 
  # Creating a final weight factor
  mutate(comp_weight = (0.75*corr_weight) + (0.25*dist_weight)) %>% 
  # Removing negative correlations
  filter(comp_weight > 0) %>% 
  # Selecting only the top 50 stations when arranged by weight, to ensure a high
  # degree of matching confidence balanced against computational effort to mean
  # across multiple data points
  arrange(desc(comp_weight)) %>% 
  head(100)
  
# ==== Interpolating station data ====

# Separating data for the station of interest
maindat <- refdat %>% filter(ec_station_id == stat_id)

# Filtering only relevant interpolation stations from the rest
intpdat <- refdat %>% filter(ec_station_id %in% weight_table$active_station)

# Interpolating missing data in the main station dataset
interpolated <- maindat %>% 
  mutate(precip_intp_flag = ifelse(!is.na(total_precip), 'recorded', 'interpolated')) %>% 
  mutate(og_total_precip = total_precip) %>% 
  rowwise() %>% 
  mutate(total_precip = ifelse(
    is.na(og_total_precip),
    calc_intp_val(intpdat, weight_table, currdate = datetime),
    og_total_precip
  ))

# ==== Writing interpolated record to disk ====
write_csv(interpolated, 
          file.path(out_path, 
                    paste0('stat_', stat_id, '_', 
                           str_remove_all(start_date, '-'), '-', 
                           str_remove_all(end_date, '-'), '.csv')))
