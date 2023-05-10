# !usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2021-08-16

# Description: A script that generates a database containing a 'nearness' score
# between each active station and all other inactive stations in BC, where
# nearness is scored as a weighted sum of the distance between the 2 stations
# (25% weight) and the correlation between their respective average annual
# rainfall patterns (75%)

# Time
start <- Sys.time()

# ==== Loading libraries ====
library(sf)
library(dplyr)
library(readr)
library(furrr)
library(purrr)
library(DBI)
library(RPostgres)

# ==== Reading data ====

# Metadata file
stations <- read.csv('metadata/EC_station_metadata.csv', skip = 2)
stations <- st_as_sf(stations, coords = c(8,7), crs = 4326)

# Reading creditials specified by user
creds <- fromJSON(file = 'options/credentials.json')

# Opening connection to postgres database
conn <- dbConnect(drv = RPostgres::Postgres(), 
                  host = creds$host, dbname = creds$dbname, 
                  user = creds$user, password = creds$password)

# Reading the representative precip year database
rep_year_db <- dbGetQuery(conn, 'select * from climate.precip_representative_year') %>% 
  select(index, ec_station_id, mean_total_precip)

# ==== Identifying active stations ====

# Identifying active stations for daily data
stations <- stations %>% 
  mutate(across(contains('Year'), as.integer)) %>% 
  mutate(active = `DLY.Last.Year` >= 2021) %>% 
  # Filtering only stations with precip data (only stations that have a
  # representative year)
  filter(Station.ID %in% unique(rep_year_db$ec_station_id))

# Getting a vector of active station ids/ids we're interested in 
active_ids <- dbGetQuery(conn, 'select distinct ec_station_id from climate.precip_daily_active')[,1]
active_ids <- setNames(active_ids, active_ids)

# ==== Getting composite weights between active and inactive stations ====

# Dropping the table if it already exists
dbExecute(conn, 'drop table if exists climate.precip_pairwise_weights')

# Setting a plan for parellel processing 
plan('multisession')

walk(active_ids, ~{
  print(paste0('Processing active station ', .x))
  
  # Getting all other stations
  stat <- stations %>% filter(Station.ID == .x)
  active_id <- stat$Station.ID
  
  # Getting a vector of distances between this station and all others
  dist <- st_distance(stat, 
                      filter(stations, Station.ID != .x), 
                      by_element = T) %>% 
    units::drop_units() %>% 
    setNames(pull(filter(stations, Station.ID != .x), Station.ID))
  # Standardizing to 0/1, and subtracting from 1 so that closer areas are
  # assigned higher weights
  dist_stand <- 1 - ((dist - min(dist))/(max(dist) - min(dist)))
  
  # Arranging by distance,and selecting only the nearest 50 stations to be used
  # for pairwise comparison
  near_stations <- head(sort(dist_stand, decreasing = T), 300)
  
  # Getting the representative precipitation year for this station, arranged by
  # the date
  ryear <- rep_year_db %>% 
    filter(ec_station_id == stat$Station.ID) %>% 
    arrange(index)
  
  # If there are no data points, skipping
  if(nrow(ryear) == 0) {
    print(paste("Skipping station", .x, "as it has not precipitation data"))
    return()
  }
  
  # Removing leap year date if present, so there are no correlation issues
  if(nrow(ryear) == 366) ryear <- ryear %>% filter(index != '2-29')
  
  # The vector of rainfall values for this year
  ppVals <- as.numeric(ryear$mean_total_precip)
  
  # For each of the inactive stations
  out_df <- future_map_dfr(names(near_stations), ~{
    tryCatch({
      print(.x)
      # Getting the rep year data for this station arranged by index
      rep_year <- rep_year_db %>% 
        filter(ec_station_id == .x) %>% 
        arrange(index)
      
      # If there are none, skipping
      if(nrow(rep_year) == 0) stop('Error: No representative year data available for this station')
      
      # Removing leap year data
      if(nrow(rep_year) == 366) rep_year <- rep_year %>% filter(index != '2-29')
      
      # Getting rainfall values
      ppInactive <- as.numeric(rep_year$mean_total_precip)
      
      # Getting a completeness factor that will be used to weight the resulting
      # correlation
      completeness <- 1 - (sum(is.na(ppInactive))/length(ppInactive))
      
      # If there is no data at all for precip:
      if (completeness == 0) {
        # Setting the correlation weight to also be 0
        pwcor <- completeness
      }else{
        # Calculating the correlation
        pwcor <- cor(ppVals, ppInactive, 
                     use = 'pairwise.complete.obs', 
                     method = 'pearson')
        
        # Weighting correlation by completeness
        pwcor <- pwcor*completeness
      }
    
      # Creating an output list that will be combined to a dataframe
      list('active_station' = active_id, 
           'station_id' = .x, 
           'corr_weight' = pwcor, 
           'rep_year_completeness' = completeness,
           'dist_weight' = dist_stand[.x] )
    }, error = function(e){
      print(paste('Error at station', .x))
      print(e)
    })
  })
  # Creating a combined weight using a weighted mean, with a 75% weight on
  # correlation and 25% weight on distance. This is what will be returned
  out_df <- out_df %>% 
    mutate(comp_weight = (0.75*corr_weight) + (0.25*dist_weight)) %>% 
    arrange(desc(comp_weight))
  
  # Writing to table
  dbWriteTable(conn, SQL('climate.precip_pairwise_weights'), 
               out_df,
               overwrite = F, append = T)
})

# Closing
plan('sequential')
dbDisconnect(conn)
Sys.time() - start
