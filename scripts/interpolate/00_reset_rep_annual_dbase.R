# !usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2021-08-12

# Description: Creating a database containing a "representative" annual data
# pattern for each station, by aggregating data by day for each station across
# its historical record

# ==== Loading libraries ====
library(readr)
library(lubridate)
library(purrr)
library(dplyr)
library(tidyr)
library(DBI)
library(RPostgres)

# ==== Reading data ====

# Metadata file
stations <- read_csv('data/station_list_BC.csv')

# Connecting to database
conn <- dbConnect(drv = RPostgres::Postgres(),
                  dbname = 'gws',
                  host = '10.0.1.83',
                  port = '5432',
                  user = 'matt', 
                  password = 'Gws2005')

# ==== Database of representative annual (daily) data by station ====

dbExecute(conn, 'drop table if exists ecclimate.precip_representative_year')

# Getting the station IDS as a vector
stat_ids <- stations$`Station ID`

# For each station, reading it's daily data and getting a representative year of
# data
for (stat in stat_ids) {
  # Status update
  print(stat)
  # Reading daily data
  dat <- dbGetQuery(conn, 
                    paste0("select * from ecclimate.daily where ec_station_id = ", 
                    "'", stat, "'"))
  # If this station contains no precip data, skipping:
  if ( all(is.na(dat$total_precip)) ){
    print(paste("Skipping station", stat, 'as it has no precip data'))
    next
  }else{
    # Saving station name and id as vectors
    name <- unique(dat$station_name)
    climate_id <- unique(dat$climate_id)
    
    # Creating a representative year dataframe for this station
    rep_year <- dat %>% 
      # Creating a combined month-day column so we can aggregate by month-day
      unite('index', month, day, sep = '-', remove = F) %>% 
      # Selecting only relevant columns
      select(index, total_precip) %>% 
      # COnverting data to numeric
      mutate(across(!'index', as.numeric)) %>% 
      # Grouping by month-day
      group_by(index) %>% 
      # Getting a groupwise mean - i.e meaned data per year
      summarize(across(everything(), 
                       list('mean' = function(x) {mean(x, na.rm = T)}, 
                            'n' = function(x) {length(na.omit(x))}),
                       .names = '{.fn}_{.col}'))
    
    # Adding station identification columns
    rep_year$station_name <- name
    rep_year$ec_station_id <- stat
    
    # Overwrite if this is the first station of the list, otherwise append
    overwrite <- ifelse(stat == stat_ids[1], T, F)
    
    # Writing to table
    dbWriteTable(conn, SQL('ecclimate.precip_representative_year'), rep_year,
                 overwrite = overwrite, append = !overwrite)
    
  }
}
