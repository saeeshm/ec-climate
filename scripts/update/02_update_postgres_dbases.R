# Author: Saeesh Mangwani
# Date: 2021-09-19

# Description: A script that formats and updates the daily and hourly climate
# databases with the newly downloaded station csvs

# ==== Loading libraries ====
library(dplyr)
library(purrr)
library(stringr)
library(readr)
library(lubridate)
library(rjson)
library(RPostgres)
library(DBI)

# ==== Initializing global variables ====

# Path to recent downloaded data 
download_path <- 'data/download'

# Reading creditials specified by user
creds <- fromJSON(file = 'credentials.json')

# Opening connection to postgres database
conn <- dbConnect(drv = RPostgres::Postgres(), 
                  host = creds$host, dbname = creds$dbname, 
                  user = creds$user, password = creds$password)

# Path to all station directories
dnames <- list.files(download_path, full.names = T) %>% 
  # Naming them by station ID
  setNames(list.files(download_path))

# ==== Station list ====

# Reading the BC stations list (downloaded from the previous script)
station_list <- read_csv('data/station_list_BC.csv')

# Updating the station metadata table in the database
dbWriteTable(conn, 
             DBI::Id(schema = creds$schema, table = "station_metadata"),
             station_list,
             append = F,
             overwrite = T)
# ==== Daily data ====

# Getting filenames for all daily data for each station
fnames <- map(dnames, ~{
  list.files(paste0(.x, '/daily'), full.names = T)
})

# Removing stations where there is no new daily data
fnames <- fnames[map_lgl(fnames, ~{length(.x) > 0})]

# Reading all data tables
daily_tables <- map_depth(fnames, 2,  ~{
  read_csv(.x, col_types = cols(.default = "c"))
})

# Joining individual tables for each station into a single dataframe per
# station
daily_joined <- map(daily_tables, ~{
  map_dfr(.x, invisible)
})

# Adding an identifier column using the station name
daily_joined <- imap(daily_joined, ~{
  .x <- mutate(.x, EC_Station_ID = .y)
})

# Joining all the data into a single dataframe
daily <- map_dfr(daily_joined, invisible) %>%
  mutate(DateTime = ymd(`Date/Time`)) %>% 
  select(-`Date/Time`) %>% 
  # Removing non-numeric characters from the windspeed column
  mutate(across(matches('Spd'), ~ {str_remove_all(.x, '[^\\d]*')})) %>%
  # Sorting all columns in alphabetical order
  select(sort(tidyselect::peek_vars()))

# Cleaning names to be consistent with the database format
short_names <- names(daily) %>% 
  str_remove_all('\\(.*\\)') %>% 
  str_trim() %>% 
  str_replace_all(' ', '_') %>% 
  tolower()
names(daily) <- short_names

# removing all rows that are ahead of the current date (the downloads infill
# empty rows for future dates in the current month that make future updating
# difficult, best to just remove them)
daily <- daily %>% filter(datetime < Sys.Date())

# Getting the date range covered by the new data
dateRange <- range(ymd(daily$datetime))

# Getting any data covering the same date range that is already in the database
curr_data <- dbGetQuery(conn, 
                        paste0('select * from ', creds$schema, '.daily where datetime between ',
                               "'", dateRange[1], "' ",
                               'and ',
                               "'", dateRange[2], "'"))

# Anti-joining to only get data not already present
if(nrow(curr_data) > 0){
  update_data <- anti_join(daily, curr_data, by = c('climate_id', 'datetime'))
}else{
  update_data <- daily
}

# Appending the update rows to the database
dbWriteTable(conn, 
             DBI::Id(schema = creds$schema, table = "daily"), 
             update_data,
             append = T, 
             overwrite = F)

# Cleaning
rm(list = ls()[!ls() %in% c('conn', 'creds', 'dnames', 'download_path')])
gc()

# ==== Hourly ====

# Getting filenames for all hourly data for each station
fnames <- map(dnames, ~{
  list.files(paste0(.x, '/hourly'), full.names = T)
})

# Removing stations where there is no new hourly data
fnames <- fnames[map_lgl(fnames, ~{length(.x) > 0})]

# Reading data for all stations within this data range
hourly_tables <- map_depth(fnames, 2,  ~{
  suppressWarnings(read_csv(.x, col_types = cols(.default = "c")))
})

# Joining individual tables for each station into a single dataframe per
# station
hourly_joined <- map(hourly_tables, ~{
  map_dfr(.x, invisible)
})

# Adding an identifier column using the station name
hourly_joined <- imap(hourly_joined, ~{
  .x <- mutate(.x, EC_Station_ID = .y)
})

# Joining all the data into a single dataframe for this year-range
hourly <- map_dfr(hourly_joined, invisible) %>%
  mutate(DateTime = ymd_hm(`Date/Time (LST)`)) %>% 
  select(-`Date/Time (LST)`) %>% 
  mutate(across(matches('Spd'), ~ {str_remove_all(.x, '[^\\d]*')})) %>%
  select(sort(tidyselect::peek_vars()))

# Renaming for database consistency
short_names <- names(hourly) %>% 
  # Removing all bracketed content
  str_remove_all('\\(.*\\)') %>% 
  # Removing all full stops
  str_remove_all('\\.') %>% 
  # removing whitespace
  str_trim() %>% 
  # Replacing spaces with underscores
  str_replace_all(' ', '_') %>% 
  tolower()
names(hourly) <- short_names

# Removing all rows that are ahead today's date
hourly <- hourly %>% filter(datetime < Sys.Date())

# Getting the date range covered by the new data
dateRange <- range(ymd_hms(hourly$datetime))

# Getting any data covering the same date range that is already in the database
curr_data <- dbGetQuery(conn, 
                        paste0('select * from ', creds$schema, '.hourly where datetime between ',
                               "'", dateRange[1], "' ",
                               'and ',
                               "'", dateRange[2], "'"))

# Anti-joining to only get data not already present
if(nrow(curr_data) > 0){
  update_data <- anti_join(hourly, 
                           curr_data %>% 
                             mutate(datetime = ymd_hms(paste(datetime, time))), 
                           by = c('climate_id', 'datetime'))
}else{
  update_data <- hourly
}


# Appending the update rows to the database
dbWriteTable(conn, 
             DBI::Id(schema = creds$schema, table = "hourly"), 
             update_data,
             append = T, 
             overwrite = F)

# Closing connection
dbDisconnect(conn)
