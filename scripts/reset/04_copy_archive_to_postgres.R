# !usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2021-09-06

# Description: Resetting the EC climate database in postgres. Drops existing
# tables, re-constructs database with correct variable and type formatting and
# re-copies all historical EC data from a base set of downloaded CSVs.
# Historical data are available, pre-downloaded, until 2021, following which the
# database will need to be updated with any new EC data.

# ==== Loading libraries ====
library(DBI)
library(RPostgres)
library(readr)
library(tibble)
library(stringr)
library(purrr)
library(dplyr)
library(data.table)
library(rjson)
library(optparse)
library(lubridate)

# ==== Initializing option parsing ====
option_list <-  list(
  make_option(c("-m", "--maxdate"), type="character", default=as.character(Sys.Date()), 
              help="A year-month (YYYY-MM) combination indicating the maximum date upto which data has been downloaded through this database initialization. Defaults to the current date [Default= %default]", 
              metavar="character")
)

# Parse any provided options and store them in a list
opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)

# ==== Initializing variables ====

# Reading filepaths from JSON
fpaths <- fromJSON(file = 'options/filepaths.json')

# Reading creditials specified by user
creds <- fromJSON(file = 'options/credentials.json')

# Path to base historical data
base_data_path <- fpaths$base_dn_formatted

# ===== Opening posgres connection =====

conn <- dbConnect(drv = RPostgres::Postgres(), 
                  host = creds$host, dbname = creds$dbname, 
                  user = creds$user, password = creds$password)



# ==== Creating database containers ====

# Daily database ----------

# Setting header names names and types (in sql format)
type_table <- bind_rows(list(name = 'climate_id', type = 'TEXT'),
                        list(name = 'cool_deg_days', type = 'real'),
                        list(name = 'cool_deg_days_flag', type = 'TEXT'),
                        list(name = 'data_quality', type = 'TEXT'),
                        list(name = 'datetime', type = 'DATE'),
                        list(name = 'day', type = 'INT'),
                        list(name = 'dir_of_max_gust', type = 'real'),
                        list(name = 'dir_of_max_gust_flag', type = 'TEXT'),
                        list(name = 'ec_station_id', type = 'TEXT'),
                        list(name = 'heat_deg_days', type = 'real'),
                        list(name = 'heat_deg_days_flag', type = 'TEXT'),
                        list(name = 'latitude', type = 'real'),
                        list(name = 'longitude', type = 'real'),
                        list(name = 'max_temp', type = 'real'),
                        list(name = 'max_temp_flag', type = 'TEXT'),
                        list(name = 'mean_temp', type = 'real'),
                        list(name = 'mean_temp_flag', type = 'TEXT'),
                        list(name = 'min_temp', type = 'real'),
                        list(name = 'min_temp_flag', type = 'TEXT'),
                        list(name = 'month', type = 'INT'),
                        list(name = 'snow_on_grnd', type = 'real'),
                        list(name = 'snow_on_grnd_flag', type = 'TEXT'),
                        list(name = 'spd_of_max_gust', type = 'real'),
                        list(name = 'spd_of_max_gust_flag', type = 'TEXT'),
                        list(name = 'station_name', type = 'TEXT'),
                        list(name = 'total_precip', type = 'real'),
                        list(name = 'total_precip_flag', type = 'TEXT'),
                        list(name = 'total_rain', type = 'real'),
                        list(name = 'total_rain_flag', type = 'TEXT'),
                        list(name = 'total_snow', type = 'real'),
                        list(name = 'total_snow_flag', type = 'TEXT'),
                        list(name = 'year', type = 'INT'))

# Creating the sql query that creates the database
str <- ''
for (i in 1:nrow(type_table)){
  newline <- paste(type_table[[i, 1]], type_table[[i, 2]])
  if(i != nrow(type_table)) newline <- paste0(newline, ',')
  str <- paste0(str, newline, '\n')
}

# Formatting query
query <- paste0('create table ', creds$schema, '.daily(', str, ')')
# Creating table in database
dbExecute(conn, query)

# Hourly database ----------

# Setting names and types (in sql format)
type_table <- bind_rows(list(name = 'climate_id', type = 'TEXT'),
          list(name = 'datetime', type = 'DATE'),
          list(name = 'day', type = 'INT'),
          list(name = 'dew_point_temp', type = 'real'),
          list(name = 'dew_point_temp_flag', type = 'TEXT'),
          list(name = 'ec_station_id', type = 'TEXT'),
          list(name = 'hmdx', type = 'real'),
          list(name = 'hmdx_flag', type = 'TEXT'),
          list(name = 'latitude', type = 'real'),
          list(name = 'longitude', type = 'real'),
          list(name = 'month', type = 'INT'),
          list(name = 'precip_amount', type = 'real'),
          list(name = 'precip_amount_flag', type = 'TEXT'),
          list(name = 'rel_hum', type = 'TEXT'),
          list(name = 'rel_hum_flag', type = 'TEXT'),
          list(name = 'station_name', type = 'TEXT'),
          list(name = 'stn_press', type = 'real'),
          list(name = 'stn_press_flag', type = 'TEXT'),
          list(name = 'temp', type = 'real'),
          list(name = 'temp_flag', type = 'TEXT'),
          list(name = 'time', type = 'TIME'),
          list(name = 'visibility', type = 'real'),
          list(name = 'visibility_flag', type = 'TEXT'),
          list(name = 'weather', type = 'TEXT'),
          list(name = 'wind_chill', type = 'real'),
          list(name = 'wind_chill_flag', type = 'TEXT'),
          list(name = 'wind_dir', type = 'real'),
          list(name = 'wind_dir_flag', type = 'TEXT'),
          list(name = 'wind_spd', type = 'real'),
          list(name = 'wind_spd_flag', type = 'TEXT'),
          list(name = 'year', type = 'INT'))

# Creating the sql query that creates the database
str <- ''
for (i in 1:nrow(type_table)){
  newline <- paste(type_table[[i, 1]], type_table[[i, 2]])
  if(i != nrow(type_table)) newline <- paste0(newline, ',')
  str <- paste0(str, newline, '\n')
}

# Formatting query
query <- paste0('CREATE TABLE ', creds$schema, '.hourly(', str, ')')
# Creating table in database
dbExecute(conn, query)

# ==== Copying data to postgres ====

# Daily
fnames <- list.files(file.path(base_data_path, 'daily'), full.names = T)
walk(fnames, ~{
  print(.x)
  # Reading data with the correct type specification
  dat <- read_csv(.x, col_types = 'cdccDidccdcdddcdcdcidcdccdcdcdci')
  # Adding to database
  dbWriteTable(conn, 
               DBI::Id(schema = creds$schema, table = "daily"), 
               dat,
               append = T, 
               overwrite = F)
  # Removing to restart
  rm(dat)
  gc()
})

# Hourly
fnames <- list.files(file.path(base_data_path, 'hourly'), full.names = T)
walk(fnames, ~{
  print(.x)
  # Reading data with the correct type specification
  dat <- read_csv(.x, col_types = 'cTidccdcddidccdcdctdccdcdcdci')
  # Adding to database
  dbWriteTable(conn, 
               DBI::Id(schema = creds$schema, table = "hourly"), 
               dat,
               append = T, 
               overwrite = F)
  # Removing to restart
  rm(dat)
  gc()
})

# ==== Clearing missing data ====

# Due to how the data are stored by EC, the files all contain an additional
# number of empty data rows for all the months of 2021, even though actual data
# has only been recorded until September 2021. These extra rows need to be
# dropped to ensure that complete data is updated on top of this script
dbExecute(conn, paste0(
  'delete from ', creds$schema, '.daily \n',
  'where datetime > \'', opt$maxdate, '\' '
))

dbExecute(conn, paste0(
  'delete from ', creds$schema, '.hourly \n',
  'where year >= ', year(opt$maxdate), 
  ' and month > ', month(opt$maxdate)
))
