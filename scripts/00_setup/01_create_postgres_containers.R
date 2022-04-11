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
library(dplyr)
library(rjson)

# ==== Initializing variables ====

# Reading creditials specified by user
creds <- fromJSON(file = 'scripts/credentials.json')

# Setting default schema unless pre-specified
if (is.null(creds$schema)) creds$schema <- 'precip'

# Opening connection to postgres database
conn <- dbConnect(drv = RPostgres::Postgres(), 
                  host = creds$host, dbname = creds$dbname, 
                  user = creds$user, password = creds$password)


# Path to base historical data
base_data_path <- 'data/base_download'

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
query <- paste0('CREATE TABLE ', creds$schema, '.daily(', str, ')')
# Dropping if exists
dbExecute(conn, paste0('drop table if exists ', creds$schema, '.daily'))
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
# Dropping if exists
dbExecute(conn, paste0('drop table if exists ', creds$schema, '.hourly'))
# Creating table in database
dbExecute(conn, query)

# ==== Calling the bat file that copies the data to postgres ====
system('E:/saeeshProjects/ec-precipitation/scripts/00_setup/create_postgres_dbase.bat')
