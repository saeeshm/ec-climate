# !usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2022-05-24

# Description: Downloading the historical archive of climate data for BC

# ==== Loading libraries ====
library(dplyr)
library(readr)
library(stringr)
library(lubridate)
library(rjson)
library(DBI)
library(RPostgres)
library(optparse)

# ==== Initializing option parsing ====
option_list <-  list(
  make_option(c("-e", "--enddate"), type="character", default=substr(Sys.Date(), 1, 7), 
              help="A year-month (YYYY-MM) combination indicating the end date for data download. Defaults to the current date [Default= %default]", 
              metavar="character"),
  make_option(c("-p", "--province"), type="character", default='BC', 
              help="Two-letter abbreviation for the province for which data should be gathered. Defaults to BC. It is highly recommended to choose a single province, otherwise the data volume is huge. The metadata for all stations Canada is always downloaded by default, separately from the provincial metadata table  [Default= %default]", 
              metavar="character")
  
)

# Parse any provided options and store them in a list
opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)

# ==== User variables ====

# Reading filepaths from JSON
fpaths <- fromJSON(file = 'options/filepaths.json')

# Path to batch file for running download
bat_path <- ifelse(.Platform$OS.type == 'unix', 
                   'scripts/_temp_download_ec_climdata.sh',
                   'scripts/_temp_download_ec_climdata.bat')

# Path to folder that will store downloaded data - creating it if doesn't exist,
# and overwriting if it does to store the new archive download
download_path <- fpaths$base_download
if (!dir.exists(download_path)) {
  dir.create(download_path)
}else{
  unlink(download_path, recursive = T)
  dir.create(download_path)
}

# Path to archive
archive_path <- fpaths$download_archive
if (!dir.exists(archive_path)) dir.create(archive_path)

# ==== Download parameters ====

# End date to get all historical data until
end_date <- opt$enddate

# Indexing vector for provinces
provinces <- c('NL', 'PE', 'NS', 'NB', 'QC', 'ON', 'MB',
               'SK', 'AB', 'BC', 'YT', 'NT', 'NU') %>%
  setNames(c('NEWFOUNDLAND', 'PRINCE EDWARD ISLAND', 'NOVA SCOTIA',
             'NEW BRUNSWICK', 'QUEBEC', 'ONTARIO',
             'MANITOBA', 'SASKATCHEWAN', 'ALBERTA',
             'BRITISH COLUMBIA', 'YUKON TERRITORY', 'NORTHWEST TERRITORIES',
             'NUNAVUT'))

# Province for which to download data
province <- provinces[provinces == opt$province]

# ==== Downloading the most recent station list ====
print("Downloading EC Station metadata tables...")
station_list <- read_csv(
  'https://drive.google.com/uc?export=download&id=1HDRnj41YBWpMioLPwAFiLlK4SK8NV72C',
  skip = 3,
  col_types = 'ccccccdddddiiiiiiii'
)
# Writing full station list to disk
write_csv(station_list, 'data/station_list_CA.csv', append = F)

# Filtering only for province of interest
station_list <- station_list %>% filter(Province %in% names(province))
# Writing to disk
write_csv(station_list, paste0('data/station_list_', province, '.csv'), 
          append = F)

# ==== Initializing data download ====

# Clearing the download folder of any data if present
unlink(download_path, recursive = T)
dir.create(download_path)

# Creating batch file for calling the download script
if(.Platform$OS.type == 'unix'){
  sink(bat_path)
  cat('#!/bin/sh')
  cat('\n')
  cat('\n')
  cat('# Activating conda environment')
  cat('\n')
  cat(paste('source', fpaths$conda_path))
  cat('\n')
  cat(paste('conda activate', fpaths$conda_env))
  cat('\n')
  cat('\n')
  cat('# Navigating to the script home directory')
  cat('\n')
  cat(paste('cd', normalizePath(file.path(getwd(), 'scripts'))))
  cat('\n')
  cat('\n')
  cat('# Calling the script to download weather data (hourly and daily) for all BC stations')
  cat('\n')
  cat(paste0('python get_canadian_weather_observations.py ', 
             '--hourly --daily ',
             ifelse(!is.null(end_date), paste0('--end-date "', end_date, '" '), ''),
             # '--station-file "E:\\saeeshProjects\\ec-climate-database\\data\\station_list_CA.csv" ',
             '-o "', normalizePath(download_path), '" ',
             province
  ))
  cat('\n')
  cat('\n')
  sink()
}else{
  sink(bat_path)
  cat(':: Calling the activation script to run conda')
  cat('\n')
  cat(paste('call', fpaths$conda_path))
  cat('\n')
  cat('\n')
  cat(':: Activating the GW environment')
  cat('\n')
  cat(paste('call conda activate', fpaths$conda_env))
  cat('\n')
  cat('\n')
  cat(':: Navigating to the script home directory')
  cat('\n')
  cat('\n')
  cat(paste('cd /d', normalizePath(file.path(getwd(), 'scripts'))))
  cat('\n')
  cat('\n')
  cat(':: Calling the script to download weather data (hourly and daily) for all BC stations')
  cat('\n')
  cat(paste0('python get_canadian_weather_observations.py ', 
             '--hourly --daily ',
             ifelse(!is.null(end_date), paste0('--end-date "', end_date, '" '), ''),
             # '--station-file "E:\\saeeshProjects\\ec-climate-database\\data\\station_list_CA.csv" ',
             '-o "', normalizePath(download_path), '" ',
             province
  ))
  cat('\n')
  cat('\n')
  sink()
}

# ==== Executing the batch file ====
if(.Platform$OS.type == 'unix'){
  system(paste('sh', normalizePath(bat_path)))
}else{
  system(normalizePath(bat_path))
}

# ==== Adding download to the archive ====

# Creating the archive folder if needed
if (!dir.exists(archive_path)) dir.create(archive_path)

# Removing the oldest downloads to only keep the most recent 3 --------

# Getting available files and naming them by their datestamps
fnames <- list.files(archive_path, pattern = "\\d{4}-\\d{2}-\\d{2}$") %>% 
  setNames(str_extract(., "\\d{4}-\\d{2}-\\d{2}"))

# Sorting names and selecting only the most recent 3
dates <- names(fnames) %>% 
  ymd() %>% 
  unique() %>%
  sort() %>% 
  tail(3)

# Indexing the list to select only those files dated before these 2
fnames <- fnames[!(ymd(names(fnames)) %in% dates)]

# Removing these files (if there are any to remove)
if (length(fnames) > 0) file.remove(paste0(archive_path, fnames))

# Copying the current download to the archive
file.copy(download_path, archive_path, recursive = T)
file.rename(file.path(archive_path, 'download'), 
            file.path(archive_path, paste0('download_', Sys.Date())))

# Removing the batch file
file.remove(normalizePath(bat_path))




