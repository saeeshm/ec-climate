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

# ==== User variables ====

# Path to batch file for running download
bat_path <- 'scripts/_temp_download_ec_climdata.bat'

# Path to folder that will store downloaded data - creating it if doesn't exist,
# and overwriting if it does to store the new archive download
download_path <- 'data/base_download'
if (!dir.exists(download_path)) {
  dir.create(download_path)
}else{
  unlink(download_path, recursive = T)
  dir.create(download_path)
}

# Path to archive
archive_path <- 'data/archive'
if (!dir.exists(download_path)) dir.create(download_path)

# ==== Download parameters ====

# End date - to get all historical data until today
end_date <- substr(Sys.Date(), 1, 7)

# Indexing vector for provinces
provinces <- c('NL', 'PE', 'NS', 'NB', 'QC', 'ON', 'MB',
               'SK', 'AB', 'BC', 'YT', 'NT', 'NU') %>%
  setNames(c('NEWFOUNDLAND', 'PRINCE EDWARD ISLAND', 'NOVA SCOTIA',
             'NEW BRUNSWICK', 'QUEBEC', 'ONTARIO',
             'MANITOBA', 'SASKATCHEWAN', 'ALBERTA',
             'BRITISH COLUMBIA', 'YUKON TERRITORY', 'NORTHWEST TERRITORIES',
             'NUNAVUT'))
# Province for which to download data - currently set to BC
province <- provinces[10]
# province <- 'BC'

# ==== Downloading the most recent station list ====
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
sink(bat_path)
cat(':: Calling the activation script to run conda')
cat('\n')
cat('call C:\\Users\\OWNER\\miniconda3\\Scripts\\activate.bat')
cat('\n')
cat('\n')
cat(':: Activating the GW environment')
cat('\n')
cat('call conda activate gwenv')
cat('\n')
cat('\n')
cat(':: Navigating to the script home directory')
cat('\n')
cat('e:')
cat('\n')
cat(paste('cd', normalizePath(file.path(getwd(), 'scripts'))))
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
cat('pause')
sink()

# ==== Executing the batch file ====
system(normalizePath(bat_path))

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



