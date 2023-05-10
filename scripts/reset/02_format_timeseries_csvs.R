# Author: Saeesh Mangwani
# Date: 2021-08-06

# Description: A script that formats daily and hourly climate database csv from
# station csvs

# ==== Loading libraries ====
library(dplyr)
library(purrr)
library(stringr)
library(readr)
library(lubridate)

# ==== Initializing global variables ====

# Reading filepaths from JSON
fpaths <- fromJSON(file = 'options/filepaths.json')

# Path to downloaded data 
download_path <- fpaths$base_download
if (!dir.exists(download_path)) dir.create(download_path)

# Path to output directory
out_path <- fpaths$base_dn_formatted
if (!dir.exists(out_path)) dir.create(out_path)

# Creating container directories if not present
if(!dir.exists(file.path(out_path, 'daily'))) dir.create(file.path(out_path, 'daily'))
if(!dir.exists(file.path(out_path, 'hourly'))) dir.create(file.path(out_path, 'hourly'))

# Path to all station directories
dnames <- list.files(paste0(download_path, '/'), full.names = T) %>% 
  # Naming them by station ID
  setNames(list.files(paste0(download_path, '/')))

# ==== Daily data ====

# Getting filenames for all daily data for each station
fnames <- map(dnames, ~{
  list.files(paste0(.x, '/daily'), full.names = T)
})

# Removing stations where there is no daily data
fnames <- fnames[map_lgl(fnames, ~{length(.x) > 0})]

# Naming each filename with its year
fnames <- map(fnames, ~{
  year_names <- str_match(.x, '_(\\d{4})_P1D.csv')[,2]
  setNames(.x, year_names)
})

# Getting all the unique years for which data exists for each station as a
# vector
years <- map(fnames, ~{unique(names(.x))})

# Getting the earliest and latest dates across all stations
max_year <- max(as.integer(map_chr(years, max, na.rm = T)), na.rm = T)
min_year <- min(as.integer(map_chr(years, min, na.rm = T)), na.rm = T)

# Creating a range between them
year_range <- min_year:max_year

# Creating a vector of 10-year invterval thresholds from this range
n <- length(year_range)
thresholds <- vector('integer', 0)
while (n > 0) {
  # lower limit - resets to 1 if the decrement passes 0, to ensure no out of
  # bounds exception
  low <- ifelse((n - 10) < 0, 1, (n - 10))
  thresholds <- c(thresholds, max(year_range[low:n]))
  n <- n - 10
}

# Ensuring the lowest threshold is the earliest year
thresholds[length(thresholds)] <- min_year

# Setting names
names(thresholds) <- thresholds

# Incrementing the first threshold to ensure no data are missed, since the
# data are sorted into thresholds using 1-sided intervals
thresholds[1] <- thresholds[1] + 1

# Iterating through each 10 year interval
for (i in 1:length(thresholds)) {
  
  # If we've reached the end of the interval list, breaking
  if (is.na(thresholds[i + 1])) {
    print('No more date ranges, ending loop...')
    break
  }else{
    print(paste("Joining data between", 
                names(thresholds[i + 1]), 'and', names(thresholds[i])))
  }
  
  # For each station, filtering only the daily data that falls within this
  # date range
  curr_fnames <- map(fnames, ~{
    year_names <- names(.x)
    .x[year_names >= thresholds[i+1] & year_names < thresholds[i]]
  })
  
  # Removing stations where there is no hourly data in this year-range
  curr_fnames <- curr_fnames[map_lgl(curr_fnames, ~{length(.x) > 0})]
  
  # Reading data for all stations within this data range
  daily_tables <- map_depth(curr_fnames, 2,  ~{
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
  
  # Joining all the data into a single dataframe for this year-range
  daily <- map_dfr(daily_joined, invisible) %>%
    mutate(DateTime = ymd(`Date/Time`)) %>% 
    select(-`Date/Time`) %>% 
    # Removing non-numeric characters from the windspeed column
    mutate(across(matches('Spd'), ~ {str_remove_all(.x, '[^\\d]*')})) %>%
    # Sorting all columns in alphabetical order
    select(sort(tidyselect::peek_vars()))
  
  # Cleaning some problematic columns
  
  # Writing to csv, indicating the date-range covered
  write_csv(daily, paste0(out_path, '/daily/daily_climData_', 
                          names(thresholds[i + 1]),'-', names(thresholds[i]),
                          '.csv'))
}

# Clearing objects
rm(list = ls()[!ls() %in% c('download_path', 'out_path', 'dnames')])
gc()

# ==== Hourly data ====

# Getting filenames for all hourly data for each station
fnames <- map(dnames, ~{
  list.files(paste0(.x, '/hourly'), full.names = T)
})

# Removing stations where there is no hourly data
fnames <- fnames[map_lgl(fnames, ~{length(.x) > 0})]

# Naming each filename with its year
fnames <- map(fnames, ~{
  month_year_names <- str_match(.x, '(\\d{2}-\\d{4})_P1H.csv')[,2]
  setNames(.x, month_year_names)
})

# Getting all the years for which data exists for each station as a vector
years <- map(fnames, ~{unique(str_remove(names(.x), '\\d{2}-'))})

# Getting the earliest and latest dates across all stations
max_year <- max(as.integer(map_chr(years, max, na.rm = T)), na.rm = T)
min_year <- min(as.integer(map_chr(years, min, na.rm = T)), na.rm = T)

# Creating a range between them
year_range <- as.integer(min_year):as.integer(max_year)

# Creating a vector of 5-year invterval thresholds from this range
n <- length(year_range)
thresholds <- vector('integer', 0)
while (n > 0) {
  # lower limit - resets to 1 if the decrement passes 0, to ensure no out of
  # bounds exception
  low <- ifelse((n - 5) < 0, 1, (n - 5))
  thresholds <- c(thresholds, max(year_range[low:n]))
  n <- n - 5
}

# Ensuring the lowest threshold is the earliest year
thresholds[length(thresholds)] <- as.integer(min_year)

# Setting names
names(thresholds) <- thresholds

# Incrementing the first threshold to ensure no data are missed, since the
# data are sorted into thresholds using 1-sided intervals
thresholds[1] <- thresholds[1] + 1

# Iterating through each 5 year interval
for (i in 1:length(thresholds)) {
  
  # If we've reached the end of the interval list, breaking
  if (is.na(thresholds[i + 1])) {
    print('No more date ranges, ending loop...')
    break
  }else{
    print(paste("Joining data between", 
                names(thresholds[i + 1]), 'and', names(thresholds[i])))
  }
  
  # For each station, filtering only the daily data that falls within this
  # date range
  curr_fnames <- map(fnames, ~{
    year_names <- as.integer(str_remove(names(.x), '\\d{2}-'))
    .x[year_names >= thresholds[i+1] & year_names < thresholds[i]]
  })
  
  # Removing stations where there is no hourly data in this year-range
  curr_fnames <- curr_fnames[map_lgl(curr_fnames, ~{length(.x) > 0})]
  
  # Reading data for all stations within this data range
  hourly_tables <- map_depth(curr_fnames, 2,  ~{
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
    mutate(Hmdx = as.numeric(Hmdx)) %>% 
    mutate(`Dew Point Temp (°C)` = as.numeric(`Dew Point Temp (°C)`)) %>% 
    select(sort(tidyselect::peek_vars()))
  
  # Writing to csv, indicating the date-range covered
  write_csv(hourly, paste0(out_path, '/hourly/hourly_climData_', 
                          names(thresholds[i + 1]),'-', names(thresholds[i]),
                          '.csv'))
}
