# Author: Saeesh Mangwani
# Date: 2022-04-10

# Description: Cleaning column names within the base downloads of data to ease
# integration with postgres

# ==== Libraries ====
library(purrr)
library(readr)
library(stringr)
library(dplyr)
library(data.table)

# Path to base data
base_data_path <- 'data/base_download_formatted'

# ==== Cleaning names and rewriting ====

# Cleaning names in the daily files
fnames <- list.files(file.path(base_data_path, 'daily'), full.names = T)
walk(fnames, ~{
  print(.x)
  # Reading
  dat <- fread(.x, colClasses = 'character')
  # Replacing all missing strings with NAs
  dat %>% mutate(across(everything(), ~{ifelse(.x == '', NA, .x)}))
  # Cleaning names
  names <- names(dat) %>% 
    str_remove_all(., '\\(.*\\)') %>% 
    str_remove_all(., '\\.') %>% 
    str_trim(.) %>% 
    str_replace_all(., ' ', '_') %>% 
    tolower(.)
  # Resetting names
  setnames(dat, names(dat), names)
  # Resetting variable order
  dat <- dat %>% select(sort(tidyselect::peek_vars()))
  # writing back to disk
  fwrite(dat, .x, na = NA)
})

# Cleaning names in the hourly files
fnames <- list.files(file.path(base_data_path, 'hourly'), full.names = T)
walk(fnames, ~{
  print(.x)
  # Reading
  dat <- fread(.x, colClasses = 'character')
  # Replacing all missing strings with NAs
  dat %>% mutate(across(everything(), ~{ifelse(.x == '', NA, .x)}))
  # Cleaning names
  names <- names(dat) %>% 
    str_remove_all(., '\\(.*\\)') %>% 
    str_remove_all(., '\\.') %>% 
    str_trim(.) %>% 
    str_replace_all(., ' ', '_') %>% 
    tolower(.)
  # Resetting names
  setnames(dat, names(dat), names)
  # Resetting variable order
  dat <- dat %>% select(sort(tidyselect::peek_vars()))
  # writing back to disk
  fwrite(dat, .x, na = NA)
})
