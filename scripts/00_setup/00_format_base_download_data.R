# Author: Saeesh Mangwani
# Date: 2022-04-10

# Description: Cleaning column names within the base downloads of data to ease
# integration with postgres

# ==== Libraries ====
library(purrr)
library(readr)
library(stringr)
library(dplyr)

# ==== Cleaning names and rewriting ====

# Cleaning names in the daily files
fnames <- list.files(file.path(base_data_path, 'daily'), full.names = T)
walk(fnames, ~{
  print(.x)
  # Reading
  dat <- read_csv(.x, col_types = cols(.default = "c"))
  # Cleaning names
  names <- names(dat) %>% 
    str_remove_all(., '\\(.*\\)') %>% 
    str_remove_all(., '\\.') %>% 
    str_trim(.) %>% 
    str_replace_all(., ' ', '_') %>% 
    tolower(.)
  print(names)
  # Resetting names
  names(dat) <- names
  # Resetting variable order
  dat <- dat %>% select(sort(tidyselect::peek_vars()))
  # writing back to disk
  write_csv(dat, .x)
})

# Cleaning names in the hourly files
fnames <- list.files(file.path(base_data_path, 'hourly'), full.names = T)
walk(fnames, ~{
  print(.x)
  # Reading
  dat <- read_csv(.x, col_types = cols(.default = "c"))
  # Cleaning names
  names <- names(dat) %>% 
    str_remove_all(., '\\(.*\\)') %>% 
    str_remove_all(., '\\.') %>% 
    str_trim(.) %>% 
    str_replace_all(., ' ', '_') %>% 
    tolower(.)
  print(names)
  # Resetting names
  names(dat) <- names
  # Resetting variable order
  dat <- dat %>% select(sort(tidyselect::peek_vars()))
  # writing back to disk
  write_csv(dat, .x)
})
