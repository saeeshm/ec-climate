# Author: Saeesh Mangwani
# Date: 2022-05-30

# Description: Helper functions for posting databases to Depth2water

# ==== Libraries ====

# ==== Helper functions ====

# Formats a simple get request involving date filtering
format_simple_query <- function(schema, table, date_col = NULL, start_date = NULL, end_date = NULL){
  # Base query
  query <- paste0('select * from ', schema, '.', table)
  # If dates are given
  if(!is.null(start_date) & !is.null(end_date)){
    query <- paste0(
      query, '\n',
      'where "', date_col, '" >= \'', start_date, '\'',
      ' and "', date_col, '" <= \'', end_date, '\''
    )
  }else if(!is.null(start_date)){
    query <- paste0(
      query, '\n',
      'where "', date_col, '" >= \'', start_date, '\''
    )
  }else if(!is.null(end_date)){
    query <- paste0(
      query, '\n',
      'where "', date_col, '" <= \'', end_date, '\''
    )
  }
  return(query)
}


# Returns the interpolated precipitation value given a reference dataset, a
# weight table and a date
calc_intp_val <- function(intpdat, weight_table, currdate){
  print(paste('Interpolation precipitation record for date:', currdate))
  # Getting all data from the reference table at the given date
  dat <- intpdat %>% 
    select(ec_station_id, datetime, total_precip) %>% 
    tibble() %>% 
    filter(datetime == ymd(currdate))
  
  # Joining the weights table, and filtering stations where either the weights
  # are NA (not selectable for interpolation) or the precipitation is NA (no
  # data, can't interpolate)
  intp_val <- dat %>% 
    left_join(weight_table %>% 
                mutate(active_station = as.character(active_station)), 
              by=c('ec_station_id' = 'active_station')) %>% 
    # Removing missing weights and missing precipt
    filter(!is.na(comp_weight)) %>% 
    filter(!is.na(total_precip)) %>% 
    # Calculating the weighted precip per station
    mutate(wt_precip = total_precip * comp_weight) %>% 
    # Summarizing the final weighted mean precip value
    summarize(intp_val = sum(wt_precip)/sum(comp_weight)) %>% 
    pull(intp_val)
  
  return(intp_val)
}
