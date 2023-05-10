# !usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2022-05-24

# Description: Setup parameters for creating a postgres database hosting EC
# Climate data

# ==== Loading libraries ====
library(DBI)
library(RPostgres)
library(rjson)

# ==== Resetting database containers ====

# Reading credentials specified by user
creds <- fromJSON(file = 'options/credentials.json')

print('Opening database connection...')
conn <- dbConnect(drv = RPostgres::Postgres(), 
                  host = creds$host, dbname = creds$dbname, 
                  user = creds$user, password = creds$password)

print('Resetting EC Climate schema...')
# Dropping tables and ecclimate schema if present
dbExecute(conn, paste0('drop table if exists ', creds$schema, '.daily'))
dbExecute(conn, paste0('drop table if exists ', creds$schema, '.hourly'))
dbExecute(conn, paste0('drop table if exists ', creds$schema, '.station_metadata'))
dbExecute(conn, paste0('drop table if exists ', creds$schema, '.precip_representative_year'))
dbExecute(conn, paste0('drop schema if exists ', creds$schema))
# Creating schema and granting permissions
dbExecute(conn, paste0('create schema ', creds$schema))
dbExecute(conn, paste0('grant all on schema ', creds$schema, 
                       ' to postgres, ', creds$user))

print('Closing connection...')
dbDisconnect(conn)

print('EC Climate Schema reset')
