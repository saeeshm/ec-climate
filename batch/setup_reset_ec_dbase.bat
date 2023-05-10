:: Navigating to project directory
cd /d E:\saeeshProjects\databases\ec-climate

:: Setting up the postgres database
Rscript scripts\reset\00_dbase_setup.R

:: Downloading the provincial (BC) archive until today. This takes a LONG time. 
:: You can change the max data that data is downloaded with an -e flag (eg: -e 2023-01)
Rscript scripts\reset\01_download_bc_archive.R -p BC

:: Formatting the downladed data as timeseries CSVs. This also takes a LONG time.
Rscript scripts\reset\02_format_timeseries_csvs.R

:: Cleaning column names in formatted csvs for easy posting to Postgres
Rscript scripts\reset\03_clean_col_names.R

:: Copying formatted archive to postgres. Data beyond today's data are removed, since 
:: EC populates these wth empty rows. If you have limited the max data during the 
:: download in script `01`, use the same date here with an -m flag (eg: -m 2023-01)
Rscript scripts\reset\04_copy_archive_to_postgres.R


