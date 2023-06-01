# Navigating to project directory
# cd ~/itme/code/GWProjects/databases/ec-climate

# Calling script for downloading new data
Rscript scripts/update/01_download_new_ec_data.R

# Updating data to postgres
Rscript scripts/update/02_update_postgres_dbases.R


