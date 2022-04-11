:: Setting database password
SET PGPASSWORD=Gws2005

:: Updating daily tables
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.daily from 'E:/saeeshProjects/get_canadian_weather_observations/output/daily/daily_climData_BC_1872-1891.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.daily from 'E:/saeeshProjects/get_canadian_weather_observations/output/daily/daily_climData_BC_1891-1901.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.daily from 'E:/saeeshProjects/get_canadian_weather_observations/output/daily/daily_climData_BC_1901-1911.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.daily from 'E:/saeeshProjects/get_canadian_weather_observations/output/daily/daily_climData_BC_1911-1921.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.daily from 'E:/saeeshProjects/get_canadian_weather_observations/output/daily/daily_climData_BC_1921-1931.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.daily from 'E:/saeeshProjects/get_canadian_weather_observations/output/daily/daily_climData_BC_1931-1941.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.daily from 'E:/saeeshProjects/get_canadian_weather_observations/output/daily/daily_climData_BC_1941-1951.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.daily from 'E:/saeeshProjects/get_canadian_weather_observations/output/daily/daily_climData_BC_1951-1961.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.daily from 'E:/saeeshProjects/get_canadian_weather_observations/output/daily/daily_climData_BC_1961-1971.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.daily from 'E:/saeeshProjects/get_canadian_weather_observations/output/daily/daily_climData_BC_1971-1981.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.daily from 'E:/saeeshProjects/get_canadian_weather_observations/output/daily/daily_climData_BC_1981-1991.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.daily from 'E:/saeeshProjects/get_canadian_weather_observations/output/daily/daily_climData_BC_1991-2001.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.daily from 'E:/saeeshProjects/get_canadian_weather_observations/output/daily/daily_climData_BC_2001-2011.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.daily from 'E:/saeeshProjects/get_canadian_weather_observations/output/daily/daily_climData_BC_2011-2021.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"

:: Updating hourly tables
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.hourly from 'E:/saeeshProjects/get_canadian_weather_observations/output/hourly/hourly_climData_BC_1953-1961.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.hourly from 'E:/saeeshProjects/get_canadian_weather_observations/output/hourly/hourly_climData_BC_1961-1966.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.hourly from 'E:/saeeshProjects/get_canadian_weather_observations/output/hourly/hourly_climData_BC_1966-1971.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.hourly from 'E:/saeeshProjects/get_canadian_weather_observations/output/hourly/hourly_climData_BC_1971-1976.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.hourly from 'E:/saeeshProjects/get_canadian_weather_observations/output/hourly/hourly_climData_BC_1976-1981.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.hourly from 'E:/saeeshProjects/get_canadian_weather_observations/output/hourly/hourly_climData_BC_1981-1986.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.hourly from 'E:/saeeshProjects/get_canadian_weather_observations/output/hourly/hourly_climData_BC_1986-1991.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.hourly from 'E:/saeeshProjects/get_canadian_weather_observations/output/hourly/hourly_climData_BC_1991-1996.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.hourly from 'E:/saeeshProjects/get_canadian_weather_observations/output/hourly/hourly_climData_BC_1996-2001.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.hourly from 'E:/saeeshProjects/get_canadian_weather_observations/output/hourly/hourly_climData_BC_2001-2006.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.hourly from 'E:/saeeshProjects/get_canadian_weather_observations/output/hourly/hourly_climData_BC_2006-2011.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.hourly from 'E:/saeeshProjects/get_canadian_weather_observations/output/hourly/hourly_climData_BC_2011-2016.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"
psql -U matt -h 10.0.1.83 -d gws -c "\copy climate.hourly from 'E:/saeeshProjects/get_canadian_weather_observations/output/hourly/hourly_climData_BC_2016-2021.csv' WITH DELIMITER ',' CSV HEADER NULL as 'NA';"