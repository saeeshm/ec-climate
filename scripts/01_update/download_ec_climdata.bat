:: Calling the activation script to run conda
call C:\Users\OWNER\miniconda3\Scripts\activate.bat

:: Activating the GW environment
call conda activate gwenv

:: Navigating to the script home directory
e:
cd E:\saeeshProjects\ec-climate-database\scripts

:: Calling the script to download weather data (hourly and daily) for all BC stations
python get_canadian_weather_observations.py --hourly --daily --start-date "2021-09" --end-date "2022-04" -o "E:\saeeshProjects\ec-climate-database\data\download" BC

pause