@echo off
echo.
echo  FISH FARM IoT - FULL SYSTEM START
echo  =================================
echo.

echo [0/4] Starting Kafka Server...
set KAFKA_CONFIG=C:\Kafka\kafka_2.13-4.1.0\config\server.properties
start "Kafka Server" cmd /k "C:\Kafka\kafka_2.13-4.1.0\bin\windows\kafka-server-start.bat %KAFKA_CONFIG%"
echo  Waiting for Kafka to initialize...
timeout /t 15 >nul

echo [1/4].1 Starting ETL (505,730 rows)...
start "ETL" cmd /c "python etl_final.py"
timeout /t 8 >nul

echo [1/4].2 Starting Alert Producer...
start "Alert Producer" cmd /c "python alert_producer.py"
timeout /t 8 >nul

echo [2/4] Starting Flask API...
start "API" cmd /c "python server.py"
timeout /t 6 >nul

echo [3/4] Opening Dashboard...
start "" "iot_fishfarm_dashboard.html"

echo.
echo  ALL SYSTEMS LIVE!
echo  - Kafka: Running on default ports
echo  - ETL: Classifying ^& inserting every 5s
echo  - API: http://127.0.0.1:5000/api/data
echo  - Dashboard: Live + A/B+/F--
echo.
echo  Close any window to stop.
pause