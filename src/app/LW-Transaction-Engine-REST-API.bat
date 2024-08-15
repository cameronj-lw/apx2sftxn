@echo off
call C:\lw\python_venv\311-lw-transaction-engine\integration-lw-transaction-engine\src\.venv\Scripts\activate.bat
call python C:\lw\python_venv\311-lw-transaction-engine\integration-lw-transaction-engine\src\app\rest_api.py

:: Now implement shutdown
for /f "tokens=2 delims=:" %%i in ('ipconfig ^| findstr /i "IPv4 Address"') do set ip=%%i
set ip=%ip:~1%

:: Use the obtained IP address in the curl command
curl -X POST http://%ip%:9002/api/shutdown
