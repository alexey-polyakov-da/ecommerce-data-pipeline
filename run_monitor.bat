
@echo off
cd /d C:\Users\User\ecommerce-data-pipeline

.venv\Scripts\python.exe monitor.py

exit /b %ERRORLEVEL%
