

@echo off
cd /d C:\Users\User\ecommerce-data-pipeline
C:\Users\User\ecommerce-data-pipeline\.venv\Scripts\python.exe pipeline.py
exit /b %ERRORLEVEL%
