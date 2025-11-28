@echo off
chcp 932 >nul
title Job Scraper - Townwork

echo ========================================
echo   Job Scraper - Townwork
echo ========================================
echo.

REM Check if real Python is installed (not Windows Store stub)
REM Windows Store stub returns errorlevel 9009
python --version >nul 2>&1
if %errorlevel% equ 9009 goto :install_python
if %errorlevel% neq 0 goto :install_python

REM Double check - if python exists but no pip, it's likely the stub
pip --version >nul 2>&1
if %errorlevel% neq 0 goto :install_python

goto :python_ok

:install_python
echo Python is not installed. Installing Python 3.11...
echo.

REM Download Python 3.11 installer
echo Downloading Python 3.11 (about 25MB)...
echo Please wait...
echo.
powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri 'https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe' -OutFile '%TEMP%\python-installer.exe'"

if not exist "%TEMP%\python-installer.exe" (
    echo [ERROR] Failed to download Python installer.
    echo Please download manually from: https://www.python.org/downloads/
    pause
    exit /b 1
)

echo Download complete!
echo.
echo Installing Python 3.11 (this may take a few minutes)...
echo Please wait...
echo.

REM Install Python silently with PATH option
"%TEMP%\python-installer.exe" /quiet InstallAllUsers=0 PrependPath=1 Include_test=0 Include_launcher=1

REM Clean up installer
del "%TEMP%\python-installer.exe" >nul 2>&1

echo.
echo ========================================
echo   Python 3.11 installed successfully!
echo ========================================
echo.
echo Please close this window and run start.bat again.
echo (PATH needs to be refreshed)
echo.
pause
exit /b 0

:python_ok
echo Python OK.
python --version
echo.

REM Check packages
echo Checking dependencies...
pip show playwright >nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo [SETUP] Installing required packages...
    echo.
    pip install -r requirements.txt
    if %errorlevel% neq 0 (
        echo.
        echo [ERROR] Failed to install packages.
        pause
        exit /b 1
    )
    echo.
    echo Installing Playwright browser...
    playwright install chromium
    echo.
    echo Setup complete!
    echo.
)

echo Starting application...
echo.

REM Run application
python main.py

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] An error occurred during execution.
    echo.
)

pause
