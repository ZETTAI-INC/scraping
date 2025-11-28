@echo off
chcp 932 >nul
title Setup - Job Scraper

echo ========================================
echo   Initial Setup
echo ========================================
echo.

REM Check if real Python is installed (not Windows Store stub)
python --version 2>nul | findstr /R "^Python" >nul 2>&1
if %errorlevel% neq 0 (
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

    if %errorlevel% neq 0 (
        echo [ERROR] Python installation failed.
        echo Please install manually from: https://www.python.org/downloads/
        pause
        exit /b 1
    )

    REM Clean up installer
    del "%TEMP%\python-installer.exe" >nul 2>&1

    echo.
    echo ========================================
    echo   Python 3.11 installed successfully!
    echo ========================================
    echo.
    echo Please close this window and run setup.bat again.
    echo (PATH needs to be refreshed)
    echo.
    pause
    exit /b 0
)

echo [OK] Python found
python --version
echo.

REM Upgrade pip
echo Upgrading pip...
python -m pip install --upgrade pip
echo.

REM Install dependencies
echo Installing dependencies...
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Failed to install packages.
    pause
    exit /b 1
)
echo.

REM Install Playwright browser
echo Installing Playwright browser...
playwright install chromium
if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Failed to install Playwright.
    pause
    exit /b 1
)
echo.

REM Create data folders
if not exist "data\db" mkdir "data\db"
if not exist "data\output" mkdir "data\output"
if not exist "data\screenshots" mkdir "data\screenshots"
echo [OK] Data folders created
echo.

echo ========================================
echo   Setup Complete!
echo ========================================
echo.
echo Double-click "start.bat" to run the application.
echo.
pause
