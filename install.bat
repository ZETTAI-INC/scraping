@echo off
chcp 932 >nul
title Installing Dependencies

echo ========================================
echo   Installing Dependencies
echo ========================================
echo.

echo Step 1: Upgrading pip...
python -m pip install --upgrade pip
echo.

echo Step 2: Installing packages from requirements.txt...
pip install playwright pandas openpyxl PyQt6 APScheduler beautifulsoup4 lxml aiofiles python-dotenv pydantic
echo.
echo pip install errorlevel: %errorlevel%
echo.

echo Step 3: Installing Playwright browser...
playwright install chromium
echo.
echo playwright install errorlevel: %errorlevel%
echo.

echo ========================================
echo   Installation Complete
echo ========================================
echo.

echo Verifying installations:
echo.
pip show playwright >nul 2>&1 && echo [OK] playwright || echo [MISSING] playwright
pip show PyQt6 >nul 2>&1 && echo [OK] PyQt6 || echo [MISSING] PyQt6
pip show pandas >nul 2>&1 && echo [OK] pandas || echo [MISSING] pandas
echo.

pause
