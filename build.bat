@echo off
REM Job Collector ビルドスクリプト (Windows)
REM 使用方法: build.bat

echo ========================================
echo Job Collector - Build Script
echo ========================================

REM 仮想環境の確認
if not exist "venv" (
    echo Creating virtual environment...
    python -m venv venv
)

REM 仮想環境を有効化
call venv\Scripts\activate.bat

REM 依存パッケージのインストール
echo Installing dependencies...
pip install -r requirements.txt

REM Playwright のブラウザをインストール
echo Installing Playwright browsers...
playwright install chromium

REM PyInstallerでビルド
echo Building executable...
pyinstaller build_exe.spec --clean --noconfirm

echo ========================================
echo Build completed!
echo Output: dist\JobCollector.exe
echo ========================================

pause
