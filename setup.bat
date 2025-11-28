@echo off
chcp 65001 >nul
title セットアップ - 求人情報収集システム

echo ========================================
echo   初回セットアップ
echo ========================================
echo.

REM Pythonが利用可能か確認
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [エラー] Pythonがインストールされていません。
    echo.
    echo 以下のURLからPythonをダウンロードしてインストールしてください:
    echo https://www.python.org/downloads/
    echo.
    echo ※インストール時に「Add Python to PATH」にチェックを入れてください
    echo.
    pause
    exit /b 1
)

echo [OK] Python が見つかりました
python --version
echo.

REM pipのアップグレード
echo pipをアップグレード中...
python -m pip install --upgrade pip
echo.

REM 依存パッケージのインストール
echo 依存パッケージをインストール中...
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo.
    echo [エラー] パッケージのインストールに失敗しました。
    pause
    exit /b 1
)
echo.

REM Playwrightブラウザのインストール
echo Playwrightブラウザをインストール中...
playwright install chromium
if %errorlevel% neq 0 (
    echo.
    echo [エラー] Playwrightのインストールに失敗しました。
    pause
    exit /b 1
)
echo.

REM dataフォルダの作成
if not exist "data\db" mkdir "data\db"
if not exist "data\output" mkdir "data\output"
if not exist "data\screenshots" mkdir "data\screenshots"
echo [OK] データフォルダを作成しました
echo.

echo ========================================
echo   セットアップ完了！
echo ========================================
echo.
echo 「start.bat」をダブルクリックしてアプリを起動できます。
echo.
pause
