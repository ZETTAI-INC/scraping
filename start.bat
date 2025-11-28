@echo off
chcp 65001 >nul
title 求人情報収集システム

echo ========================================
echo   求人情報自動収集システム - タウンワーク
echo ========================================
echo.

REM Pythonが利用可能か確認
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [エラー] Pythonがインストールされていません。
    echo.
    echo Pythonをインストールしてください:
    echo https://www.python.org/downloads/
    echo.
    pause
    exit /b 1
)

echo Pythonを確認しました。
echo.

REM 必要なパッケージがインストールされているか確認
echo 依存パッケージを確認中...
pip show playwright >nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo [初回セットアップ] 必要なパッケージをインストールします...
    echo.
    pip install -r requirements.txt
    if %errorlevel% neq 0 (
        echo.
        echo [エラー] パッケージのインストールに失敗しました。
        pause
        exit /b 1
    )
    echo.
    echo Playwrightブラウザをインストール中...
    playwright install chromium
    echo.
    echo セットアップ完了！
    echo.
)

echo アプリケーションを起動します...
echo.

REM アプリケーション起動
python main.py

if %errorlevel% neq 0 (
    echo.
    echo [エラー] アプリケーションの実行中にエラーが発生しました。
    echo.
)

pause
