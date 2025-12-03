@echo off
REM スクレイパーテスト実行スクリプト
REM
REM 使用方法:
REM   run_tests.bat          - 全テスト実行（E2E除く）
REM   run_tests.bat quick    - 高速テストのみ（URL生成、マッピング）
REM   run_tests.bat e2e      - E2Eテストのみ（実際のサイトアクセス）
REM   run_tests.bat all      - 全テスト実行（E2E含む）
REM   run_tests.bat coverage - カバレッジレポート付き

cd /d "%~dp0"

REM 仮想環境の有効化（存在する場合）
if exist ".venv\Scripts\activate.bat" (
    call .venv\Scripts\activate.bat
) else if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
)

set TEST_MODE=%1

if "%TEST_MODE%"=="" (
    echo === 通常テスト実行（E2E除く） ===
    python -m pytest tests/ -v --ignore=tests/test_scraping_e2e.py
    goto :end
)

if "%TEST_MODE%"=="quick" (
    echo === 高速テスト実行（URL生成、マッピングのみ） ===
    python -m pytest tests/test_url_generation.py tests/test_mappings.py -v
    goto :end
)

if "%TEST_MODE%"=="e2e" (
    echo === E2Eテスト実行（実際のサイトアクセス） ===
    python -m pytest tests/test_scraping_e2e.py -v -m e2e
    goto :end
)

if "%TEST_MODE%"=="all" (
    echo === 全テスト実行（E2E含む） ===
    python -m pytest tests/ -v
    goto :end
)

if "%TEST_MODE%"=="coverage" (
    echo === カバレッジレポート付きテスト実行 ===
    python -m pytest tests/ -v --cov=scrapers --cov-report=html --cov-report=term --ignore=tests/test_scraping_e2e.py
    echo カバレッジレポート: htmlcov/index.html
    goto :end
)

echo 不明なオプション: %TEST_MODE%
echo.
echo 使用方法:
echo   run_tests.bat          - 通常テスト（E2E除く）
echo   run_tests.bat quick    - 高速テストのみ
echo   run_tests.bat e2e      - E2Eテストのみ
echo   run_tests.bat all      - 全テスト
echo   run_tests.bat coverage - カバレッジ付き

:end
