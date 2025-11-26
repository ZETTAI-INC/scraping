#!/bin/bash
# Job Collector ビルドスクリプト (Mac/Linux)
# 使用方法: ./build.sh

echo "========================================"
echo "Job Collector - Build Script"
echo "========================================"

# 仮想環境の確認
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# 仮想環境を有効化
source venv/bin/activate

# 依存パッケージのインストール
echo "Installing dependencies..."
pip install -r requirements.txt

# Playwright のブラウザをインストール
echo "Installing Playwright browsers..."
playwright install chromium

# PyInstallerでビルド
echo "Building executable..."
pyinstaller build_exe.spec --clean --noconfirm

echo "========================================"
echo "Build completed!"
echo "Output: dist/JobCollector"
echo "========================================"
