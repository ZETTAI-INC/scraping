#!/usr/bin/env python3
"""
求人情報自動収集システム - メインエントリーポイント
"""
import sys
import os

# パス設定
sys.path.insert(0, os.path.dirname(__file__))

def main():
    """メイン関数"""
    from src.gui.main_window import main as gui_main
    gui_main()


if __name__ == "__main__":
    main()
