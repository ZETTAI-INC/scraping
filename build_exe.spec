# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec file for Job Collector
Windows exe化用設定
"""

import sys
from pathlib import Path

block_cipher = None

# プロジェクトルート
project_root = Path('.').absolute()

a = Analysis(
    ['main.py'],
    pathex=[str(project_root)],
    binaries=[],
    datas=[
        ('config', 'config'),  # 設定ファイル
        ('scrapers', 'scrapers'),  # スクレイパー
        ('utils', 'utils'),  # ユーティリティ
    ],
    hiddenimports=[
        'PyQt6',
        'PyQt6.QtWidgets',
        'PyQt6.QtCore',
        'PyQt6.QtGui',
        'sqlite3',
        'asyncio',
        'playwright',
        'playwright.async_api',
        'apscheduler',
        'apscheduler.schedulers.background',
        'apscheduler.triggers.interval',
        'pandas',
        'openpyxl',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'streamlit',
        'matplotlib',
        'numpy',
        'scipy',
        'tkinter',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='JobCollector',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,  # GUI アプリなのでコンソールなし
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,  # アイコンファイルがあれば指定
)
