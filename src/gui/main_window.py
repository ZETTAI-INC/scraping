"""
メインウィンドウ
要件定義 11章 UI/UX設計に準拠
"""
import sys
import asyncio
import subprocess
import platform
from datetime import datetime
import time
from pathlib import Path
from typing import Optional, List, Dict
import logging

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QLineEdit, QComboBox, QCheckBox,
    QTableWidget, QTableWidgetItem, QProgressBar, QStatusBar,
    QGroupBox, QSpinBox, QTextEdit, QTabWidget, QMessageBox,
    QFileDialog, QHeaderView, QSplitter, QFrame, QScrollArea,
    QGridLayout, QSizePolicy
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QFont, QColor

# Matplotlib for charts
import matplotlib
matplotlib.use('QtAgg')
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

# 日本語フォント設定（Windows用）
# インポート時に設定することで、全てのグラフに適用される
plt.rcParams['font.family'] = ['MS Gothic', 'Yu Gothic', 'Meiryo', 'Hiragino Sans', 'sans-serif']
plt.rcParams['axes.unicode_minus'] = False  # マイナス記号の文字化け対策

# パス設定
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.services.crawl_service import CrawlService
from src.filters.job_filter import JobFilter, FilterResult
from src.gui.styles import MODERN_STYLE

logger = logging.getLogger(__name__)


class CrawlWorker(QThread):
    """クローリングワーカースレッド"""
    finished = pyqtSignal(dict)
    progress = pyqtSignal(str, int, int)  # message, current, total
    error = pyqtSignal(str)
    time_update = pyqtSignal(float, int, int)  # elapsed_time, scraped_count, saved_count
    detail_progress = pyqtSignal(int, int, int)  # current_detail, total_details, total_scraped
    stopped = pyqtSignal()  # 停止完了シグナル
    realtime_update = pyqtSignal(float, int, str)  # elapsed_time, scraped_count, current_task

    def __init__(self, service: CrawlService, keywords: List[str], areas: List[str], max_pages: int, parallel: int, sources: List[str] = None):
        super().__init__()
        self.service = service
        self.keywords = keywords
        self.areas = areas
        self.max_pages = max_pages
        self.parallel = parallel
        self.sources = sources or ["townwork"]  # デフォルトはタウンワーク
        self._stop_requested = False
        self._current_scraped_count = 0  # リアルタイム件数追跡用
        self._current_task = ""  # 現在のタスク
        self._start_time = 0

    def request_stop(self):
        """停止をリクエスト"""
        self._stop_requested = True

    def run(self):
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # 進捗コールバックを設定
            total_combinations = len(self.keywords) * len(self.areas) * len(self.sources)
            current_idx = [0]  # リストにして参照渡し

            # 時間計測開始
            self._start_time = time.time()
            self._current_scraped_count = 0

            def progress_callback(message: str, current: int, total: int):
                self.progress.emit(message, current_idx[0] + 1, total_combinations)
                # リアルタイム更新も発行
                elapsed = time.time() - self._start_time
                self.realtime_update.emit(elapsed, self._current_scraped_count, self._current_task)

            def detail_progress_callback(current_detail: int, total_details: int, total_scraped: int):
                self.detail_progress.emit(current_detail, total_details, total_scraped)

            def realtime_count_callback(count: int):
                # スクレイパーから件数報告を受け取った際に、リアルタイム更新を発行
                self._current_scraped_count = count
                elapsed = time.time() - self._start_time
                self.realtime_update.emit(elapsed, count, self._current_task)

            self.service.set_progress_callback(progress_callback)
            self.service.set_detail_progress_callback(detail_progress_callback)
            self.service.set_realtime_count_callback(realtime_count_callback)

            # 各組み合わせを順次実行して進捗を報告
            all_results = {
                'scraped_count': 0,
                'total_count': 0,
                'new_count': 0,
                'saved_count': 0,
                'duplicate_count': 0,
                'existing_count': 0,
                'jobs': [],
                'error': None,
            }

            stopped = False
            for source in self.sources:
                if self._stop_requested:
                    stopped = True
                    break
                for kw in self.keywords:
                    if self._stop_requested:
                        stopped = True
                        break
                    for area in self.areas:
                        if self._stop_requested:
                            stopped = True
                            break

                        # 進捗を報告
                        source_labels = {"townwork": "タウンワーク", "indeed": "Indeed", "baitoru": "バイトル", "hellowork": "ハローワーク"}
                        source_label = source_labels.get(source, source)
                        self._current_task = f"[{source_label}] {area} × {kw}"
                        self.progress.emit(f"{self._current_task} を検索中...", current_idx[0] + 1, total_combinations)

                        # リアルタイム更新を発行
                        elapsed = time.time() - self._start_time
                        self.realtime_update.emit(elapsed, self._current_scraped_count, self._current_task)

                        if source == "townwork":
                            result = loop.run_until_complete(
                                self.service.crawl_townwork(
                                    keywords=[kw],
                                    areas=[area],
                                    max_pages=self.max_pages,
                                    parallel=self.parallel
                                )
                            )
                        elif source == "indeed":
                            result = loop.run_until_complete(
                                self.service.crawl_indeed(
                                    keywords=[kw],
                                    areas=[area],
                                    max_pages=1  # Indeed は403対策のため1ページ固定
                                )
                            )
                        elif source == "baitoru":
                            result = loop.run_until_complete(
                                self.service.crawl_baitoru(
                                    keywords=[kw],
                                    areas=[area],
                                    max_pages=self.max_pages
                                )
                            )
                        elif source == "hellowork":
                            result = loop.run_until_complete(
                                self.service.crawl_hellowork(
                                    keywords=[kw],
                                    areas=[area],
                                    max_pages=self.max_pages
                                )
                            )
                        else:
                            continue

                        all_results['total_count'] += result.get('total_count', 0)
                        all_results['scraped_count'] += result.get('scraped_count', result.get('total_count', 0))
                        all_results['new_count'] += result.get('new_count', 0)
                        all_results['saved_count'] += result.get('saved_count', 0)
                        all_results['duplicate_count'] += result.get('duplicate_count', 0)
                        all_results['existing_count'] += result.get('existing_count', 0)
                        all_results['jobs'].extend(result.get('jobs', []))
                        if result.get('error'):
                            all_results['error'] = result['error']

                        current_idx[0] += 1

                        # リアルタイム件数を更新
                        self._current_scraped_count = all_results['scraped_count']

                        # 経過時間と取得件数を報告
                        elapsed = time.time() - self._start_time
                        self.time_update.emit(
                            elapsed,
                            all_results['scraped_count'],
                            all_results['saved_count']
                        )
                        # リアルタイム更新も発行
                        self.realtime_update.emit(elapsed, self._current_scraped_count, self._current_task)

            # 時間計測終了
            end_time = time.time()
            total_elapsed = end_time - self._start_time

            # 結果に時間情報を追加
            all_results['elapsed_time'] = total_elapsed
            all_results['start_time'] = self._start_time
            all_results['end_time'] = end_time
            all_results['stopped'] = stopped

            loop.close()

            if stopped:
                self.stopped.emit()
            else:
                self.finished.emit(all_results)

        except Exception as e:
            self.error.emit(str(e))


class MainWindow(QMainWindow):
    """メインウィンドウ"""

    def __init__(self):
        super().__init__()
        self.service = CrawlService()
        self.current_jobs = []
        self.filter_result: Optional[FilterResult] = None
        self.crawl_worker: Optional[CrawlWorker] = None
        self.latest_run_summary: Optional[dict] = None

        # フィルタチェックボックスの辞書
        self.filter_checks: Dict[str, QCheckBox] = {}

        # 地域チェックボックスの辞書
        self.area_checks: Dict[str, QCheckBox] = {}

        # キーワードチェックボックスの辞書
        self.keyword_checks: Dict[str, QCheckBox] = {}

        self.init_ui()
        self.load_stats()

    def init_ui(self):
        """UIを初期化"""
        self.setWindowTitle("求人情報自動収集システム - タウンワーク / Indeed / バイトル / ハローワーク")
        self.setMinimumSize(1200, 800)

        # メインウィジェット
        main_widget = QWidget()
        self.setCentralWidget(main_widget)

        # メインレイアウト
        main_layout = QVBoxLayout(main_widget)
        main_layout.setContentsMargins(5, 5, 5, 5)

        # 左右パネルを水平スプリッターで配置（幅を自由に変更可能）
        self.main_splitter = QSplitter(Qt.Orientation.Horizontal)
        self.main_splitter.setChildrenCollapsible(False)

        # 左パネル（検索条件 + フィルタ設定）
        left_panel = self.create_left_panel()
        self.main_splitter.addWidget(left_panel)

        # 右パネル（結果表示）
        right_panel = self.create_right_panel()
        self.main_splitter.addWidget(right_panel)

        # 初期の幅比率を設定（左:右 = 500:700）- 左パネルを広げる
        self.main_splitter.setSizes([500, 700])

        main_layout.addWidget(self.main_splitter)

        # ステータスバー
        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)
        self.statusBar.showMessage("準備完了")

    def create_left_panel(self) -> QWidget:
        """左パネル（タブ形式：地域選択 / キーワード / フィルタ・オプション）を作成"""
        # メインパネル
        panel = QWidget()
        panel.setMinimumWidth(350)
        layout = QVBoxLayout(panel)
        layout.setSpacing(5)
        layout.setContentsMargins(5, 5, 5, 5)

        # 媒体選択（タブの上に配置）
        source_group = QGroupBox("対象媒体")
        source_layout = QVBoxLayout(source_group)

        self.townwork_check = QCheckBox("タウンワーク")
        self.townwork_check.setChecked(True)
        source_layout.addWidget(self.townwork_check)

        self.indeed_check = QCheckBox("Indeed")
        self.indeed_check.setChecked(False)
        self.indeed_check.setToolTip("Indeed検索（403対策のため1ページ/組み合わせに制限）")
        source_layout.addWidget(self.indeed_check)

        self.baitoru_check = QCheckBox("バイトル")
        self.baitoru_check.setChecked(False)
        self.baitoru_check.setToolTip("バイトル検索（カテゴリがない場合はキーワード検索にフォールバック）")
        source_layout.addWidget(self.baitoru_check)

        self.hellowork_check = QCheckBox("ハローワーク")
        self.hellowork_check.setChecked(False)
        self.hellowork_check.setToolTip("ハローワーク求人検索（都道府県選択でフォーム検索）")
        source_layout.addWidget(self.hellowork_check)

        layout.addWidget(source_group)

        # タブウィジェット（3つのタブ）
        self.left_tabs = QTabWidget()
        self.left_tabs.setStyleSheet("""
            QTabWidget::pane {
                border: 1px solid #e1e4e8;
                border-radius: 4px;
                background-color: #ffffff;
            }
            QTabBar::tab {
                padding: 8px 16px;
                margin-right: 2px;
                background-color: #f3f4f6;
                border: 1px solid #e1e4e8;
                border-bottom: none;
                border-top-left-radius: 4px;
                border-top-right-radius: 4px;
            }
            QTabBar::tab:selected {
                background-color: #ffffff;
                font-weight: bold;
            }
            QTabBar::tab:hover {
                background-color: #e5e7eb;
            }
        """)

        # タブ1: 地域選択
        area_tab = self.create_area_tab()
        self.left_tabs.addTab(area_tab, "地域選択")

        # タブ2: キーワード選択 + 追加キーワード
        keyword_tab = self.create_keyword_tab()
        self.left_tabs.addTab(keyword_tab, "キーワード")

        # タブ3: フィルタ設定 + 検索オプション
        filter_tab = self.create_filter_tab_left()
        self.left_tabs.addTab(filter_tab, "フィルタ・オプション")

        layout.addWidget(self.left_tabs, 1)

        # 実行ボタン・停止ボタン（タブの下に配置）
        btn_layout = QHBoxLayout()

        self.search_btn = QPushButton("検索実行")
        self.search_btn.setProperty("class", "primary")
        self.search_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.search_btn.setMinimumHeight(40)
        self.search_btn.clicked.connect(self.start_crawl)
        btn_layout.addWidget(self.search_btn)

        self.stop_btn = QPushButton("停止")
        self.stop_btn.setStyleSheet("""
            QPushButton {
                background-color: #dc3545;
                color: white;
                border: none;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #c82333;
            }
            QPushButton:pressed {
                background-color: #bd2130;
            }
            QPushButton:disabled {
                background-color: #6c757d;
            }
        """)
        self.stop_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.stop_btn.setMinimumHeight(40)
        self.stop_btn.setEnabled(False)
        self.stop_btn.clicked.connect(self.stop_crawl)
        btn_layout.addWidget(self.stop_btn)

        layout.addLayout(btn_layout)

        # 進捗表示ラベル（クローリング中の詳細）
        self.progress_label = QLabel("")
        self.progress_label.setStyleSheet("color: #1f77b4; font-weight: bold;")
        self.progress_label.setVisible(False)
        layout.addWidget(self.progress_label)

        # 進捗バー
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)

        # 時間計測表示ラベル
        self.time_label = QLabel("")
        self.time_label.setStyleSheet("""
            QLabel {
                color: #2e7d32;
                font-weight: bold;
                font-size: 12px;
                padding: 5px;
                background-color: #e8f5e9;
                border: 1px solid #c8e6c9;
                border-radius: 4px;
            }
        """)
        self.time_label.setVisible(False)
        layout.addWidget(self.time_label)

        # 今回の新規取得件数（目立つ表示）
        self.new_count_label = QLabel("今回の新規取得：-- 件（抽出：-- 件）")
        self.new_count_label.setStyleSheet("""
            QLabel {
                color: #1565c0;
                font-weight: bold;
                font-size: 14px;
                padding: 10px;
                background-color: #ffffff;
                border: 2px solid #1976d2;
                border-radius: 6px;
            }
        """)
        self.new_count_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.new_count_label)

        # 最終クローリング日時
        self.last_crawl_label = QLabel("最終クローリング日時：--")
        self.last_crawl_label.setStyleSheet("""
            QLabel {
                color: #1565c0;
                font-weight: bold;
                font-size: 12px;
                padding: 5px;
                background-color: #e3f2fd;
                border: 1px solid #90caf9;
                border-radius: 4px;
            }
        """)
        layout.addWidget(self.last_crawl_label)

        # 統計情報
        stats_group = QGroupBox("統計情報")
        stats_layout = QVBoxLayout(stats_group)
        self.stats_label = QLabel("読み込み中...")
        stats_layout.addWidget(self.stats_label)
        layout.addWidget(stats_group)

        return panel

    def create_area_tab(self) -> QWidget:
        """地域選択タブを作成"""
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)

        # 全選択/全解除ボタン
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(20)
        select_all_btn = QPushButton("全選択")
        select_all_btn.setMinimumHeight(40)
        select_all_btn.clicked.connect(self.select_all_areas)
        deselect_all_btn = QPushButton("全解除")
        deselect_all_btn.setMinimumHeight(40)
        deselect_all_btn.clicked.connect(self.deselect_all_areas)
        btn_layout.addWidget(select_all_btn)
        btn_layout.addWidget(deselect_all_btn)
        layout.addLayout(btn_layout)

        # 地方別選択ボタン
        self.region_definitions = [
            ("北海道", ["北海道"]),
            ("東北", ["青森", "岩手", "宮城", "秋田", "山形", "福島"]),
            ("関東", ["茨城", "栃木", "群馬", "埼玉", "千葉", "東京", "神奈川"]),
            ("中部", ["新潟", "富山", "石川", "福井", "山梨", "長野", "岐阜", "静岡", "愛知"]),
            ("近畿", ["三重", "滋賀", "京都", "大阪", "兵庫", "奈良", "和歌山"]),
            ("中国", ["鳥取", "島根", "岡山", "広島", "山口"]),
            ("四国", ["徳島", "香川", "愛媛", "高知"]),
            ("九州", ["福岡", "佐賀", "長崎", "熊本", "大分", "宮崎", "鹿児島", "沖縄"]),
        ]

        # 地方ボタングループ
        region_group = QGroupBox("地方で選択")
        region_layout = QGridLayout(region_group)
        region_layout.setHorizontalSpacing(10)
        region_layout.setVerticalSpacing(10)
        self.region_buttons: Dict[str, QPushButton] = {}

        for i, (region_name, prefs) in enumerate(self.region_definitions):
            btn = QPushButton(region_name)
            btn.setCheckable(True)
            btn.setCursor(Qt.CursorShape.PointingHandCursor)
            btn.setMinimumHeight(36)
            btn.clicked.connect(lambda checked, p=prefs, b=btn: self.toggle_areas_by_region(p, b))
            self.region_buttons[region_name] = btn
            region_layout.addWidget(btn, i // 4, i % 4)

        layout.addWidget(region_group)

        # 都道府県チェックボックス
        pref_group = QGroupBox("都道府県")
        pref_layout = QGridLayout(pref_group)
        pref_layout.setHorizontalSpacing(10)
        pref_layout.setVerticalSpacing(8)

        all_prefectures = [
            ("北海道", False), ("青森", False), ("岩手", False), ("宮城", False),
            ("秋田", False), ("山形", False), ("福島", False),
            ("茨城", False), ("栃木", False), ("群馬", False), ("埼玉", False),
            ("千葉", False), ("東京", True), ("神奈川", False),
            ("新潟", False), ("富山", False), ("石川", False), ("福井", False),
            ("山梨", False), ("長野", False), ("岐阜", False), ("静岡", False),
            ("愛知", False),
            ("三重", False), ("滋賀", False), ("京都", False), ("大阪", False),
            ("兵庫", False), ("奈良", False), ("和歌山", False),
            ("鳥取", False), ("島根", False), ("岡山", False), ("広島", False),
            ("山口", False),
            ("徳島", False), ("香川", False), ("愛媛", False), ("高知", False),
            ("福岡", False), ("佐賀", False), ("長崎", False), ("熊本", False),
            ("大分", False), ("宮崎", False), ("鹿児島", False), ("沖縄", False),
        ]

        for i, (area, default) in enumerate(all_prefectures):
            check = QCheckBox(area)
            check.setChecked(default)
            self.area_checks[area] = check
            pref_layout.addWidget(check, i // 4, i % 4)

        layout.addWidget(pref_group)
        layout.addStretch()

        scroll.setWidget(widget)
        return scroll

    def create_keyword_tab(self) -> QWidget:
        """キーワード選択タブを作成"""
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)

        # 全選択/全解除ボタン
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(20)
        select_all_btn = QPushButton("全選択")
        select_all_btn.setMinimumHeight(40)
        select_all_btn.clicked.connect(self.select_all_keywords)
        deselect_all_btn = QPushButton("全解除")
        deselect_all_btn.setMinimumHeight(40)
        deselect_all_btn.clicked.connect(self.deselect_all_keywords)
        btn_layout.addWidget(select_all_btn)
        btn_layout.addWidget(deselect_all_btn)
        layout.addLayout(btn_layout)

        # キーワードチェックボックス
        keyword_group = QGroupBox("キーワード一覧")
        keyword_layout = QGridLayout(keyword_group)
        keyword_layout.setHorizontalSpacing(10)
        keyword_layout.setVerticalSpacing(8)

        keywords = [
            ("IT", True), ("エンジニア", False), ("プログラマー", False),
            ("SE", False), ("Web", False), ("システム", False),
            ("事務", False), ("経理", False), ("総務", False),
            ("人事", False), ("秘書", False), ("受付", False),
            ("営業", False), ("販売", False), ("接客", False),
            ("店長", False), ("マネージャー", False),
            ("製造", False), ("工場", False), ("物流", False),
            ("倉庫", False), ("ドライバー", False), ("配送", False),
            ("看護", False), ("介護", False), ("医療", False),
            ("保育", False), ("薬剤師", False),
            ("飲食", False), ("調理", False), ("ホール", False),
            ("清掃", False), ("警備", False),
            ("デザイン", False), ("企画", False), ("マーケティング", False),
            ("コールセンター", False), ("データ入力", False), ("軽作業", False),
        ]

        for i, (keyword, default) in enumerate(keywords):
            check = QCheckBox(keyword)
            check.setChecked(default)
            self.keyword_checks[keyword] = check
            keyword_layout.addWidget(check, i // 3, i % 3)

        layout.addWidget(keyword_group)

        # 追加キーワード入力
        custom_group = QGroupBox("追加キーワード")
        custom_layout = QVBoxLayout(custom_group)
        self.custom_keyword_input = QTextEdit()
        self.custom_keyword_input.setPlaceholderText("カンマ区切りで入力（例: コンサル, マネジメント）")
        self.custom_keyword_input.setMinimumHeight(80)
        self.custom_keyword_input.setMaximumHeight(120)
        custom_layout.addWidget(self.custom_keyword_input)
        layout.addWidget(custom_group)

        layout.addStretch()

        scroll.setWidget(widget)
        return scroll

    def create_filter_tab_left(self) -> QWidget:
        """フィルタ・オプションタブを作成"""
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)

        # 検索オプション
        option_group = QGroupBox("検索オプション")
        option_layout = QGridLayout(option_group)
        option_layout.setColumnStretch(2, 1)

        option_layout.addWidget(QLabel("最大ページ数:"), 0, 0)
        self.max_pages_spin = QSpinBox()
        self.max_pages_spin.setRange(1, 20)
        self.max_pages_spin.setValue(5)
        option_layout.addWidget(self.max_pages_spin, 0, 1)

        option_layout.addWidget(QLabel("並列数:"), 1, 0)
        self.parallel_spin = QSpinBox()
        self.parallel_spin.setRange(1, 12)
        self.parallel_spin.setValue(5)
        option_layout.addWidget(self.parallel_spin, 1, 1)

        layout.addWidget(option_group)

        # フィルタ設定
        filter_group = QGroupBox("フィルタ設定")
        filter_layout = QVBoxLayout(filter_group)

        # 全選択/全解除ボタン
        btn_layout = QHBoxLayout()
        select_all_btn = QPushButton("全選択")
        select_all_btn.clicked.connect(self.select_all_filters)
        deselect_all_btn = QPushButton("全解除")
        deselect_all_btn.clicked.connect(self.deselect_all_filters)
        btn_layout.addWidget(select_all_btn)
        btn_layout.addWidget(deselect_all_btn)
        filter_layout.addLayout(btn_layout)

        # フィルタ項目
        filters_info = [
            ("duplicate_phone", "電話番号重複削除", "同一電話番号の求人を1件に集約"),
            ("large_company", "大企業除外（1001人以上）", "従業員数1,001人以上の企業を除外"),
            ("dispatch_keyword", "派遣・紹介キーワード除外", "人材派遣、人材紹介等を含む企業を除外"),
            ("industry", "業界フィルタ", "広告、メディア、出版業界を除外"),
            ("location_okinawa", "沖縄県除外", "勤務地が沖縄県の求人を除外"),
            ("phone_prefix", "電話番号プレフィックス除外", "0120, 050, 沖縄局番等を除外"),
        ]

        for key, label, tooltip in filters_info:
            check = QCheckBox(label)
            check.setChecked(True)
            check.setToolTip(tooltip)
            self.filter_checks[key] = check
            filter_layout.addWidget(check)

        # 追加の除外キーワード入力
        filter_layout.addWidget(QLabel("追加除外キーワード:"))
        self.extra_keywords_input = QLineEdit()
        self.extra_keywords_input.setPlaceholderText("カンマ区切りで入力")
        self.extra_keywords_input.setToolTip("会社名・事業内容に含まれる場合に除外")
        filter_layout.addWidget(self.extra_keywords_input)

        # 従業員数しきい値
        emp_layout = QHBoxLayout()
        emp_layout.addWidget(QLabel("従業員数上限:"))
        self.employee_threshold_spin = QSpinBox()
        self.employee_threshold_spin.setRange(100, 10000)
        self.employee_threshold_spin.setValue(1001)
        self.employee_threshold_spin.setSingleStep(100)
        emp_layout.addWidget(self.employee_threshold_spin)
        emp_layout.addWidget(QLabel("人以上を除外"))
        filter_layout.addLayout(emp_layout)

        layout.addWidget(filter_group)
        layout.addStretch()

        scroll.setWidget(widget)
        return scroll

    def toggle_areas_by_region(self, area_list: List[str], button: QPushButton):
        """地方ボタンの状態に応じて地域を選択/解除"""
        is_checked = button.isChecked()
        for area in area_list:
            if area in self.area_checks:
                self.area_checks[area].setChecked(is_checked)

    def update_region_buttons(self):
        """都道府県の選択状態に応じて地方ボタンの状態を更新"""
        for region_name, prefs in self.region_definitions:
            if region_name in self.region_buttons:
                # その地方の全都道府県が選択されているかチェック
                all_selected = all(
                    self.area_checks.get(pref, QCheckBox()).isChecked()
                    for pref in prefs
                )
                self.region_buttons[region_name].setChecked(all_selected)

    def select_all_areas(self):
        """全地域を選択"""
        for check in self.area_checks.values():
            check.setChecked(True)
        # 地方ボタンの状態を更新
        for btn in self.region_buttons.values():
            btn.setChecked(True)

    def deselect_all_areas(self):
        """全地域を解除"""
        for check in self.area_checks.values():
            check.setChecked(False)
        # 地方ボタンの状態を更新
        for btn in self.region_buttons.values():
            btn.setChecked(False)

    def get_selected_areas(self) -> List[str]:
        """選択された地域を取得"""
        return [area for area, check in self.area_checks.items() if check.isChecked()]

    def select_all_keywords(self):
        """全キーワードを選択"""
        for check in self.keyword_checks.values():
            check.setChecked(True)

    def deselect_all_keywords(self):
        """全キーワードを解除"""
        for check in self.keyword_checks.values():
            check.setChecked(False)

    def get_selected_keywords(self) -> List[str]:
        """選択されたキーワードを取得"""
        keywords = [kw for kw, check in self.keyword_checks.items() if check.isChecked()]

        # カスタムキーワードを追加
        custom = self.custom_keyword_input.toPlainText()
        if custom:
            custom_keywords = [k.strip() for k in custom.replace("\n", ",").split(",") if k.strip()]
            keywords.extend(custom_keywords)

        return keywords

    def select_all_filters(self):
        """全フィルタを選択"""
        for check in self.filter_checks.values():
            check.setChecked(True)

    def deselect_all_filters(self):
        """全フィルタを解除"""
        for check in self.filter_checks.values():
            check.setChecked(False)

    def get_filter_settings(self) -> Dict[str, bool]:
        """現在のフィルタ設定を取得"""
        return {key: check.isChecked() for key, check in self.filter_checks.items()}

    def create_right_panel(self) -> QWidget:
        """右パネル（結果表示）を作成"""
        panel = QWidget()
        layout = QVBoxLayout(panel)

        tabs = QTabWidget()

        # 検索結果タブ
        results_tab = self.create_results_tab()
        tabs.addTab(results_tab, "検索結果")

        # フィルタ結果タブ
        filter_tab = self.create_filter_tab()
        tabs.addTab(filter_tab, "フィルタ結果")

        # 分析タブ
        analysis_tab = self.create_analysis_tab()
        tabs.addTab(analysis_tab, "分析")

        layout.addWidget(tabs)
        return panel

    def create_results_tab(self) -> QWidget:
        """検索結果タブを作成"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        self.results_table = QTableWidget()
        self.results_table.setColumnCount(7)
        self.results_table.setHorizontalHeaderLabels([
            "会社名", "職種", "勤務地", "給与", "雇用形態", "URL", "取得日時"
        ])
        self.results_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.results_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.results_table.setAlternatingRowColors(True)
        self.results_table.setShowGrid(False)
        self.results_table.verticalHeader().setVisible(False)
        layout.addWidget(self.results_table)

        btn_layout = QHBoxLayout()

        self.result_count_label = QLabel("0 件")
        btn_layout.addWidget(self.result_count_label)

        btn_layout.addStretch()

        self.apply_filter_btn = QPushButton("フィルタ適用")
        self.apply_filter_btn.setProperty("class", "success")
        self.apply_filter_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.apply_filter_btn.clicked.connect(self.apply_filter)
        btn_layout.addWidget(self.apply_filter_btn)

        self.export_btn = QPushButton("CSVエクスポート（全件）")
        self.export_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.export_btn.clicked.connect(self.export_csv)
        btn_layout.addWidget(self.export_btn)

        layout.addLayout(btn_layout)
        return tab

    def create_filter_tab(self) -> QWidget:
        """フィルタ結果タブを作成"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # フィルタ結果サマリー
        self.filter_result_text = QTextEdit()
        self.filter_result_text.setReadOnly(True)
        self.filter_result_text.setFont(QFont("Monaco", 11))
        self.filter_result_text.setStyleSheet("""
            QTextEdit {
                background-color: #f8f9fa;
                border: 1px solid #dee2e6;
                padding: 10px;
            }
        """)
        layout.addWidget(self.filter_result_text, 1)

        # フィルタ後テーブル
        self.filtered_table = QTableWidget()
        self.filtered_table.setColumnCount(7)
        self.filtered_table.setHorizontalHeaderLabels([
            "会社名", "職種", "勤務地", "給与", "雇用形態", "URL", "取得日時"
        ])
        self.filtered_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.filtered_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.filtered_table.setAlternatingRowColors(True)
        self.filtered_table.setShowGrid(False)
        self.filtered_table.verticalHeader().setVisible(False)
        layout.addWidget(self.filtered_table, 2)

        btn_layout = QHBoxLayout()

        self.filtered_count_label = QLabel("フィルタ後: 0 件")
        btn_layout.addWidget(self.filtered_count_label)

        btn_layout.addStretch()

        self.export_filtered_btn = QPushButton("フィルタ済みCSVエクスポート")
        self.export_filtered_btn.setProperty("class", "primary")
        self.export_filtered_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.export_filtered_btn.clicked.connect(self.export_filtered_csv)
        btn_layout.addWidget(self.export_filtered_btn)

        layout.addLayout(btn_layout)
        return tab

    def create_analysis_tab(self) -> QWidget:
        """分析タブを作成（新規取得件数の推移グラフ）"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # 上部コントロール
        control_layout = QHBoxLayout()

        control_layout.addWidget(QLabel("過去"))
        self.analysis_count_spin = QSpinBox()
        self.analysis_count_spin.setRange(1, 100)
        self.analysis_count_spin.setValue(20)
        self.analysis_count_spin.setSuffix(" 回")
        self.analysis_count_spin.setMinimumWidth(80)
        control_layout.addWidget(self.analysis_count_spin)

        control_layout.addWidget(QLabel("の推移"))

        self.refresh_chart_btn = QPushButton("グラフ更新")
        self.refresh_chart_btn.setProperty("class", "primary")
        self.refresh_chart_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.refresh_chart_btn.clicked.connect(self.update_analysis_chart)
        control_layout.addWidget(self.refresh_chart_btn)

        control_layout.addStretch()

        layout.addLayout(control_layout)

        # Matplotlibチャート
        self.analysis_figure = Figure(figsize=(8, 5), dpi=100)
        self.analysis_canvas = FigureCanvas(self.analysis_figure)
        self.analysis_canvas.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        layout.addWidget(self.analysis_canvas)

        # 統計サマリー
        self.analysis_summary_label = QLabel("グラフ更新ボタンを押すと、過去のクローリング結果を表示します")
        self.analysis_summary_label.setStyleSheet("""
            QLabel {
                color: #424242;
                font-size: 12px;
                padding: 10px;
                background-color: #f5f5f5;
                border: 1px solid #e0e0e0;
                border-radius: 4px;
            }
        """)
        layout.addWidget(self.analysis_summary_label)

        return tab

    def update_analysis_chart(self):
        """分析チャートを更新"""
        count = self.analysis_count_spin.value()

        # クロールログを取得（回数ごと）
        crawl_data = self._get_crawl_logs(count)

        if not crawl_data:
            self.analysis_summary_label.setText(f"クローリングデータがありません")
            self.analysis_figure.clear()
            self.analysis_canvas.draw()
            return

        # データを抽出
        labels = [row['label'] for row in crawl_data]
        new_counts = [row['new_count'] for row in crawl_data]

        # グラフ描画
        self.analysis_figure.clear()
        ax = self.analysis_figure.add_subplot(111)

        x = np.arange(len(labels))
        width = 0.5

        bars = ax.bar(x, new_counts, width, label='新規取得', color='#4caf50', alpha=0.8)

        ax.set_xlabel('実行回')
        ax.set_ylabel('件数')
        ax.set_title(f'過去{count}回の新規取得件数推移')
        ax.set_xticks(x)

        # ラベルを見やすく
        if len(labels) > 15:
            step = max(1, len(labels) // 15)
            display_labels = [l if i % step == 0 else '' for i, l in enumerate(labels)]
        else:
            display_labels = labels

        ax.set_xticklabels(display_labels, rotation=45, ha='right', fontsize=8)

        # 値をバーの上に表示
        for bar, cnt in zip(bars, new_counts):
            if cnt > 0:
                ax.annotate(f'{cnt}',
                           xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                           xytext=(0, 3),
                           textcoords="offset points",
                           ha='center', va='bottom', fontsize=8)

        self.analysis_figure.tight_layout()
        self.analysis_canvas.draw()

        # サマリー更新
        total_new = sum(new_counts)
        avg_new = total_new / len(new_counts) if new_counts else 0
        max_new = max(new_counts) if new_counts else 0
        max_idx = new_counts.index(max_new) if new_counts and max_new > 0 else -1
        max_label = labels[max_idx] if max_idx >= 0 else '-'

        self.analysis_summary_label.setText(
            f"表示: {len(crawl_data)}回分 | "
            f"累計新規: {total_new:,}件 | "
            f"平均新規: {avg_new:.1f}件/回 | 最大新規: {max_new}件 ({max_label})"
        )

    def _get_crawl_logs(self, limit: int) -> List[Dict]:
        """過去n回のクロールログを取得"""
        try:
            with self.service.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT
                        id,
                        started_at,
                        new_count,
                        total_count
                    FROM crawl_logs
                    ORDER BY started_at DESC
                    LIMIT ?
                """, (limit,))

                rows = cursor.fetchall()
                # 古い順に並べ替え
                rows = list(reversed(rows))
                result = []
                for row in rows:
                    started_at = row[1]
                    # 日時からラベルを作成（MM/DD HH:MM形式）
                    if started_at:
                        try:
                            dt = datetime.strptime(started_at, '%Y-%m-%d %H:%M:%S')
                            label = dt.strftime('%m/%d %H:%M')
                        except:
                            label = started_at[:16] if len(started_at) >= 16 else started_at
                    else:
                        label = f"#{row[0]}"
                    result.append({
                        'label': label,
                        'new_count': row[2] or 0,
                        'total_count': row[3] or 0
                    })
                return result
        except Exception as e:
            logger.error(f"Error fetching crawl logs: {e}")
            return []

    def _format_elapsed_time(self, elapsed_time: float) -> str:
        """経過時間を見やすい文字列にフォーマット"""
        if elapsed_time < 60:
            return f"{elapsed_time:.1f}秒"
        elif elapsed_time < 3600:
            minutes = int(elapsed_time // 60)
            seconds = int(elapsed_time % 60)
            return f"{minutes}分{seconds}秒"
        else:
            hours = int(elapsed_time // 3600)
            minutes = int((elapsed_time % 3600) // 60)
            seconds = int(elapsed_time % 60)
            return f"{hours}時間{minutes}分{seconds}秒"

    def load_stats(self, run_summary: Optional[dict] = None):
        """統計情報を読み込み"""
        if run_summary is not None:
            self.latest_run_summary = run_summary

        try:
            stats = self.service.get_stats()
            lines = []

            if self.latest_run_summary:
                run = self.latest_run_summary
                lines.append(
                    f"今回: 取得 {run.get('scraped_count', 0):,} 件 / 保存 {run.get('saved_count', 0):,} 件 (新規 {run.get('new_count', 0):,} 件)"
                )
                # スキップ情報
                dup = run.get('duplicate_count', 0)
                exist = run.get('existing_count', 0)
                if dup > 0 or exist > 0:
                    skip_parts = []
                    if exist > 0:
                        skip_parts.append(f"既存{exist}")
                    if dup > 0:
                        skip_parts.append(f"重複{dup}")
                    lines.append(f"      スキップ: {'+'.join(skip_parts)} 件")
                elapsed = run.get('elapsed_time') or 0
                lines.append(f"      所要時間: {self._format_elapsed_time(elapsed)}")

            lines.append(f"DB累計: {stats.get('total_jobs', 0):,} 件")
            lines.append(f"新着: {stats.get('new_jobs', 0):,} 件")
            lines.append(f"DBサイズ: {stats.get('db_size_mb', 0):.1f} MB")

            self.stats_label.setText("\n".join(lines))
        except Exception as e:
            self.stats_label.setText(f"エラー: {e}")

    def get_selected_sources(self) -> List[str]:
        """選択された媒体を取得"""
        sources = []
        if self.townwork_check.isChecked():
            sources.append("townwork")
        if self.indeed_check.isChecked():
            sources.append("indeed")
        if self.baitoru_check.isChecked():
            sources.append("baitoru")
        if self.hellowork_check.isChecked():
            sources.append("hellowork")
        return sources

    def start_crawl(self):
        """クローリングを開始"""
        # 選択された媒体を取得
        sources = self.get_selected_sources()
        if not sources:
            QMessageBox.warning(self, "警告", "対象媒体を1つ以上選択してください")
            return

        # 選択されたキーワードを取得
        keywords = self.get_selected_keywords()
        if not keywords:
            QMessageBox.warning(self, "警告", "キーワードを1つ以上選択してください")
            return

        # 選択された地域を取得
        areas = self.get_selected_areas()
        if not areas:
            QMessageBox.warning(self, "警告", "地域を1つ以上選択してください")
            return

        max_pages = self.max_pages_spin.value()
        parallel = self.parallel_spin.value()

        # 媒体名リスト（表示用）
        source_names = []
        if "townwork" in sources:
            source_names.append("タウンワーク")
        if "indeed" in sources:
            source_names.append("Indeed")
        if "baitoru" in sources:
            source_names.append("バイトル")
        if "hellowork" in sources:
            source_names.append("ハローワーク")

        # 確認ダイアログ
        total_combinations = len(keywords) * len(areas) * len(sources)
        if total_combinations > 10:
            reply = QMessageBox.question(
                self, "確認",
                f"媒体: {', '.join(source_names)}\n"
                f"キーワード: {len(keywords)}個\n地域: {len(areas)}個\n"
                f"合計 {total_combinations} パターンを検索します。\n\n"
                f"処理に時間がかかる可能性があります。続行しますか？",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if reply == QMessageBox.StandardButton.No:
                return

        self.search_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.progress_label.setVisible(True)
        self.progress_label.setText("クローリング開始中...")
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, total_combinations)
        self.progress_bar.setValue(0)
        # 抽出中の初期表示をnew_count_labelに設定
        self.new_count_label.setText("抽出中... 0 件取得済み")
        self.new_count_label.setStyleSheet("""
            QLabel {
                color: #1565c0;
                font-weight: bold;
                font-size: 14px;
                padding: 10px;
                background-color: #e3f2fd;
                border: 2px solid #1976d2;
                border-radius: 6px;
            }
        """)
        self.statusBar.showMessage(f"クローリング中... ({', '.join(source_names)} / {len(keywords)}キーワード x {len(areas)}地域)")

        self.crawl_worker = CrawlWorker(self.service, keywords, areas, max_pages, parallel, sources)
        self.crawl_worker.finished.connect(self.on_crawl_finished)
        self.crawl_worker.progress.connect(self.on_crawl_progress)
        self.crawl_worker.error.connect(self.on_crawl_error)
        self.crawl_worker.time_update.connect(self.on_time_update)
        self.crawl_worker.detail_progress.connect(self.on_detail_progress)
        self.crawl_worker.stopped.connect(self.on_crawl_stopped)
        self.crawl_worker.realtime_update.connect(self.on_realtime_update)
        self.crawl_worker.start()

    def stop_crawl(self):
        """クローリングを停止"""
        if hasattr(self, 'crawl_worker') and self.crawl_worker.isRunning():
            self.stop_btn.setEnabled(False)
            self.stop_btn.setText("停止中...")
            self.progress_label.setText("停止処理中... 現在の検索が完了するまでお待ちください")
            self.crawl_worker.request_stop()

    def on_crawl_stopped(self):
        """クローリング停止完了"""
        self.search_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.stop_btn.setText("停止")
        self.progress_label.setText("停止しました")
        self.statusBar.showMessage("クローリングを停止しました")

    def on_crawl_progress(self, message: str, current: int, total: int):
        """クローリング進捗更新"""
        self.progress_label.setText(f"{message} ({current}/{total})")
        self.progress_bar.setValue(current)
        self.statusBar.showMessage(message)

    def on_time_update(self, elapsed_time: float, scraped_count: int, saved_count: int):
        """時間計測更新"""
        time_str = self._format_elapsed_time(elapsed_time)

        # 抽出件数をnew_count_labelにリアルタイム表示
        self.new_count_label.setText(f"一覧取得完了: {scraped_count} 件（{time_str}）")
        self.new_count_label.setStyleSheet("""
            QLabel {
                color: #1565c0;
                font-weight: bold;
                font-size: 14px;
                padding: 10px;
                background-color: #e3f2fd;
                border: 2px solid #1976d2;
                border-radius: 6px;
            }
        """)

    def on_detail_progress(self, current_detail: int, total_details: int, total_scraped: int):
        """詳細取得進捗更新"""
        # 詳細取得の進捗をnew_count_labelにリアルタイム表示
        self.new_count_label.setText(f"詳細取得中... {current_detail}/{total_details} 件（全{total_scraped}件中）")
        self.new_count_label.setStyleSheet("""
            QLabel {
                color: #1565c0;
                font-weight: bold;
                font-size: 14px;
                padding: 10px;
                background-color: #e3f2fd;
                border: 2px solid #1976d2;
                border-radius: 6px;
            }
        """)

    def on_realtime_update(self, elapsed_time: float, scraped_count: int, current_task: str):
        """リアルタイム進捗更新（一覧取得中）"""
        time_str = self._format_elapsed_time(elapsed_time)
        task_info = f" - {current_task}" if current_task else ""
        self.new_count_label.setText(f"抽出中... {scraped_count} 件取得済み（{time_str}）{task_info}")
        self.new_count_label.setStyleSheet("""
            QLabel {
                color: #e65100;
                font-weight: bold;
                font-size: 14px;
                padding: 10px;
                background-color: #fff3e0;
                border: 2px solid #ff9800;
                border-radius: 6px;
            }
        """)

    def on_crawl_finished(self, result: dict):
        """クローリング完了"""
        self.search_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.stop_btn.setText("停止")
        self.progress_label.setVisible(False)
        self.progress_bar.setVisible(False)
        self.time_label.setVisible(False)  # 完了時は非表示（他の情報と重複するため）

        # 最終クローリング日時を更新
        now = datetime.now()
        self.last_crawl_label.setText(f"最終クローリング日時：{now.strftime('%Y/%m/%d %H:%M')}")

        # 最終的な時間情報を表示
        elapsed_time = result.get('elapsed_time', 0)
        scraped_count = result.get('scraped_count', result.get('total_count', 0))
        saved_count = result.get('saved_count', scraped_count)
        new_count = result.get('new_count', 0)
        duplicate_count = result.get('duplicate_count', 0)
        existing_count = result.get('existing_count', 0)
        time_str = self._format_elapsed_time(elapsed_time)

        # スキップ情報のテキスト作成
        skip_info = ""
        if existing_count > 0 or duplicate_count > 0:
            skip_parts = []
            if existing_count > 0:
                skip_parts.append(f"既存{existing_count}")
            if duplicate_count > 0:
                skip_parts.append(f"重複{duplicate_count}")
            skip_info = f"（スキップ: {'+'.join(skip_parts)}件）"

        # 新規取得件数ラベルを更新（目立つ表示）- 所要時間も含める
        self.new_count_label.setText(f"今回の新規取得：{new_count} 件（抽出：{scraped_count} 件 / {time_str}）{skip_info}")
        # 新規が0件なら灰色、あれば緑色に
        if new_count > 0:
            self.new_count_label.setStyleSheet("""
                QLabel {
                    color: #1b5e20;
                    font-weight: bold;
                    font-size: 14px;
                    padding: 10px;
                    background-color: #ffffff;
                    border: 2px solid #2e7d32;
                    border-radius: 6px;
                }
            """)
        else:
            self.new_count_label.setStyleSheet("""
                QLabel {
                    color: #616161;
                    font-weight: bold;
                    font-size: 14px;
                    padding: 10px;
                    background-color: #ffffff;
                    border: 2px solid #757575;
                    border-radius: 6px;
                }
            """)

        # 今回のスクレイピング結果のみを表示（過去のDB結果は含めない）
        jobs = result.get('jobs', [])
        self.current_jobs = jobs
        self.update_results_table(jobs)
        self.load_stats({
            'scraped_count': scraped_count,
            'saved_count': saved_count,
            'new_count': new_count,
            'duplicate_count': duplicate_count,
            'existing_count': existing_count,
            'elapsed_time': elapsed_time,
        })

        msg = f"完了: 保存 {saved_count}件 (取得 {scraped_count}件), 所要時間: {time_str}"
        self.statusBar.showMessage(msg)

        if result.get('error'):
            QMessageBox.warning(self, "警告", f"エラーが発生しました: {result['error']}")

    def on_crawl_error(self, error_msg: str):
        """クローリングエラー"""
        self.search_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.stop_btn.setText("停止")
        self.progress_label.setVisible(False)
        self.progress_bar.setVisible(False)
        self.time_label.setVisible(False)
        self.statusBar.showMessage(f"エラー: {error_msg}")
        QMessageBox.critical(self, "エラー", f"クローリングエラー: {error_msg}")

    def update_results_table(self, jobs: list):
        """結果テーブルを更新"""
        self.results_table.setRowCount(len(jobs))

        for row, job in enumerate(jobs):
            company = job.get('company_name') or job.get('company', '')
            title = job.get('job_title') or job.get('title', '')
            location = job.get('work_location') or job.get('location', '')
            salary = job.get('salary', '')
            employment_type = job.get('employment_type', '')
            page_url = job.get('page_url') or job.get('url', '')

            self.results_table.setItem(row, 0, QTableWidgetItem(company))
            self.results_table.setItem(row, 1, QTableWidgetItem(title))
            self.results_table.setItem(row, 2, QTableWidgetItem(location))
            self.results_table.setItem(row, 3, QTableWidgetItem(salary))
            self.results_table.setItem(row, 4, QTableWidgetItem(employment_type))
            self.results_table.setItem(row, 5, QTableWidgetItem(page_url[:50]))

            crawled_at = job.get('crawled_at', '')
            if crawled_at and hasattr(crawled_at, 'strftime'):
                crawled_at = crawled_at.strftime('%Y-%m-%d %H:%M')
            self.results_table.setItem(row, 6, QTableWidgetItem(str(crawled_at)))

        self.result_count_label.setText(f"{len(jobs):,} 件")

    def update_filtered_table(self, jobs: list):
        """フィルタ後テーブルを更新"""
        self.filtered_table.setRowCount(len(jobs))

        for row, job in enumerate(jobs):
            company = job.get('company_name') or job.get('company', '')
            title = job.get('job_title') or job.get('title', '')
            location = job.get('work_location') or job.get('location', '')
            salary = job.get('salary', '')
            employment_type = job.get('employment_type', '')
            page_url = job.get('page_url') or job.get('url', '')

            self.filtered_table.setItem(row, 0, QTableWidgetItem(company))
            self.filtered_table.setItem(row, 1, QTableWidgetItem(title))
            self.filtered_table.setItem(row, 2, QTableWidgetItem(location))
            self.filtered_table.setItem(row, 3, QTableWidgetItem(salary))
            self.filtered_table.setItem(row, 4, QTableWidgetItem(employment_type))
            self.filtered_table.setItem(row, 5, QTableWidgetItem(page_url[:50]))

            crawled_at = job.get('crawled_at', '')
            if crawled_at and hasattr(crawled_at, 'strftime'):
                crawled_at = crawled_at.strftime('%Y-%m-%d %H:%M')
            self.filtered_table.setItem(row, 6, QTableWidgetItem(str(crawled_at)))

        self.filtered_count_label.setText(f"フィルタ後: {len(jobs):,} 件")

    def apply_filter(self):
        """選択されたフィルタを適用"""
        if not self.current_jobs:
            QMessageBox.information(self, "情報", "フィルタを適用するデータがありません")
            return

        # フィルタ設定を取得
        settings = self.get_filter_settings()

        # 追加キーワードを取得
        extra_keywords = [k.strip() for k in self.extra_keywords_input.text().split(",") if k.strip()]

        # 従業員数しきい値を取得
        employee_threshold = self.employee_threshold_spin.value()

        # カスタムフィルタを作成
        custom_filter = CustomizableJobFilter(
            enable_duplicate_phone=settings.get('duplicate_phone', True),
            enable_large_company=settings.get('large_company', True),
            enable_dispatch_keyword=settings.get('dispatch_keyword', True),
            enable_industry=settings.get('industry', True),
            enable_location_okinawa=settings.get('location_okinawa', True),
            enable_phone_prefix=settings.get('phone_prefix', True),
            extra_keywords=extra_keywords,
            large_company_threshold=employee_threshold
        )

        # フィルタ適用
        self.filter_result = custom_filter.filter_jobs(self.current_jobs)

        # 結果を表示
        self.filter_result_text.setText(self.filter_result.get_summary())
        self.update_filtered_table(self.filter_result.filtered_jobs)

        self.statusBar.showMessage(f"フィルタ適用完了: {len(self.filter_result.filtered_jobs):,}件")

    def export_csv(self):
        """CSVエクスポート（全件）"""
        if not self.current_jobs:
            QMessageBox.information(self, "情報", "エクスポートするデータがありません")
            return

        try:
            # 選択されたキーワードと地域を取得
            keywords = self.get_selected_keywords()
            areas = self.get_selected_areas()
            keyword_str = "_".join(keywords[:3]) if keywords else "all"
            area_str = "_".join(areas[:3]) if areas else "all"

            csv_path = self.service.export_to_csv(
                self.current_jobs,
                keyword=keyword_str,
                area=area_str
            )
            self.show_export_success_dialog(csv_path)
        except Exception as e:
            QMessageBox.critical(self, "エラー", f"CSV出力エラー: {e}")

    def export_filtered_csv(self):
        """CSVエクスポート（フィルタ済み）"""
        if not self.filter_result or not self.filter_result.filtered_jobs:
            QMessageBox.information(self, "情報", "フィルタを適用してからエクスポートしてください")
            return

        try:
            # 選択されたキーワードと地域を取得
            keywords = self.get_selected_keywords()
            areas = self.get_selected_areas()
            keyword_str = "_".join(keywords[:3]) if keywords else "all"
            area_str = "_".join(areas[:3]) if areas else "all"

            csv_path = self.service.export_to_csv(
                self.filter_result.filtered_jobs,
                keyword=keyword_str + "_filtered",
                area=area_str
            )
            self.show_export_success_dialog(csv_path)
        except Exception as e:
            QMessageBox.critical(self, "エラー", f"CSV出力エラー: {e}")

    def show_export_success_dialog(self, csv_path: str):
        """CSVエクスポート成功ダイアログを表示（フォルダを開くボタン付き）"""
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle("エクスポート完了")
        msg_box.setText("CSVを出力しました")
        msg_box.setInformativeText(csv_path)
        msg_box.setIcon(QMessageBox.Icon.Information)

        # ボタンを追加
        open_folder_btn = msg_box.addButton("フォルダを開く", QMessageBox.ButtonRole.ActionRole)
        msg_box.addButton("OK", QMessageBox.ButtonRole.AcceptRole)

        msg_box.exec()

        # フォルダを開くボタンが押された場合
        if msg_box.clickedButton() == open_folder_btn:
            self.open_folder(csv_path)

    def open_folder(self, file_path: str):
        """ファイルが存在するフォルダを開く"""
        try:
            folder_path = Path(file_path).parent
            system = platform.system()

            if system == "Darwin":  # macOS
                subprocess.run(["open", str(folder_path)])
            elif system == "Windows":
                subprocess.run(["explorer", str(folder_path)])
            else:  # Linux
                subprocess.run(["xdg-open", str(folder_path)])
        except Exception as e:
            QMessageBox.warning(self, "警告", f"フォルダを開けませんでした: {e}")


class CustomizableJobFilter(JobFilter):
    """カスタマイズ可能なフィルタ"""

    def __init__(
        self,
        enable_duplicate_phone: bool = True,
        enable_large_company: bool = True,
        enable_dispatch_keyword: bool = True,
        enable_industry: bool = True,
        enable_location_okinawa: bool = True,
        enable_phone_prefix: bool = True,
        extra_keywords: List[str] = None,
        large_company_threshold: int = 1001
    ):
        super().__init__(
            exclude_keywords=extra_keywords,
            large_company_threshold=large_company_threshold
        )

        self.enable_duplicate_phone = enable_duplicate_phone
        self.enable_large_company = enable_large_company
        self.enable_dispatch_keyword = enable_dispatch_keyword
        self.enable_industry = enable_industry
        self.enable_location_okinawa = enable_location_okinawa
        self.enable_phone_prefix = enable_phone_prefix

    def filter_jobs(self, jobs: List[Dict]) -> FilterResult:
        """選択されたフィルタのみ適用"""
        result = FilterResult(total_count=len(jobs))

        # Step 1: 電話番号重複削除
        if self.enable_duplicate_phone:
            jobs, dup_count = self._remove_phone_duplicates(jobs)
            result.duplicate_phone_count = dup_count
        else:
            result.duplicate_phone_count = 0

        filtered_jobs = []
        for job in jobs:
            exclude_reason = self._check_exclusion_custom(job)
            if exclude_reason:
                if "従業員数" in exclude_reason:
                    result.large_company_count += 1
                elif "派遣" in exclude_reason or "紹介" in exclude_reason or "キーワード" in exclude_reason:
                    result.dispatch_keyword_count += 1
                elif "業界" in exclude_reason:
                    result.industry_count += 1
                elif "沖縄" in exclude_reason or "勤務地" in exclude_reason:
                    result.location_count += 1
                elif "電話番号" in exclude_reason:
                    result.phone_prefix_count += 1

                job['is_filtered'] = True
                job['filter_reason'] = exclude_reason
            else:
                filtered_jobs.append(job)

        result.filtered_jobs = filtered_jobs
        result.excluded_count = result.total_count - len(filtered_jobs)

        return result

    def _check_exclusion_custom(self, job: Dict) -> Optional[str]:
        """選択されたフィルタのみで除外チェック"""

        # 従業員数フィルタ
        if self.enable_large_company:
            employee_count = job.get('employee_count')
            if employee_count and employee_count >= self.large_company_threshold:
                return f"従業員数{employee_count}人"

        company_name = job.get('company_name', job.get('company', ''))
        business_desc = job.get('business_description', job.get('business_content', ''))
        combined_text = f"{company_name} {business_desc}"

        # 派遣・紹介キーワードフィルタ
        if self.enable_dispatch_keyword:
            for keyword in self.exclude_keywords:
                if keyword in combined_text:
                    return f"除外キーワード（{keyword}）"

            # 雇用形態・タイトル・職種・その他フィールドに「派遣」が含まれる場合も除外
            employment_type = job.get('employment_type', '') or job.get('雇用形態', '') or ''
            title = job.get('title', '') or job.get('job_title', '') or ''
            job_type = job.get('job_type', '') or job.get('職種', '') or ''
            working_style = job.get('working_style', '') or job.get('勤務形態', '') or ''
            job_description = job.get('job_description', '') or job.get('仕事内容', '') or ''

            # 派遣関連のキーワード
            dispatch_keywords = ['派遣', '派遣社員', '無期雇用派遣', '登録型派遣']

            fields_to_check = [
                ('雇用形態', employment_type),
                ('タイトル', title),
                ('職種', job_type),
                ('勤務形態', working_style),
            ]

            for field_name, field_value in fields_to_check:
                if field_value:
                    for dispatch_kw in dispatch_keywords:
                        if dispatch_kw in field_value:
                            return f"{field_name}に派遣（{field_value}）"

            # 仕事内容の冒頭に「派遣」が含まれる場合も除外
            if job_description:
                desc_start = job_description[:50]
                for dispatch_kw in dispatch_keywords:
                    if dispatch_kw in desc_start:
                        return f"仕事内容冒頭に派遣"

        # 業界フィルタ（企業名のみをチェック、事業内容はチェックしない）
        # 事業内容に「広告」が含まれるだけで除外されないようにする
        if self.enable_industry:
            for industry in self.exclude_industries:
                if industry in company_name:
                    return f"除外業界（{industry}）"

        # 勤務地フィルタ（沖縄）
        if self.enable_location_okinawa:
            address_pref = job.get('address_pref', '')
            work_location = job.get('work_location', job.get('location', ''))
            location_text = f"{address_pref} {work_location}"

            for location in self.exclude_locations:
                if location in location_text:
                    return f"除外勤務地（{location}）"

        # 電話番号プレフィックスフィルタ
        if self.enable_phone_prefix:
            phone = job.get('phone_number_normalized', '')
            if phone:
                for prefix in self.exclude_phone_prefixes:
                    if phone.startswith(prefix):
                        return f"除外電話番号（{prefix}）"

        return None


def main():
    """アプリケーションを起動"""
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    app.setStyleSheet(MODERN_STYLE)

    logging.basicConfig(level=logging.INFO)

    window = MainWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
