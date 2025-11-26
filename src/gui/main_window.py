"""
メインウィンドウ
要件定義 11章 UI/UX設計に準拠
"""
import sys
import asyncio
import subprocess
import platform
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict
import logging

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QLineEdit, QComboBox, QCheckBox,
    QTableWidget, QTableWidgetItem, QProgressBar, QStatusBar,
    QGroupBox, QSpinBox, QTextEdit, QTabWidget, QMessageBox,
    QFileDialog, QHeaderView, QSplitter, QFrame, QScrollArea,
    QGridLayout
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QFont, QColor

# パス設定
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.services.crawl_service import CrawlService
from src.filters.job_filter import JobFilter, FilterResult

logger = logging.getLogger(__name__)


class CrawlWorker(QThread):
    """クローリングワーカースレッド"""
    finished = pyqtSignal(dict)
    progress = pyqtSignal(str, int, int)
    error = pyqtSignal(str)

    def __init__(self, service: CrawlService, keywords: List[str], areas: List[str], max_pages: int):
        super().__init__()
        self.service = service
        self.keywords = keywords
        self.areas = areas
        self.max_pages = max_pages

    def run(self):
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            result = loop.run_until_complete(
                self.service.crawl_townwork(
                    keywords=self.keywords,
                    areas=self.areas,
                    max_pages=self.max_pages
                )
            )

            loop.close()
            self.finished.emit(result)

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
        self.setWindowTitle("求人情報自動収集システム - タウンワーク")
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

        # 初期の幅比率を設定（左:右 = 1:2）
        self.main_splitter.setSizes([400, 800])

        main_layout.addWidget(self.main_splitter)

        # ステータスバー
        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)
        self.statusBar.showMessage("準備完了")

    def create_left_panel(self) -> QWidget:
        """左パネル（検索条件 + フィルタ設定）を作成"""
        # スクロールエリアで包む
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        scroll.setMinimumWidth(350)

        # パネルウィジェット
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setSpacing(5)
        layout.setContentsMargins(5, 5, 5, 5)

        # 媒体選択
        source_group = QGroupBox("対象媒体")
        source_layout = QVBoxLayout(source_group)
        self.townwork_check = QCheckBox("タウンワーク")
        self.townwork_check.setChecked(True)
        self.townwork_check.setEnabled(False)
        source_layout.addWidget(self.townwork_check)
        layout.addWidget(source_group)

        # 地域選択とキーワード選択をスプリッターで配置（高さ可変）
        selection_splitter = QSplitter(Qt.Orientation.Vertical)
        selection_splitter.setChildrenCollapsible(False)

        # 地域選択
        area_group = self.create_area_selection_group()
        selection_splitter.addWidget(area_group)

        # キーワード選択
        keyword_group = self.create_keyword_selection_group()
        selection_splitter.addWidget(keyword_group)

        # 初期サイズを設定（地域:キーワード = 1:1）
        selection_splitter.setSizes([250, 250])

        layout.addWidget(selection_splitter, 1)

        # フィルタ設定（アコーディオン - 折りたたみ可能）
        filter_accordion = self.create_filter_accordion()
        layout.addWidget(filter_accordion)

        # 検索オプション
        option_group = QGroupBox("検索オプション")
        option_layout = QHBoxLayout(option_group)

        option_layout.addWidget(QLabel("最大ページ数:"))
        self.max_pages_spin = QSpinBox()
        self.max_pages_spin.setRange(1, 20)
        self.max_pages_spin.setValue(5)
        option_layout.addWidget(self.max_pages_spin)
        option_layout.addStretch()

        layout.addWidget(option_group)

        # 実行ボタン
        self.search_btn = QPushButton("検索実行")
        self.search_btn.setStyleSheet("""
            QPushButton {
                background-color: #1f77b4;
                color: white;
                padding: 10px;
                font-size: 14px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #1a5a8a;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        self.search_btn.clicked.connect(self.start_crawl)
        layout.addWidget(self.search_btn)

        # 進捗バー
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)

        # 統計情報
        stats_group = QGroupBox("統計情報")
        stats_layout = QVBoxLayout(stats_group)
        self.stats_label = QLabel("読み込み中...")
        stats_layout.addWidget(self.stats_label)
        layout.addWidget(stats_group)

        scroll.setWidget(panel)
        return scroll

    def create_area_selection_group(self) -> QGroupBox:
        """地域選択グループを作成"""
        group = QGroupBox("地域選択（複数選択可）")
        group.setMinimumHeight(200)
        layout = QVBoxLayout(group)
        layout.setContentsMargins(5, 10, 5, 5)
        layout.setSpacing(5)

        # 全選択/全解除ボタン
        btn_layout = QHBoxLayout()
        select_all_btn = QPushButton("全選択")
        select_all_btn.clicked.connect(self.select_all_areas)
        deselect_all_btn = QPushButton("全解除")
        deselect_all_btn.clicked.connect(self.deselect_all_areas)
        btn_layout.addWidget(select_all_btn)
        btn_layout.addWidget(deselect_all_btn)
        layout.addLayout(btn_layout)

        # 地方別選択ボタン（トグル式：押すたびに選択/解除を切り替え）
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

        # 地方ボタンを2行に配置
        region_grid = QGridLayout()
        region_grid.setSpacing(3)
        self.region_buttons: Dict[str, QPushButton] = {}

        for i, (region_name, prefs) in enumerate(self.region_definitions):
            btn = QPushButton(region_name)
            btn.setCheckable(True)  # トグルボタンにする
            btn.setStyleSheet("""
                QPushButton {
                    padding: 3px 8px;
                    font-size: 11px;
                }
                QPushButton:checked {
                    background-color: #4CAF50;
                    color: white;
                }
            """)
            btn.clicked.connect(lambda checked, p=prefs, b=btn: self.toggle_areas_by_region(p, b))
            self.region_buttons[region_name] = btn
            region_grid.addWidget(btn, i // 4, i % 4)

        layout.addLayout(region_grid)

        # 47都道府県一覧
        all_prefectures = [
            # 北海道・東北
            ("北海道", False), ("青森", False), ("岩手", False), ("宮城", False),
            ("秋田", False), ("山形", False), ("福島", False),
            # 関東
            ("茨城", False), ("栃木", False), ("群馬", False), ("埼玉", False),
            ("千葉", False), ("東京", True), ("神奈川", False),
            # 中部
            ("新潟", False), ("富山", False), ("石川", False), ("福井", False),
            ("山梨", False), ("長野", False), ("岐阜", False), ("静岡", False),
            ("愛知", False),
            # 近畿
            ("三重", False), ("滋賀", False), ("京都", False), ("大阪", False),
            ("兵庫", False), ("奈良", False), ("和歌山", False),
            # 中国
            ("鳥取", False), ("島根", False), ("岡山", False), ("広島", False),
            ("山口", False),
            # 四国
            ("徳島", False), ("香川", False), ("愛媛", False), ("高知", False),
            # 九州・沖縄
            ("福岡", False), ("佐賀", False), ("長崎", False), ("熊本", False),
            ("大分", False), ("宮崎", False), ("鹿児島", False), ("沖縄", False),
        ]

        # スクロール可能なエリア
        self.area_scroll = QScrollArea()
        self.area_scroll.setWidgetResizable(True)
        self.area_scroll.setMinimumHeight(60)

        area_widget = QWidget()
        grid = QGridLayout(area_widget)
        grid.setSpacing(3)
        grid.setContentsMargins(5, 5, 5, 5)

        for i, (area, default) in enumerate(all_prefectures):
            check = QCheckBox(area)
            check.setChecked(default)
            self.area_checks[area] = check
            grid.addWidget(check, i // 4, i % 4)  # 4列で表示

        self.area_scroll.setWidget(area_widget)
        layout.addWidget(self.area_scroll, 1)  # stretch factor 1

        return group

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

    def create_keyword_selection_group(self) -> QGroupBox:
        """キーワード選択グループを作成"""
        group = QGroupBox("キーワード選択（複数選択可）")
        group.setMinimumHeight(180)
        layout = QVBoxLayout(group)
        layout.setContentsMargins(5, 10, 5, 5)
        layout.setSpacing(5)

        # 全選択/全解除ボタン
        btn_layout = QHBoxLayout()
        select_all_btn = QPushButton("全選択")
        select_all_btn.clicked.connect(self.select_all_keywords)
        deselect_all_btn = QPushButton("全解除")
        deselect_all_btn.clicked.connect(self.deselect_all_keywords)
        btn_layout.addWidget(select_all_btn)
        btn_layout.addWidget(deselect_all_btn)
        layout.addLayout(btn_layout)

        # キーワード一覧（全量）
        keywords = [
            # IT関連
            ("IT", True), ("エンジニア", False), ("プログラマー", False),
            ("SE", False), ("Web", False), ("システム", False),
            # 事務系
            ("事務", False), ("経理", False), ("総務", False),
            ("人事", False), ("秘書", False), ("受付", False),
            # 営業系
            ("営業", False), ("販売", False), ("接客", False),
            ("店長", False), ("マネージャー", False),
            # 製造・物流
            ("製造", False), ("工場", False), ("物流", False),
            ("倉庫", False), ("ドライバー", False), ("配送", False),
            # 医療・介護
            ("看護", False), ("介護", False), ("医療", False),
            ("保育", False), ("薬剤師", False),
            # 飲食・サービス
            ("飲食", False), ("調理", False), ("ホール", False),
            ("清掃", False), ("警備", False),
            # その他
            ("デザイン", False), ("企画", False), ("マーケティング", False),
            ("コールセンター", False), ("データ入力", False), ("軽作業", False),
        ]

        # スクロール可能なエリア
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setMinimumHeight(60)

        keyword_widget = QWidget()
        grid = QGridLayout(keyword_widget)
        grid.setSpacing(3)
        grid.setContentsMargins(5, 5, 5, 5)

        for i, (keyword, default) in enumerate(keywords):
            check = QCheckBox(keyword)
            check.setChecked(default)
            self.keyword_checks[keyword] = check
            grid.addWidget(check, i // 3, i % 3)

        scroll.setWidget(keyword_widget)
        layout.addWidget(scroll, 1)  # stretch factor 1

        # カスタムキーワード入力（固定高さ部分）
        custom_widget = QWidget()
        custom_layout = QVBoxLayout(custom_widget)
        custom_layout.setContentsMargins(0, 5, 0, 0)
        custom_layout.setSpacing(3)
        custom_layout.addWidget(QLabel("追加キーワード（カンマ区切り）:"))
        self.custom_keyword_input = QLineEdit()
        self.custom_keyword_input.setPlaceholderText("例: コンサル, マネジメント")
        custom_layout.addWidget(self.custom_keyword_input)
        layout.addWidget(custom_widget)

        return group

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
        custom = self.custom_keyword_input.text()
        if custom:
            custom_keywords = [k.strip() for k in custom.split(",") if k.strip()]
            keywords.extend(custom_keywords)

        return keywords

    def create_filter_accordion(self) -> QWidget:
        """フィルタ設定のアコーディオン（折りたたみ可能）を作成"""
        container = QWidget()
        container_layout = QVBoxLayout(container)
        container_layout.setContentsMargins(0, 0, 0, 0)
        container_layout.setSpacing(0)

        # アコーディオンのヘッダーボタン
        self.filter_toggle_btn = QPushButton("▶ フィルタ設定（クリックで展開）")
        self.filter_toggle_btn.setCheckable(True)
        self.filter_toggle_btn.setChecked(False)
        self.filter_toggle_btn.setStyleSheet("""
            QPushButton {
                text-align: left;
                padding: 8px;
                background-color: #e0e0e0;
                border: 1px solid #ccc;
                border-radius: 3px;
                font-weight: bold;
            }
            QPushButton:checked {
                background-color: #d0d0d0;
            }
            QPushButton:hover {
                background-color: #d5d5d5;
            }
        """)
        self.filter_toggle_btn.clicked.connect(self.toggle_filter_accordion)
        container_layout.addWidget(self.filter_toggle_btn)

        # フィルタ設定の内容（折りたたみ可能）
        self.filter_content = QWidget()
        self.filter_content.setVisible(False)  # 初期状態は非表示
        content_layout = QVBoxLayout(self.filter_content)
        content_layout.setContentsMargins(5, 5, 5, 5)
        content_layout.setSpacing(5)

        # 全選択/全解除ボタン
        btn_layout = QHBoxLayout()
        select_all_btn = QPushButton("全選択")
        select_all_btn.clicked.connect(self.select_all_filters)
        deselect_all_btn = QPushButton("全解除")
        deselect_all_btn.clicked.connect(self.deselect_all_filters)
        btn_layout.addWidget(select_all_btn)
        btn_layout.addWidget(deselect_all_btn)
        content_layout.addLayout(btn_layout)

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
            check.setChecked(True)  # デフォルトでON
            check.setToolTip(tooltip)
            self.filter_checks[key] = check
            content_layout.addWidget(check)

        # 追加の除外キーワード入力
        content_layout.addWidget(QLabel("追加除外キーワード:"))
        self.extra_keywords_input = QLineEdit()
        self.extra_keywords_input.setPlaceholderText("カンマ区切りで入力")
        self.extra_keywords_input.setToolTip("会社名・事業内容に含まれる場合に除外")
        content_layout.addWidget(self.extra_keywords_input)

        # 従業員数しきい値
        emp_layout = QHBoxLayout()
        emp_layout.addWidget(QLabel("従業員数上限:"))
        self.employee_threshold_spin = QSpinBox()
        self.employee_threshold_spin.setRange(100, 10000)
        self.employee_threshold_spin.setValue(1001)
        self.employee_threshold_spin.setSingleStep(100)
        emp_layout.addWidget(self.employee_threshold_spin)
        emp_layout.addWidget(QLabel("人以上を除外"))
        content_layout.addLayout(emp_layout)

        container_layout.addWidget(self.filter_content)

        return container

    def toggle_filter_accordion(self):
        """フィルタアコーディオンの表示/非表示を切り替え"""
        is_expanded = self.filter_toggle_btn.isChecked()
        self.filter_content.setVisible(is_expanded)
        if is_expanded:
            self.filter_toggle_btn.setText("▼ フィルタ設定（クリックで折りたたむ）")
        else:
            self.filter_toggle_btn.setText("▶ フィルタ設定（クリックで展開）")

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
        layout.addWidget(self.results_table)

        btn_layout = QHBoxLayout()

        self.result_count_label = QLabel("0 件")
        btn_layout.addWidget(self.result_count_label)

        btn_layout.addStretch()

        self.apply_filter_btn = QPushButton("フィルタ適用")
        self.apply_filter_btn.setStyleSheet("""
            QPushButton {
                background-color: #28a745;
                color: white;
                padding: 8px 16px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #218838;
            }
        """)
        self.apply_filter_btn.clicked.connect(self.apply_filter)
        btn_layout.addWidget(self.apply_filter_btn)

        self.export_btn = QPushButton("CSVエクスポート（全件）")
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
        layout.addWidget(self.filtered_table, 2)

        btn_layout = QHBoxLayout()

        self.filtered_count_label = QLabel("フィルタ後: 0 件")
        btn_layout.addWidget(self.filtered_count_label)

        btn_layout.addStretch()

        self.export_filtered_btn = QPushButton("フィルタ済みCSVエクスポート")
        self.export_filtered_btn.setStyleSheet("""
            QPushButton {
                background-color: #007bff;
                color: white;
                padding: 8px 16px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #0056b3;
            }
        """)
        self.export_filtered_btn.clicked.connect(self.export_filtered_csv)
        btn_layout.addWidget(self.export_filtered_btn)

        layout.addLayout(btn_layout)
        return tab

    def load_stats(self):
        """統計情報を読み込み"""
        try:
            stats = self.service.get_stats()
            text = f"""総求人数: {stats.get('total_jobs', 0):,} 件
新着: {stats.get('new_jobs', 0):,} 件
DBサイズ: {stats.get('db_size_mb', 0):.1f} MB"""
            self.stats_label.setText(text)
        except Exception as e:
            self.stats_label.setText(f"エラー: {e}")

    def start_crawl(self):
        """クローリングを開始"""
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

        # 確認ダイアログ
        total_combinations = len(keywords) * len(areas)
        if total_combinations > 10:
            reply = QMessageBox.question(
                self, "確認",
                f"キーワード: {len(keywords)}個\n地域: {len(areas)}個\n"
                f"合計 {total_combinations} パターンを検索します。\n\n"
                f"処理に時間がかかる可能性があります。続行しますか？",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if reply == QMessageBox.StandardButton.No:
                return

        self.search_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)
        self.statusBar.showMessage(f"クローリング中... ({len(keywords)}キーワード x {len(areas)}地域)")

        self.crawl_worker = CrawlWorker(self.service, keywords, areas, max_pages)
        self.crawl_worker.finished.connect(self.on_crawl_finished)
        self.crawl_worker.error.connect(self.on_crawl_error)
        self.crawl_worker.start()

    def on_crawl_finished(self, result: dict):
        """クローリング完了"""
        self.search_btn.setEnabled(True)
        self.progress_bar.setVisible(False)

        jobs = self.service.job_repository.get_jobs(source_name="townwork", limit=1000)
        self.current_jobs = jobs
        self.update_results_table(jobs)
        self.load_stats()

        msg = f"完了: {result.get('total_count', 0)}件取得, {result.get('new_count', 0)}件が新着"
        self.statusBar.showMessage(msg)

        if result.get('error'):
            QMessageBox.warning(self, "警告", f"エラーが発生しました: {result['error']}")

    def on_crawl_error(self, error_msg: str):
        """クローリングエラー"""
        self.search_btn.setEnabled(True)
        self.progress_bar.setVisible(False)
        self.statusBar.showMessage(f"エラー: {error_msg}")
        QMessageBox.critical(self, "エラー", f"クローリングエラー: {error_msg}")

    def update_results_table(self, jobs: list):
        """結果テーブルを更新"""
        self.results_table.setRowCount(len(jobs))

        for row, job in enumerate(jobs):
            self.results_table.setItem(row, 0, QTableWidgetItem(job.get('company_name', '')))
            self.results_table.setItem(row, 1, QTableWidgetItem(job.get('job_title', '')))
            self.results_table.setItem(row, 2, QTableWidgetItem(job.get('work_location', '')))
            self.results_table.setItem(row, 3, QTableWidgetItem(job.get('salary', '')))
            self.results_table.setItem(row, 4, QTableWidgetItem(job.get('employment_type', '')))
            self.results_table.setItem(row, 5, QTableWidgetItem(job.get('page_url', '')[:50]))

            crawled_at = job.get('crawled_at', '')
            if crawled_at and hasattr(crawled_at, 'strftime'):
                crawled_at = crawled_at.strftime('%Y-%m-%d %H:%M')
            self.results_table.setItem(row, 6, QTableWidgetItem(str(crawled_at)))

        self.result_count_label.setText(f"{len(jobs):,} 件")

    def update_filtered_table(self, jobs: list):
        """フィルタ後テーブルを更新"""
        self.filtered_table.setRowCount(len(jobs))

        for row, job in enumerate(jobs):
            self.filtered_table.setItem(row, 0, QTableWidgetItem(job.get('company_name', '')))
            self.filtered_table.setItem(row, 1, QTableWidgetItem(job.get('job_title', '')))
            self.filtered_table.setItem(row, 2, QTableWidgetItem(job.get('work_location', '')))
            self.filtered_table.setItem(row, 3, QTableWidgetItem(job.get('salary', '')))
            self.filtered_table.setItem(row, 4, QTableWidgetItem(job.get('employment_type', '')))
            self.filtered_table.setItem(row, 5, QTableWidgetItem(job.get('page_url', '')[:50]))

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

        # 業界フィルタ
        if self.enable_industry:
            for industry in self.exclude_industries:
                if industry in combined_text:
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

    logging.basicConfig(level=logging.INFO)

    window = MainWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
