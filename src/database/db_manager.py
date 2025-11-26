"""
SQLiteデータベース管理
要件定義 12.2 主要テーブル定義に準拠
"""
import sqlite3
from pathlib import Path
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class DatabaseManager:
    """SQLiteデータベース管理クラス"""

    def __init__(self, db_path: str = "data/db/jobs.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_database()

    def _init_database(self):
        """データベースを初期化"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 媒体マスタテーブル
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name VARCHAR(50) NOT NULL UNIQUE,
                    display_name VARCHAR(100),
                    base_url VARCHAR(200),
                    is_active BOOLEAN DEFAULT 1,
                    priority INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 求人情報テーブル
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id VARCHAR(100) NOT NULL,
                    source_id INTEGER NOT NULL,
                    company_name VARCHAR(200) NOT NULL,
                    company_name_kana VARCHAR(200),
                    postal_code VARCHAR(8),
                    address_pref VARCHAR(10),
                    address_city VARCHAR(50),
                    address_detail VARCHAR(200),
                    phone_number VARCHAR(20),
                    phone_number_normalized VARCHAR(15),
                    fax_number VARCHAR(20),
                    job_title VARCHAR(200) NOT NULL,
                    employment_type VARCHAR(50),
                    salary VARCHAR(100),
                    salary_min INTEGER,
                    salary_max INTEGER,
                    working_hours VARCHAR(200),
                    holidays VARCHAR(500),
                    work_location VARCHAR(500),
                    business_description TEXT,
                    job_description TEXT,
                    requirements TEXT,
                    hiring_count INTEGER,
                    contact_person VARCHAR(100),
                    contact_email VARCHAR(200),
                    page_url VARCHAR(500) NOT NULL,
                    employee_count INTEGER,
                    established_year INTEGER,
                    capital BIGINT,
                    posted_date DATE,
                    expire_date DATE,
                    crawled_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL,
                    is_new BOOLEAN DEFAULT 1,
                    is_filtered BOOLEAN DEFAULT 0,
                    filter_reason VARCHAR(100),
                    UNIQUE(source_id, job_id),
                    FOREIGN KEY (source_id) REFERENCES sources(id)
                )
            """)

            # クロールログテーブル
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS crawl_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id INTEGER NOT NULL,
                    keyword VARCHAR(100),
                    area VARCHAR(100),
                    status VARCHAR(20) NOT NULL,
                    total_count INTEGER DEFAULT 0,
                    new_count INTEGER DEFAULT 0,
                    error_message TEXT,
                    started_at DATETIME,
                    finished_at DATETIME,
                    FOREIGN KEY (source_id) REFERENCES sources(id)
                )
            """)

            # 検索条件保存テーブル
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS search_conditions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name VARCHAR(100) NOT NULL,
                    conditions TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # インデックス作成
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_phone ON jobs(phone_number_normalized)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_crawled ON jobs(crawled_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_pref ON jobs(address_pref)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_new ON jobs(is_new)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_filtered ON jobs(is_filtered)")

            # デフォルト媒体を登録
            self._insert_default_sources(cursor)

            conn.commit()
            logger.info(f"Database initialized: {self.db_path}")

    def _insert_default_sources(self, cursor):
        """デフォルトの媒体マスタを登録"""
        sources = [
            ("townwork", "タウンワーク", "https://townwork.net", 1, 3),
            ("indeed", "Indeed", "https://jp.indeed.com", 1, 1),
            ("hellowork", "ハローワーク", "https://www.hellowork.mhlw.go.jp", 1, 2),
            ("baitoru", "バイトル", "https://www.baitoru.com", 1, 4),
            ("mahhabaito", "マッハバイト", "https://j-sen.jp", 1, 5),
            ("linebaito", "LINEバイト", "https://baito.line.me", 1, 6),
            ("rikunavi", "リクナビ", "https://job.rikunabi.com", 1, 7),
            ("mynavi", "マイナビ", "https://tenshoku.mynavi.jp", 1, 8),
            ("entenshoku", "エン転職", "https://employment.en-japan.com", 1, 9),
            ("kaigojob", "カイゴジョブ", "https://www.kaigojob.com", 1, 10),
            ("jobmedley", "ジョブメドレー", "https://job-medley.com", 1, 11),
        ]

        for name, display_name, base_url, is_active, priority in sources:
            cursor.execute("""
                INSERT OR IGNORE INTO sources (name, display_name, base_url, is_active, priority)
                VALUES (?, ?, ?, ?, ?)
            """, (name, display_name, base_url, is_active, priority))

    def get_connection(self) -> sqlite3.Connection:
        """データベース接続を取得"""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def get_source_id(self, source_name: str) -> int:
        """媒体名からIDを取得"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM sources WHERE name = ?", (source_name,))
            row = cursor.fetchone()
            return row['id'] if row else None

    def get_all_sources(self) -> list:
        """全媒体を取得"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM sources ORDER BY priority")
            return [dict(row) for row in cursor.fetchall()]

    def get_db_stats(self) -> dict:
        """データベース統計を取得"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            stats = {}

            # 総求人数
            cursor.execute("SELECT COUNT(*) as count FROM jobs")
            stats['total_jobs'] = cursor.fetchone()['count']

            # 新着数
            cursor.execute("SELECT COUNT(*) as count FROM jobs WHERE is_new = 1")
            stats['new_jobs'] = cursor.fetchone()['count']

            # フィルタ済み数
            cursor.execute("SELECT COUNT(*) as count FROM jobs WHERE is_filtered = 1")
            stats['filtered_jobs'] = cursor.fetchone()['count']

            # 媒体別件数
            cursor.execute("""
                SELECT s.display_name, COUNT(j.id) as count
                FROM sources s
                LEFT JOIN jobs j ON s.id = j.source_id
                GROUP BY s.id
                ORDER BY s.priority
            """)
            stats['by_source'] = {row['display_name']: row['count'] for row in cursor.fetchall()}

            # DBファイルサイズ
            stats['db_size_mb'] = self.db_path.stat().st_size / (1024 * 1024) if self.db_path.exists() else 0

            return stats
