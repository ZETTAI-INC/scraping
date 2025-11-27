"""
クローリングサービス
スクレイパーとデータベース・フィルタを統合
"""
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable
import logging
import sys
import os

# パス追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from scrapers.townwork import TownworkScraper
from src.database.db_manager import DatabaseManager
from src.database.job_repository import JobRepository
from src.filters.job_filter import JobFilter, FilterResult
from src.services.csv_exporter import CSVExporter

logger = logging.getLogger(__name__)


class CrawlService:
    """クローリングサービスクラス"""

    def __init__(
        self,
        db_path: str = "data/db/jobs.db",
        output_dir: str = "data/output"
    ):
        self.db_manager = DatabaseManager(db_path)
        self.job_repository = JobRepository(self.db_manager)
        self.job_filter = JobFilter()
        self.csv_exporter = CSVExporter(output_dir)

        # スクレイパー（タウンワークのみ）
        self.scrapers = {
            "townwork": TownworkScraper,
        }

        # 進捗コールバック
        self.progress_callback: Optional[Callable[[str, int, int], None]] = None

    def set_progress_callback(self, callback: Callable[[str, int, int], None]):
        """進捗コールバックを設定"""
        self.progress_callback = callback

    def _report_progress(self, message: str, current: int = 0, total: int = 0):
        """進捗を報告"""
        if self.progress_callback:
            self.progress_callback(message, current, total)
        logger.info(f"{message} ({current}/{total})")

    async def crawl_townwork(
        self,
        keywords: List[str],
        areas: List[str],
        max_pages: int = 5,
        parallel: int = 5,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        タウンワークをクロール

        Args:
            keywords: 検索キーワードリスト
            areas: 地域リスト
            max_pages: 最大ページ数
            parallel: 並列数
            filters: 検索フィルタ

        Returns:
            クロール結果
        """
        result = {
            'source': 'townwork',
            'keywords': keywords,
            'areas': areas,
            'started_at': datetime.now(),
            'finished_at': None,
            'total_count': 0,
            'scraped_count': 0,  # 生の取得件数
            'saved_count': 0,
            'new_count': 0,
            'jobs': [],  # 今回取得した求人（UI表示用）
            'error': None,
        }

        try:
            self._report_progress("タウンワーク クローリング開始", 0, 1)

            # スクレイパー初期化
            scraper = TownworkScraper()

            # スクレイピング実行
            jobs = await scraper.scrape(
                keywords=keywords,
                areas=areas,
                max_pages=max_pages,
                parallel=parallel,
                filters=filters
            )

            result['total_count'] = len(jobs)
            result['scraped_count'] = len(jobs)
            self._report_progress(f"取得完了: {len(jobs)}件", 1, 2)

            # データベースに保存
            saved_count = 0
            new_count = 0
            saved_jobs = []
            saved_job_ids = []
            for job in jobs:
                try:
                    job['crawled_at'] = datetime.now()
                    # 既存チェック
                    existing = self._check_existing(job)
                    job_id = self.job_repository.save_job(job, "townwork")

                    saved_job_ids.append(job_id)
                    # UI表示用に整形したレコードを保持（DBから再読込しなくて済むように）
                    saved_jobs.append(self._prepare_job_record(job))

                    saved_count += 1
                    if not existing:
                        new_count += 1
                except Exception as e:
                    logger.warning(f"Failed to save job: {e}")

            result['saved_count'] = saved_count
            result['new_count'] = new_count
            if saved_job_ids:
                try:
                    result['jobs'] = self.job_repository.get_jobs_by_ids(saved_job_ids)
                except Exception as e:
                    logger.warning(f"Failed to fetch saved jobs by id: {e}")
                    result['jobs'] = saved_jobs
            else:
                result['jobs'] = saved_jobs

            self._report_progress(f"保存完了: {saved_count}件（新着: {new_count}件）", 2, 2)

            # クロールログを記録
            self._save_crawl_log(result)

        except Exception as e:
            result['error'] = str(e)
            logger.error(f"Crawl error: {e}", exc_info=True)

        result['finished_at'] = datetime.now()
        return result

    def _check_existing(self, job: Dict[str, Any]) -> bool:
        """既存の求人かチェック"""
        source_id = self.db_manager.get_source_id("townwork")
        if not source_id:
            return False

        job_identifier = job.get('job_id') or job.get('job_number')
        page_url = job.get('page_url') or job.get('url')

        if not job_identifier and not page_url:
            return False

        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT 1
                FROM jobs
                WHERE source_id = ?
                  AND (job_id = ? OR page_url = ?)
                LIMIT 1
                """,
                (source_id, job_identifier or page_url, page_url or job_identifier)
            )
            return cursor.fetchone() is not None

    def _prepare_job_record(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """テーブル表示用にキーを正規化"""
        return {
            "company_name": job.get("company_name") or job.get("company", ""),
            "job_title": job.get("job_title") or job.get("title", ""),
            "work_location": job.get("work_location") or job.get("location", ""),
            "salary": job.get("salary", ""),
            "employment_type": job.get("employment_type", ""),
            "page_url": job.get("page_url") or job.get("url", ""),
            "crawled_at": job.get("crawled_at"),
        }

    def _save_crawl_log(self, result: Dict[str, Any]):
        """クロールログを保存"""
        source_id = self.db_manager.get_source_id(result['source'])
        if not source_id:
            return

        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO crawl_logs (
                    source_id, keyword, area, status,
                    total_count, new_count, error_message,
                    started_at, finished_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                source_id,
                ','.join(result['keywords']),
                ','.join(result['areas']),
                'error' if result['error'] else 'success',
                result['total_count'],
                result['new_count'],
                result['error'],
                result['started_at'],
                result['finished_at'],
            ))
            conn.commit()

    def get_jobs_with_filter(
        self,
        source_name: Optional[str] = None,
        keyword: Optional[str] = None,
        prefecture: Optional[str] = None,
        apply_filter: bool = True
    ) -> FilterResult:
        """
        フィルタを適用して求人を取得

        Args:
            source_name: 媒体名
            keyword: キーワード
            prefecture: 都道府県
            apply_filter: フィルタを適用するか

        Returns:
            FilterResult
        """
        # データベースから取得
        jobs = self.job_repository.get_jobs(
            source_name=source_name,
            keyword=keyword,
            prefecture=prefecture,
            is_filtered=False,
            limit=10000  # 最大件数
        )

        if apply_filter:
            return self.job_filter.filter_jobs(jobs)
        else:
            # フィルタなしの場合
            return FilterResult(
                total_count=len(jobs),
                filtered_jobs=jobs,
                excluded_count=0
            )

    def export_to_csv(
        self,
        jobs: List[Dict[str, Any]],
        keyword: Optional[str] = None,
        area: Optional[str] = None
    ) -> str:
        """CSVにエクスポート"""
        output_path = self.csv_exporter.export(jobs, keyword, area)
        return str(output_path)

    def get_stats(self) -> Dict[str, Any]:
        """統計情報を取得"""
        return self.db_manager.get_db_stats()

    def get_new_jobs_count(self, source_name: Optional[str] = None) -> int:
        """新着件数を取得"""
        return self.job_repository.get_job_count(
            source_name=source_name,
            is_new=True
        )

    def cleanup_old_data(self, days: int = 90) -> int:
        """古いデータを削除"""
        deleted_count = self.job_repository.delete_old_jobs(days)
        logger.info(f"Deleted {deleted_count} old jobs")
        return deleted_count


# CLIから実行可能にする
async def main():
    """テスト実行"""
    service = CrawlService()

    # テストクロール
    result = await service.crawl_townwork(
        keywords=["IT"],
        areas=["東京"],
        max_pages=2
    )

    print(f"クロール結果: {result}")

    # フィルタ適用して取得
    filter_result = service.get_jobs_with_filter(source_name="townwork")
    print(filter_result.get_summary())

    # CSV出力
    if filter_result.filtered_jobs:
        csv_path = service.export_to_csv(
            filter_result.filtered_jobs,
            keyword="IT",
            area="東京"
        )
        print(f"CSV出力: {csv_path}")


if __name__ == "__main__":
    asyncio.run(main())
