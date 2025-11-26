"""
新着監視・スケジューラー機能
要件定義 6章 新着監視・通知機能に準拠
"""
import asyncio
from datetime import datetime, timedelta
from typing import Callable, Optional, Dict, Any, List
import logging
import threading

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger(__name__)


class SchedulerService:
    """スケジューラーサービス"""

    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.is_running = False
        self.crawl_callback: Optional[Callable] = None
        self.notification_callback: Optional[Callable[[str, str], None]] = None

        # 設定
        self.settings = {
            'interval_minutes': 60,  # デフォルト60分
            'start_hour': 6,
            'end_hour': 23,
            'enabled': False,
        }

        # 統計
        self.stats = {
            'last_crawl_at': None,
            'total_crawls': 0,
            'total_new_jobs': 0,
            'errors': 0,
        }

    def set_crawl_callback(self, callback: Callable):
        """クロールコールバックを設定"""
        self.crawl_callback = callback

    def set_notification_callback(self, callback: Callable[[str, str], None]):
        """通知コールバックを設定"""
        self.notification_callback = callback

    def configure(
        self,
        interval_minutes: int = 60,
        start_hour: int = 6,
        end_hour: int = 23
    ):
        """スケジューラーを設定"""
        self.settings['interval_minutes'] = max(30, min(1440, interval_minutes))
        self.settings['start_hour'] = max(0, min(23, start_hour))
        self.settings['end_hour'] = max(0, min(23, end_hour))

        logger.info(f"Scheduler configured: interval={interval_minutes}min, "
                   f"hours={start_hour}-{end_hour}")

    def start(self):
        """スケジューラーを開始"""
        if self.is_running:
            logger.warning("Scheduler is already running")
            return

        # ジョブを追加
        self.scheduler.add_job(
            self._scheduled_crawl,
            IntervalTrigger(minutes=self.settings['interval_minutes']),
            id='scheduled_crawl',
            name='定期クローリング',
            replace_existing=True
        )

        self.scheduler.start()
        self.is_running = True
        self.settings['enabled'] = True

        logger.info("Scheduler started")
        self._notify("スケジューラー開始", "自動クローリングを開始しました")

    def stop(self):
        """スケジューラーを停止"""
        if not self.is_running:
            return

        self.scheduler.shutdown(wait=False)
        self.is_running = False
        self.settings['enabled'] = False

        logger.info("Scheduler stopped")
        self._notify("スケジューラー停止", "自動クローリングを停止しました")

    def _scheduled_crawl(self):
        """スケジュールされたクロール実行"""
        now = datetime.now()

        # 実行時間帯のチェック
        if not self._is_within_hours(now):
            logger.debug(f"Outside crawl hours: {now.hour}")
            return

        logger.info("Starting scheduled crawl")

        if self.crawl_callback:
            try:
                # コールバックを実行
                result = self.crawl_callback()

                self.stats['last_crawl_at'] = now
                self.stats['total_crawls'] += 1

                if isinstance(result, dict):
                    new_count = result.get('new_count', 0)
                    self.stats['total_new_jobs'] += new_count

                    if new_count > 0:
                        self._notify(
                            "新着求人",
                            f"{new_count}件の新着求人が見つかりました"
                        )

            except Exception as e:
                self.stats['errors'] += 1
                logger.error(f"Scheduled crawl error: {e}", exc_info=True)
                self._notify("エラー", f"クローリングエラー: {str(e)}")

    def _is_within_hours(self, dt: datetime) -> bool:
        """実行時間帯内かチェック"""
        start = self.settings['start_hour']
        end = self.settings['end_hour']
        hour = dt.hour

        if start <= end:
            return start <= hour < end
        else:
            # 例: 22時〜6時のような場合
            return hour >= start or hour < end

    def _notify(self, title: str, message: str):
        """通知を送信"""
        if self.notification_callback:
            try:
                self.notification_callback(title, message)
            except Exception as e:
                logger.error(f"Notification error: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """統計情報を取得"""
        return {
            **self.stats,
            'is_running': self.is_running,
            'settings': self.settings.copy(),
            'next_run': self._get_next_run_time(),
        }

    def _get_next_run_time(self) -> Optional[datetime]:
        """次回実行時刻を取得"""
        if not self.is_running:
            return None

        job = self.scheduler.get_job('scheduled_crawl')
        if job and job.next_run_time:
            return job.next_run_time
        return None

    def run_now(self):
        """今すぐクロールを実行"""
        if self.crawl_callback:
            # 別スレッドで実行
            thread = threading.Thread(target=self._scheduled_crawl)
            thread.start()


class NewJobMonitor:
    """新着求人モニター"""

    def __init__(self, job_repository):
        self.job_repository = job_repository
        self.last_check_at = datetime.now()

    def get_new_jobs_summary(self, hours: int = 24) -> Dict[str, Any]:
        """新着求人のサマリーを取得"""
        since = datetime.now() - timedelta(hours=hours)
        new_jobs = self.job_repository.get_new_jobs_since(since)

        # 媒体別集計
        by_source = {}
        for job in new_jobs:
            source = job.get('source_display_name', 'Unknown')
            by_source[source] = by_source.get(source, 0) + 1

        # 職種別集計
        by_job_type = {}
        for job in new_jobs:
            job_title = job.get('job_title', 'その他')
            # 職種名の正規化（最初の10文字）
            job_type = job_title[:10] if len(job_title) > 10 else job_title
            by_job_type[job_type] = by_job_type.get(job_type, 0) + 1

        return {
            'total_count': len(new_jobs),
            'by_source': by_source,
            'by_job_type': dict(sorted(by_job_type.items(), key=lambda x: x[1], reverse=True)[:10]),
            'since': since,
            'checked_at': datetime.now(),
        }

    def get_daily_stats(self, days: int = 7) -> List[Dict[str, Any]]:
        """日別統計を取得"""
        stats = []
        now = datetime.now()

        for i in range(days):
            date = now - timedelta(days=i)
            start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_of_day = start_of_day + timedelta(days=1)

            jobs = self.job_repository.get_new_jobs_since(start_of_day)
            day_jobs = [j for j in jobs if j.get('crawled_at', datetime.min) < end_of_day]

            stats.append({
                'date': start_of_day.strftime('%Y-%m-%d'),
                'day_name': ['月', '火', '水', '木', '金', '土', '日'][start_of_day.weekday()],
                'count': len(day_jobs),
            })

        return list(reversed(stats))
