"""
クローリングサービス
スクレイパーとデータベース・フィルタを統合
"""
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable
from collections import Counter
import logging
import sys
import os

# パス追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from scrapers.townwork import TownworkScraper
from scrapers.indeed import IndeedScraper
from scrapers.baitoru import BaitoruScraper
from src.database.db_manager import DatabaseManager
from src.database.job_repository import JobRepository
from src.filters.job_filter import JobFilter, FilterResult
from src.services.csv_exporter import CSVExporter

logger = logging.getLogger(__name__)

# デバッグログフラグ（True: 詳細ログ出力、False: 出力しない）
DEBUG_JOB_LOG = True


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

        # スクレイパー
        self.scrapers = {
            "townwork": TownworkScraper,
            "indeed": IndeedScraper,
            "baitoru": BaitoruScraper,
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

    def _output_debug_job_log(self, jobs: List[Dict[str, Any]]):
        """デバッグ用: 取得した全件のjob_idとURLを出力"""
        import sys

        # ログ出力用のヘルパー関数
        def log(msg: str):
            logger.info(msg)
            print(msg, file=sys.stderr, flush=True)

        log("\n" + "=" * 80)
        log(f"[DEBUG] 取得求人一覧 (全{len(jobs)}件)")
        log("=" * 80)

        job_ids = []
        urls = []

        for i, job in enumerate(jobs, 1):
            job_id = job.get('job_id') or job.get('job_number') or "N/A"
            url = job.get('page_url') or job.get('url') or "N/A"
            title = job.get('job_title') or job.get('title') or "N/A"
            company = job.get('company_name') or job.get('company') or "N/A"

            job_ids.append(job_id)
            urls.append(url)

            log(f"{i:3d}. job_id: {job_id}")
            log(f"     URL: {url}")
            log(f"     会社: {company[:30]}... | 職種: {title[:30]}...")
            log("-" * 40)

        # 重複チェック
        log("\n" + "=" * 80)
        log("[DEBUG] 重複分析")
        log("=" * 80)

        # job_id重複
        job_id_counts = Counter(job_ids)
        duplicated_job_ids = {k: v for k, v in job_id_counts.items() if v > 1 and k != "N/A"}
        if duplicated_job_ids:
            log(f"\n重複job_id ({len(duplicated_job_ids)}種類):")
            for jid, count in sorted(duplicated_job_ids.items(), key=lambda x: -x[1]):
                log(f"  {jid}: {count}回")
        else:
            log("\njob_idの重複: なし")

        # URL重複
        url_counts = Counter(urls)
        duplicated_urls = {k: v for k, v in url_counts.items() if v > 1 and k != "N/A"}
        if duplicated_urls:
            log(f"\n重複URL ({len(duplicated_urls)}種類):")
            for url, count in sorted(duplicated_urls.items(), key=lambda x: -x[1]):
                log(f"  {url[:60]}...: {count}回")
        else:
            log("\nURLの重複: なし")

        # ユニーク数
        unique_job_ids = len(set(jid for jid in job_ids if jid != "N/A"))
        unique_urls = len(set(u for u in urls if u != "N/A"))
        log(f"\n総件数: {len(jobs)}")
        log(f"ユニークjob_id数: {unique_job_ids}")
        log(f"ユニークURL数: {unique_urls}")
        log("=" * 80 + "\n")

    async def crawl_townwork(
        self,
        keywords: List[str],
        areas: List[str],
        max_pages: int = 5,
        parallel: int = 5,
        filters: Optional[Dict[str, Any]] = None,
        fetch_details: bool = True
    ) -> Dict[str, Any]:
        """
        タウンワークをクロール

        Args:
            keywords: 検索キーワードリスト
            areas: 地域リスト
            max_pages: 最大ページ数
            parallel: 並列数
            filters: 検索フィルタ
            fetch_details: 詳細ページから追加情報を取得するか

        Returns:
            クロール結果
        """
        from playwright.async_api import async_playwright
        from utils.stealth import StealthConfig, create_stealth_context
        import random

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
            self._report_progress(f"一覧取得完了: {len(jobs)}件", 1, 3)

            # 詳細ページから追加情報を取得
            if fetch_details and jobs:
                # job_idで重複を除去し、ユニークな求人のみ詳細取得
                seen_job_ids = set()
                unique_jobs = []
                duplicate_count = 0

                for job in jobs:
                    job_id = job.get('job_number') or job.get('job_id')
                    job_url = job.get('page_url') or job.get('url')

                    # job_idがある場合は重複チェック
                    if job_id:
                        if job_id in seen_job_ids:
                            duplicate_count += 1
                            continue
                        seen_job_ids.add(job_id)
                    # job_idがない場合はURLで重複チェック
                    elif job_url:
                        normalized_url = self._normalize_url(job_url)
                        if normalized_url in seen_job_ids:
                            duplicate_count += 1
                            continue
                        seen_job_ids.add(normalized_url)

                    unique_jobs.append(job)

                if duplicate_count > 0:
                    logger.info(f"重複求人をスキップ: {duplicate_count}件（ユニーク: {len(unique_jobs)}件）")

                self._report_progress(f"詳細情報取得中（{len(unique_jobs)}件）...", 2, 3)

                # 並列処理で詳細ページを取得
                async with async_playwright() as p:
                    launch_args = StealthConfig.get_launch_args()
                    launch_args["headless"] = True
                    browser = await p.chromium.launch(**launch_args)

                    try:
                        # 並列数の設定（サーバー負荷を考慮して3に制限）
                        max_concurrent = 3
                        semaphore = asyncio.Semaphore(max_concurrent)

                        async def fetch_detail_with_semaphore(job, idx):
                            async with semaphore:
                                job_url = job.get('page_url') or job.get('url')
                                if not job_url:
                                    return

                                # 各タスクごとに新しいコンテキストとページを作成
                                context = await create_stealth_context(browser)
                                try:
                                    page = await context.new_page()
                                    await StealthConfig.apply_stealth_scripts(page)

                                    self._report_progress(
                                        f"詳細取得中 ({idx+1}/{len(unique_jobs)})",
                                        idx + 1,
                                        len(unique_jobs)
                                    )

                                    detail_data = await scraper.extract_detail_info(page, job_url)
                                    job.update(detail_data)

                                    # サーバー負荷軽減のため待機
                                    await page.wait_for_timeout(random.randint(300, 800))

                                except Exception as e:
                                    logger.warning(f"Failed to fetch detail for {job_url}: {e}")
                                finally:
                                    await context.close()

                        # 全ての詳細取得タスクを並列実行
                        tasks = [
                            fetch_detail_with_semaphore(job, idx)
                            for idx, job in enumerate(unique_jobs)
                        ]
                        await asyncio.gather(*tasks)

                        # 重複していた求人にも詳細データをコピー
                        job_id_to_detail = {}
                        for job in unique_jobs:
                            job_id = job.get('job_number') or job.get('job_id')
                            if job_id:
                                job_id_to_detail[job_id] = {
                                    k: v for k, v in job.items()
                                    if k in ['address', 'phone', 'business_content',
                                             'job_description', 'published_date', 'postal_code',
                                             'working_hours', 'holidays', 'qualifications']
                                }

                        # 重複求人に詳細データを適用
                        for job in jobs:
                            job_id = job.get('job_number') or job.get('job_id')
                            if job_id and job_id in job_id_to_detail:
                                for key, value in job_id_to_detail[job_id].items():
                                    if key not in job or not job[key]:
                                        job[key] = value

                    finally:
                        await browser.close()

            self._report_progress(f"取得完了: {len(jobs)}件", 3, 3)

            # デバッグログ出力
            if DEBUG_JOB_LOG:
                self._output_debug_job_log(jobs)

            # データベースに保存
            saved_count = 0
            new_count = 0
            new_urls = []
            for job in jobs:
                try:
                    # URL差分（クエリ等）で重複を取り逃さないよう正規化
                    if job.get('page_url'):
                        job['page_url'] = self._normalize_url(job['page_url'])
                    if job.get('url'):
                        job['url'] = self._normalize_url(job['url'])

                    job['crawled_at'] = datetime.now()
                    # 既存チェック
                    existing = self._check_existing(job)
                    self.job_repository.save_job(job, "townwork")

                    saved_count += 1
                    if not existing:
                        new_count += 1
                        new_urls.append(job.get("page_url") or job.get("url") or "N/A")
                except Exception as e:
                    logger.warning(f"Failed to save job: {e}")

            result['saved_count'] = saved_count
            result['new_count'] = new_count
            # 今回取得した全データをそのまま返す（DB保存の成否に関係なく）
            result['jobs'] = [self._prepare_job_record(job) for job in jobs]

            self._report_progress(f"保存完了: {saved_count}件（新着: {new_count}件）", 2, 2)

            # 新規扱いとなったURLをログ出力
            if new_urls:
                logger.info("=== 新規扱いURL一覧 ===")
                for url in new_urls:
                    logger.info(f"NEW: {url}")
                logger.info(f"=== 新規URL合計: {len(new_urls)}件 ===")

            # クロールログを記録
            self._save_crawl_log(result)

        except Exception as e:
            result['error'] = str(e)
            logger.error(f"Crawl error: {e}", exc_info=True)

        result['finished_at'] = datetime.now()
        return result

    async def crawl_indeed(
        self,
        keywords: List[str],
        areas: List[str],
        max_pages: int = 1,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Indeedをクロール

        Args:
            keywords: 検索キーワードリスト
            areas: 地域リスト
            max_pages: 最大ページ数（403対策のため1推奨）
            filters: 検索フィルタ

        Returns:
            クロール結果
        """
        from playwright.async_api import async_playwright
        from utils.stealth import StealthConfig, create_stealth_context

        result = {
            'source': 'indeed',
            'keywords': keywords,
            'areas': areas,
            'started_at': datetime.now(),
            'finished_at': None,
            'total_count': 0,
            'scraped_count': 0,
            'saved_count': 0,
            'new_count': 0,
            'jobs': [],
            'error': None,
        }

        try:
            self._report_progress("Indeed クローリング開始", 0, 1)

            # スクレイパー初期化
            scraper = IndeedScraper()

            all_jobs = []

            async with async_playwright() as p:
                # Stealth設定を取得
                launch_args = StealthConfig.get_launch_args()
                launch_args["headless"] = False  # ブラウザ表示（ボット検出対策）

                browser = await p.chromium.launch(**launch_args)

                try:
                    context = await create_stealth_context(browser)
                    page = await context.new_page()
                    await StealthConfig.apply_stealth_scripts(page)

                    # キーワード×地域の組み合わせでスクレイピング
                    total_combinations = len(keywords) * len(areas)
                    current_idx = 0

                    for keyword in keywords:
                        for area in areas:
                            current_idx += 1
                            self._report_progress(
                                f"[Indeed] {area} × {keyword} を検索中...",
                                current_idx,
                                total_combinations
                            )

                            # 検索実行
                            url = scraper.generate_search_url(keyword, area, 1)
                            logger.info(f"Navigating to: {url}")

                            await page.goto(url, wait_until="domcontentloaded", timeout=60000)

                            # JS描画待機（長めに）
                            await page.wait_for_timeout(5000)

                            # カードが読み込まれるまで待機
                            try:
                                await page.wait_for_selector(".job_seen_beacon", timeout=10000)
                            except Exception:
                                logger.warning(f"Job cards not found for {keyword} in {area}, trying alternative wait...")
                                await page.wait_for_timeout(3000)

                            # カードを取得
                            cards = await page.query_selector_all(".job_seen_beacon")
                            logger.info(f"Found {len(cards)} job cards for {keyword} in {area}")

                            for card in cards:
                                try:
                                    job_data = await scraper._extract_card_data(card)
                                    if job_data:
                                        job_data['keyword'] = keyword
                                        job_data['area'] = area
                                        all_jobs.append(job_data)
                                except Exception as e:
                                    logger.error(f"Error extracting job card: {e}")
                                    continue

                            # 403対策：組み合わせ間の待機
                            import random
                            await page.wait_for_timeout(random.randint(3000, 5000))

                    await context.close()

                except Exception as e:
                    logger.error(f"Indeed scraping error: {e}")
                    result['error'] = str(e)

                finally:
                    await browser.close()

            result['total_count'] = len(all_jobs)
            result['scraped_count'] = len(all_jobs)
            self._report_progress(f"取得完了: {len(all_jobs)}件", 1, 2)

            # デバッグログ出力
            if DEBUG_JOB_LOG and all_jobs:
                self._output_debug_job_log(all_jobs)

            # データベースに保存
            saved_count = 0
            new_count = 0
            for job in all_jobs:
                try:
                    if job.get('page_url'):
                        job['page_url'] = self._normalize_url(job['page_url'])

                    job['crawled_at'] = datetime.now()
                    existing = self._check_existing_indeed(job)
                    self.job_repository.save_job(job, "indeed")

                    saved_count += 1
                    if not existing:
                        new_count += 1
                except Exception as e:
                    logger.warning(f"Failed to save job: {e}")

            result['saved_count'] = saved_count
            result['new_count'] = new_count
            result['jobs'] = [self._prepare_job_record(job) for job in all_jobs]

            self._report_progress(f"保存完了: {saved_count}件（新着: {new_count}件）", 2, 2)
            self._save_crawl_log_indeed(result)

        except Exception as e:
            result['error'] = str(e)
            logger.error(f"Indeed crawl error: {e}", exc_info=True)

        result['finished_at'] = datetime.now()
        return result

    def _check_existing_indeed(self, job: Dict[str, Any]) -> bool:
        """Indeed求人の既存チェック"""
        source_id = self.db_manager.get_source_id("indeed")
        if not source_id:
            return False

        job_identifier = job.get('job_number')
        page_url = self._normalize_url(job.get('page_url'))

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

    def _save_crawl_log_indeed(self, result: Dict[str, Any]):
        """Indeedのクロールログを保存"""
        source_id = self.db_manager.get_source_id("indeed")
        if not source_id:
            # Indeedソースがない場合は作成
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO sources (name, url) VALUES (?, ?)",
                    ("indeed", "https://jp.indeed.com")
                )
                conn.commit()
            source_id = self.db_manager.get_source_id("indeed")

        if source_id:
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

    async def crawl_baitoru(
        self,
        keywords: List[str],
        areas: List[str],
        max_pages: int = 3,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        バイトルをクロール

        Args:
            keywords: 検索キーワードリスト
            areas: 地域リスト（現在は無視、全国検索）
            max_pages: 最大ページ数
            filters: 検索フィルタ

        Returns:
            クロール結果
        """
        result = {
            'source': 'baitoru',
            'keywords': keywords,
            'areas': areas,
            'started_at': datetime.now(),
            'finished_at': None,
            'total_count': 0,
            'scraped_count': 0,
            'saved_count': 0,
            'new_count': 0,
            'jobs': [],
            'error': None,
        }

        try:
            from playwright.async_api import async_playwright
            from utils.stealth import StealthConfig, create_stealth_context

            self._report_progress("バイトル クローリング開始", 0, 1)

            # スクレイパー初期化
            scraper = BaitoruScraper()

            all_jobs = []
            seen_job_ids = set()  # 重複防止用のjob_idセット

            async with async_playwright() as p:
                # Stealth設定を取得
                launch_args = StealthConfig.get_launch_args()
                launch_args["headless"] = False  # ブラウザ表示（ボット検出対策）

                browser = await p.chromium.launch(**launch_args)

                try:
                    context = await create_stealth_context(browser)
                    page = await context.new_page()
                    await StealthConfig.apply_stealth_scripts(page)

                    # キーワード×地域の組み合わせでスクレイピング
                    total_combinations = len(keywords) * len(areas)
                    current_idx = 0

                    for keyword in keywords:
                        for area in areas:
                            current_idx += 1
                            self._report_progress(
                                f"[バイトル] {area} × {keyword} を検索中...",
                                current_idx,
                                total_combinations
                            )

                            # スクレイピング実行
                            jobs = await scraper.search_jobs(
                                page=page,
                                keyword=keyword,
                                area=area,
                                max_pages=max_pages
                            )

                            logger.info(f"Found {len(jobs)} jobs for keyword: {keyword} in {area}")

                            # 派遣フィルタが有効かチェック
                            enable_dispatch_filter = filters.get('enable_dispatch_keyword', True) if filters else True
                            dispatch_keywords = ['派遣', '派遣社員', '無期雇用派遣', '登録型派遣']

                            # 各求人の詳細情報を取得
                            skipped_dispatch_count = 0
                            skipped_duplicate_count = 0
                            for idx, job in enumerate(jobs):
                                job['keyword'] = keyword
                                job['area'] = area

                                # job_idで重複チェック
                                job_id = job.get('job_id')
                                if job_id and job_id in seen_job_ids:
                                    skipped_duplicate_count += 1
                                    logger.debug(f"Skipped duplicate job: {job_id}")
                                    continue

                                # 派遣フィルタが有効な場合、雇用形態に派遣を含む案件はスキップ
                                # ※カード段階ではemployment_typeのみチェック（title等は詳細取得後にフィルタ）
                                if enable_dispatch_filter:
                                    employment_type = job.get('employment_type', '') or ''

                                    should_skip = False
                                    if employment_type:
                                        for dispatch_kw in dispatch_keywords:
                                            if dispatch_kw in employment_type:
                                                should_skip = True
                                                break

                                    if should_skip:
                                        skipped_dispatch_count += 1
                                        logger.debug(f"Skipped dispatch job: {job.get('title', 'N/A')} ({employment_type})")
                                        continue

                                if job.get('page_url'):
                                    try:
                                        self._report_progress(
                                            f"[バイトル] 詳細取得中 ({idx+1}/{len(jobs)})",
                                            current_idx,
                                            total_combinations
                                        )
                                        detail_data = await scraper.extract_detail_info(page, job['page_url'])
                                        job.update(detail_data)
                                        # サーバー負荷軽減のため待機
                                        import random
                                        await page.wait_for_timeout(random.randint(800, 1500))
                                    except Exception as e:
                                        logger.warning(f"Failed to fetch detail for {job['page_url']}: {e}")

                                # 重複防止用にjob_idを記録
                                if job_id:
                                    seen_job_ids.add(job_id)

                                all_jobs.append(job)

                            if skipped_dispatch_count > 0:
                                logger.info(f"Skipped {skipped_dispatch_count} dispatch jobs before detail fetch")
                            if skipped_duplicate_count > 0:
                                logger.info(f"Skipped {skipped_duplicate_count} duplicate jobs")

                            # 待機（ボット検出対策）
                            import random
                            await page.wait_for_timeout(random.randint(2000, 4000))

                    await context.close()

                except Exception as e:
                    logger.error(f"Baitoru scraping error: {e}")
                    result['error'] = str(e)

                finally:
                    await browser.close()

            result['total_count'] = len(all_jobs)
            result['scraped_count'] = len(all_jobs)
            self._report_progress(f"取得完了: {len(all_jobs)}件", 1, 2)

            # デバッグログ出力
            if DEBUG_JOB_LOG and all_jobs:
                self._output_debug_job_log(all_jobs)

            # データベースに保存
            saved_count = 0
            new_count = 0
            for job in all_jobs:
                try:
                    if job.get('page_url'):
                        job['page_url'] = self._normalize_url(job['page_url'])

                    job['crawled_at'] = datetime.now()
                    existing = self._check_existing_baitoru(job)
                    self.job_repository.save_job(job, "baitoru")

                    saved_count += 1
                    if not existing:
                        new_count += 1
                except Exception as e:
                    logger.warning(f"Failed to save job: {e}")

            result['saved_count'] = saved_count
            result['new_count'] = new_count
            result['jobs'] = [self._prepare_baitoru_job_record(job) for job in all_jobs]

            self._report_progress(f"保存完了: {saved_count}件（新着: {new_count}件）", 2, 2)
            self._save_crawl_log_baitoru(result)

        except Exception as e:
            result['error'] = str(e)
            logger.error(f"Baitoru crawl error: {e}", exc_info=True)

        result['finished_at'] = datetime.now()
        return result

    def _check_existing_baitoru(self, job: Dict[str, Any]) -> bool:
        """バイトル求人の既存チェック"""
        source_id = self.db_manager.get_source_id("baitoru")
        if not source_id:
            return False

        job_identifier = job.get('job_number')
        page_url = self._normalize_url(job.get('page_url'))

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

    def _prepare_baitoru_job_record(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """バイトル用のテーブル表示データ整形"""
        return {
            "company_name": job.get("company_name", ""),
            "job_title": job.get("title", ""),
            "work_location": job.get("location", ""),
            "salary": job.get("salary", ""),
            "employment_type": job.get("employment_type", ""),
            "page_url": job.get("page_url", ""),
            "crawled_at": job.get("crawled_at"),
            "job_type": job.get("job_type", ""),
            "working_hours": job.get("working_hours", ""),
            "tags": job.get("tags", ""),
        }

    def _save_crawl_log_baitoru(self, result: Dict[str, Any]):
        """バイトルのクロールログを保存"""
        source_id = self.db_manager.get_source_id("baitoru")
        if not source_id:
            # バイトルソースがない場合は作成
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO sources (name, url) VALUES (?, ?)",
                    ("baitoru", "https://www.baitoru.com")
                )
                conn.commit()
            source_id = self.db_manager.get_source_id("baitoru")

        if source_id:
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

    def _check_existing(self, job: Dict[str, Any]) -> bool:
        """既存の求人かチェック"""
        source_id = self.db_manager.get_source_id("townwork")
        if not source_id:
            return False

        job_identifier = job.get('job_id') or job.get('job_number')
        page_url = self._normalize_url(job.get('page_url') or job.get('url'))

        if not job_identifier and not page_url:
            # IDもURLも無い場合は内容ハッシュで近似判定
            job_identifier = self.job_repository._generate_fallback_id(job)
        else:
            # 既存判定時もURL・テキストを正規化した値を使う
            normalized_job = {
                "company": job.get("company") or job.get("company_name") or "",
                "title": job.get("title") or job.get("job_title") or "",
                "location": job.get("location") or job.get("work_location") or "",
            }
            norm_fallback = self.job_repository._generate_fallback_id(normalized_job)
            job_identifier = job_identifier or norm_fallback

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

    def _normalize_url(self, url: Optional[str]) -> str:
        """クエリやフラグメントを除去し、末尾スラッシュを揃えたURLに正規化"""
        if not url:
            return ""
        from urllib.parse import urlparse, urlunparse

        parsed = urlparse(url)
        path = parsed.path or "/"
        path = path.rstrip("/") or "/"
        return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))

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
            # 詳細ページから取得する追加フィールド
            "address": job.get("address", ""),
            "phone": job.get("phone", ""),
            "business_content": job.get("business_content", ""),
            "job_description": job.get("job_description", ""),
            "published_date": job.get("published_date", ""),
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
