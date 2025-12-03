"""
ベーススクレイパークラス
全てのサイト固有スクレイパーの基底クラス
"""
import asyncio
import json
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from pathlib import Path
from playwright.async_api import async_playwright, Page, Browser, TimeoutError as PlaywrightTimeoutError
import logging
import sys
import os

# utilsモジュールのパスを追加
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from utils.retry import async_retry, RetryConfig, ErrorCounter
from utils.user_agents import ua_rotator
from utils.proxy import proxy_rotator
from utils.performance import PerformanceMonitor
from utils.stealth import StealthConfig, create_stealth_context
from utils.page_utils import PageUtils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """スクレイピング基底クラス"""

    def __init__(self, site_name: str, config_path: str = "config/selectors.json"):
        self.site_name = site_name
        self.config = self._load_config(config_path)
        self.site_config = self.config.get(site_name, {})
        self.selectors = self.site_config.get("selectors", {})
        self.detail_selectors = self.site_config.get("detail_selectors", {})
        self.results: List[Dict[str, Any]] = []
        self.error_counter = ErrorCounter()
        self.performance_monitor = PerformanceMonitor(name=site_name)
        self.current_filters: Dict[str, Any] = {}
        # リアルタイム件数コールバック
        self._realtime_callback = None

        # リトライ設定
        self.retry_config = RetryConfig(
            max_attempts=3,
            initial_delay=2.0,
            max_delay=30.0,
            exponential_base=2.0,
            exceptions=(PlaywrightTimeoutError, ConnectionError, Exception)
        )

    def set_realtime_callback(self, callback):
        """リアルタイム件数コールバックを設定"""
        self._realtime_callback = callback

    def _report_count(self, count: int):
        """件数を報告"""
        if self._realtime_callback:
            self._realtime_callback(count)

    def _load_config(self, config_path: str) -> Dict:
        """設定ファイルを読み込み"""
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_file, 'r', encoding='utf-8') as f:
            return json.load(f)

    def generate_search_url(self, keyword: str, area: str, page: int = 1) -> str:
        """検索URLを生成"""
        url_pattern = self.site_config.get("search_url_pattern", "")
        pagination = self.site_config.get("pagination", {})

        if pagination.get("type") == "offset":
            # Indeedのようなoffset方式
            offset = (page - 1) * pagination.get("increment", 10)
            return url_pattern.format(keyword=keyword, area=area, offset=offset)
        else:
            # 通常のページ番号方式
            return url_pattern.format(keyword=keyword, area=area, page=page)

    async def extract_job_card(self, card_element, page: Page) -> Dict[str, Any]:
        """求人カードから情報を抽出（サイトごとにオーバーライド可能）"""
        job_data = {
            "site": self.site_config.get("name", self.site_name),
            "title": "",
            "company": "",
            "location": "",
            "salary": "",
            "employment_type": "",
            "url": "",
        }

        try:
            # タイトル
            if self.selectors.get("title"):
                title_elem = await card_element.query_selector(self.selectors["title"])
                if title_elem:
                    job_data["title"] = (await title_elem.inner_text()).strip()

            # 会社名
            if self.selectors.get("company"):
                company_elem = await card_element.query_selector(self.selectors["company"])
                if company_elem:
                    job_data["company"] = (await company_elem.inner_text()).strip()

            # 勤務地
            if self.selectors.get("location"):
                location_elem = await card_element.query_selector(self.selectors["location"])
                if location_elem:
                    job_data["location"] = (await location_elem.inner_text()).strip()

            # 給与
            if self.selectors.get("salary"):
                salary_elem = await card_element.query_selector(self.selectors["salary"])
                if salary_elem:
                    job_data["salary"] = (await salary_elem.inner_text()).strip()

            # 雇用形態
            if self.selectors.get("employment_type"):
                emp_elem = await card_element.query_selector(self.selectors["employment_type"])
                if emp_elem:
                    job_data["employment_type"] = (await emp_elem.inner_text()).strip()

            # 詳細ページURL
            if self.selectors.get("detail_link"):
                link_elem = await card_element.query_selector(self.selectors["detail_link"])
                if link_elem:
                    href = await link_elem.get_attribute("href")
                    if href:
                        # 相対URLを絶対URLに変換
                        if href.startswith("/"):
                            job_data["url"] = self.site_config.get("base_url", "") + href
                        else:
                            job_data["url"] = href

        except Exception as e:
            logger.error(f"Error extracting job card: {e}")

        return job_data

    async def scrape_page(self, page: Page, url: str) -> List[Dict[str, Any]]:
        """1ページ分のデータを取得（実践的な実装）"""
        self.error_counter.record_attempt()
        jobs = []

        try:
            logger.info(f"Scraping: {url}")

            # 安全なページ遷移
            success = await PageUtils.safe_goto(page, url, timeout=30000)
            if not success:
                logger.error(f"Failed to load page: {url}")
                self.error_counter.record_failure(Exception("Page load failed"))
                return jobs

            # ブロックチェック
            block_info = await PageUtils.check_for_block(page)
            if block_info["is_blocked"]:
                logger.error(f"Access blocked: {block_info['indicators']}")
                self.error_counter.record_failure(Exception("Access blocked"))

                # デバッグ用スクリーンショット
                screenshot_path = f"data/screenshots/blocked_{self.site_name}_{asyncio.get_event_loop().time()}.png"
                await PageUtils.take_screenshot(page, screenshot_path)
                return jobs

            # 求人カードセレクタ取得
            job_cards_selector = self.selectors.get("job_cards")
            if not job_cards_selector:
                logger.warning(f"No job_cards selector defined for {self.site_name}")
                self.error_counter.record_failure(ValueError("No job_cards selector"))
                return jobs

            # セレクタが存在するか確認（JSレンダリング待機のため長めに設定）
            # 最初の試行
            selector_found = await PageUtils.verify_selector(page, job_cards_selector, timeout=15000)

            # 見つからない場合、追加待機してリトライ
            if not selector_found:
                logger.info("First selector check failed, waiting and retrying...")
                await asyncio.sleep(3)
                selector_found = await PageUtils.verify_selector(page, job_cards_selector, timeout=10000)

            if not selector_found:
                logger.warning(f"Job cards selector not found: {job_cards_selector}")
                # デバッグ用スクリーンショット
                screenshot_path = f"data/screenshots/no_selector_{self.site_name}_{asyncio.get_event_loop().time()}.png"
                await PageUtils.take_screenshot(page, screenshot_path)
                self.error_counter.record_failure(ValueError("Selector not found"))
                return jobs

            # 求人カードを全て取得
            job_cards = await page.query_selector_all(job_cards_selector)
            logger.info(f"Found {len(job_cards)} job cards")

            if len(job_cards) == 0:
                logger.warning("No job cards found despite selector match")
                return jobs

            # 各求人カードから情報を抽出
            for idx, card in enumerate(job_cards):
                try:
                    job_data = await self.extract_job_card(card, page)
                    if job_data.get("title"):  # タイトルがあるもののみ追加
                        jobs.append(job_data)
                    else:
                        logger.debug(f"Card {idx} has no title, skipping")
                except Exception as e:
                    logger.warning(f"Error extracting job card {idx}: {e}")
                    continue

            self.error_counter.record_success()
            logger.info(f"Successfully extracted {len(jobs)} jobs from {url}")

        except Exception as e:
            logger.error(f"Error scraping page {url}: {e}", exc_info=True)
            self.error_counter.record_failure(e)

        return jobs

    async def scrape_with_browser(
        self,
        browser: Browser,
        keyword: str,
        area: str,
        max_pages: int = 5
    ) -> List[Dict[str, Any]]:
        """ブラウザを使って複数ページをスクレイピング（Stealth対応）- 順次実行版"""
        all_jobs = []

        # User-Agentをローテーション
        user_agent = ua_rotator.get_random()
        logger.info(f"Using User-Agent: {user_agent[:50]}...")

        # プロキシ設定
        proxy_config = None
        if proxy_rotator.is_enabled():
            proxy = proxy_rotator.get_random()
            if proxy:
                proxy_config = proxy.to_playwright_format()
                logger.info(f"Using proxy: {proxy}")

        # Stealthコンテキスト作成
        context = await create_stealth_context(
            browser,
            user_agent=user_agent,
            proxy=proxy_config
        )

        try:
            page = await context.new_page()

            # Stealthスクリプト適用
            await StealthConfig.apply_stealth_scripts(page)

            # リソースブロッキングを適用（画像・動画・フォント等を無効化）
            if hasattr(context, '_block_resources') and context._block_resources:
                await context._setup_route_blocking(page)

            for page_num in range(1, max_pages + 1):
                url = self.generate_search_url(keyword, area, page_num)
                jobs = await self.scrape_page(page, url)
                all_jobs.extend(jobs)

                # パフォーマンス測定
                self.performance_monitor.record_item(len(jobs))

                if not jobs:  # 求人が見つからなければ終了
                    logger.info(f"No more jobs found at page {page_num}")
                    break

                # 次のページへ行く前に待機（短縮版）
                wait_time = 0.5 + (asyncio.get_event_loop().time() % 0.5)  # 0.5-1.0秒
                await asyncio.sleep(wait_time)

        except Exception as e:
            logger.error(f"Error in scrape_with_browser: {e}", exc_info=True)

        finally:
            await context.close()

        return all_jobs

    async def scrape_single_page(
        self,
        browser: Browser,
        keyword: str,
        area: str,
        page_num: int,
        task_idx: int = 0
    ) -> List[Dict[str, Any]]:
        """1ページを並列用にスクレイピング"""
        import random

        # タスク開始前にスタッガード遅延（同時アクセスを避ける）
        stagger_delay = task_idx * 1.5 + random.uniform(0.5, 1.5)
        logger.info(f"[タスク{task_idx+1}] {stagger_delay:.1f}秒後に開始...")
        await asyncio.sleep(stagger_delay)

        # User-Agentをローテーション
        user_agent = ua_rotator.get_random()

        # プロキシ設定
        proxy_config = None
        if proxy_rotator.is_enabled():
            proxy = proxy_rotator.get_random()
            if proxy:
                proxy_config = proxy.to_playwright_format()

        # Stealthコンテキスト作成
        context = await create_stealth_context(
            browser,
            user_agent=user_agent,
            proxy=proxy_config
        )

        jobs = []
        try:
            page = await context.new_page()
            await StealthConfig.apply_stealth_scripts(page)

            if hasattr(context, '_block_resources') and context._block_resources:
                await context._setup_route_blocking(page)

            url = self.generate_search_url(keyword, area, page_num)
            jobs = await self.scrape_page(page, url)

            self.performance_monitor.record_item(len(jobs))

        except Exception as e:
            logger.error(f"Error scraping page {page_num}: {e}", exc_info=True)

        finally:
            await context.close()

        return jobs

    async def scrape(
        self,
        keywords: List[str],
        areas: List[str],
        max_pages: int = 5,
        parallel: int = 2,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        非同期並列スクレイピング（ページ単位で並列化）

        Args:
            keywords: 検索キーワードリスト
            areas: 地域リスト
            max_pages: 各条件での最大ページ数
            parallel: 並列数（同時に開くブラウザコンテキスト数、推奨: 2-3）
        """
        # パフォーマンス測定開始
        self.performance_monitor.start()

        all_results = []

        # 現在のフィルタを設定
        self.current_filters = filters or {}

        # 並列数が高すぎる場合は警告して制限
        if parallel > 2:
            logger.warning(f"並列数 {parallel} は高すぎるため 2 に制限します（タイムアウト・403エラー回避）")
            parallel = 2

        async with async_playwright() as p:
            # Stealth設定を適用してブラウザ起動（表示モードに変更）
            launch_args = StealthConfig.get_launch_args()
            launch_args["headless"] = False  # ボット検出回避のため表示モード
            browser = await p.chromium.launch(**launch_args)
            logger.info(f"[{self.site_name}] ブラウザ起動完了")

            try:
                # 初回アクセス前にトップページを訪問してCookieを取得
                base_url = self.site_config.get("base_url", "")
                if base_url:
                    logger.info(f"[{self.site_name}] トップページにアクセスしてセッション確立中...")
                    try:
                        context = await create_stealth_context(browser)
                        init_page = await context.new_page()
                        await StealthConfig.apply_stealth_scripts(init_page)
                        await init_page.goto(base_url, wait_until="domcontentloaded", timeout=30000)
                        await init_page.wait_for_timeout(2000)
                        await context.close()
                        logger.info(f"[{self.site_name}] セッション確立完了")
                    except Exception as e:
                        logger.warning(f"[{self.site_name}] トップページアクセス失敗（続行）: {e}")

                # 全てのページタスクを生成（キーワード×地域×ページ）
                page_tasks = []
                for keyword in keywords:
                    for area in areas:
                        for page_num in range(1, max_pages + 1):
                            page_tasks.append((keyword, area, page_num))

                logger.info(f"[{self.site_name}] 合計 {len(page_tasks)} ページを parallel={parallel} で処理")

                # セマフォで並列数を制限
                semaphore = asyncio.Semaphore(parallel)
                active_count = [0]  # 現在アクティブなタスク数（リストで参照渡し）

                async def limited_task(keyword, area, page_num, idx):
                    async with semaphore:
                        active_count[0] += 1
                        logger.info(f"[並列確認] タスク{idx+1}開始 (p{page_num}) - 現在同時実行数: {active_count[0]}/{parallel}")
                        try:
                            result = await self.scrape_single_page(browser, keyword, area, page_num, idx)
                            return result
                        finally:
                            logger.info(f"[並列確認] タスク{idx+1}終了 (p{page_num}) - 同時実行数: {active_count[0]}/{parallel}")
                            active_count[0] -= 1

                # 並列実行
                results = await asyncio.gather(
                    *[limited_task(kw, ar, pn, i) for i, (kw, ar, pn) in enumerate(page_tasks)],
                    return_exceptions=True
                )

                # 結果をマージ
                for result in results:
                    if isinstance(result, list):
                        all_results.extend(result)
                        # リアルタイム件数報告
                        self._report_count(len(all_results))
                    elif isinstance(result, Exception):
                        logger.error(f"Task failed: {result}")
                        self.performance_monitor.record_error()

            finally:
                await browser.close()

        self.results = all_results

        # パフォーマンス測定終了
        metrics = self.performance_monitor.finish()
        logger.info(f"Scraping completed: {metrics}")
        logger.info(f"Error stats: {self.error_counter}")

        return all_results

    @abstractmethod
    async def extract_detail_info(self, page: Page, url: str) -> Dict[str, Any]:
        """詳細ページから追加情報を取得（サイトごとに実装）"""
        pass
