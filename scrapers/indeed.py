"""
Indeed Japan専用スクレイパー
2024年更新版 - 新しいサイト構造に対応
"""
import asyncio
import random
import re
from typing import Dict, Any, List, Optional
from urllib.parse import quote
from playwright.async_api import Page, Browser, TimeoutError as PlaywrightTimeoutError
from .base_scraper import BaseScraper
from utils.user_agents import ua_rotator
from utils.proxy import proxy_rotator
from utils.stealth import StealthConfig, create_stealth_context
import logging

logger = logging.getLogger(__name__)


class IndeedScraper(BaseScraper):
    """Indeed Japan用スクレイパー"""

    def __init__(self):
        super().__init__(site_name="indeed")

    def generate_search_url(self, keyword: str, area: str, page: int = 1) -> str:
        """
        Indeed用の検索URL生成

        URL形式: https://jp.indeed.com/jobs?q={keyword}&l={area}&start={offset}
        - q: 検索キーワード（URLエンコード）
        - l: 勤務地（URLエンコード）
        - start: オフセット（0, 15, 30, ...）
        """
        # エリア名を正式名称に変換
        area_codes = self.site_config.get("area_codes", {})
        area_name = area_codes.get(area, area)

        # キーワードを変換（カテゴリマッピングがあれば使用）
        job_categories = self.site_config.get("job_categories", {})
        search_keyword = job_categories.get(keyword, keyword)

        # オフセット計算（1ページ目=0, 2ページ目=15, ...）
        pagination = self.site_config.get("pagination", {})
        increment = pagination.get("increment", 15)
        offset = (page - 1) * increment

        # URLエンコード
        encoded_keyword = quote(search_keyword)
        encoded_area = quote(area_name)

        url = f"https://jp.indeed.com/jobs?q={encoded_keyword}&l={encoded_area}&start={offset}"
        logger.info(f"Generated Indeed URL: {url}")

        return url

    async def _extract_card_data(self, card) -> Optional[Dict[str, Any]]:
        """
        求人カードからデータを抽出
        2024年版 - テキスト解析方式
        """
        try:
            data = {}

            # タイトル
            title_elem = await card.query_selector(".jobTitle")
            if title_elem:
                data["title"] = (await title_elem.inner_text()).strip()

            # 会社名
            company_elem = await card.query_selector("[data-testid='company-name']")
            if company_elem:
                data["company_name"] = (await company_elem.inner_text()).strip()

            # カード全体のテキストを取得（勤務地・給与の抽出用）
            card_text = await card.inner_text()
            lines = card_text.split('\n')

            # 勤務地 - 会社名の次の行から抽出
            if data.get("company_name"):
                for j, line in enumerate(lines):
                    if data["company_name"] in line and j + 1 < len(lines):
                        location = lines[j + 1].strip()
                        # 勤務地らしい行かチェック（都道府県名を含む）
                        if any(pref in location for pref in ["東京", "大阪", "北海道", "京都", "県", "府", "都"]):
                            data["location"] = location
                        break

            # 給与 - テキストから正規表現で抽出
            salary_match = re.search(r'(月給|年収|時給)[\s\d,.万円~～\-−]+', card_text)
            if salary_match:
                data["salary"] = salary_match.group(0).strip()

            # 詳細ページURL
            link_elem = await card.query_selector("a.jcs-JobTitle")
            if not link_elem:
                link_elem = await card.query_selector("h2 a")
            if link_elem:
                href = await link_elem.get_attribute("href")
                if href:
                    if href.startswith("/"):
                        href = f"https://jp.indeed.com{href}"
                    data["page_url"] = href

                    # 求人IDを抽出
                    jk_match = re.search(r'jk=([a-f0-9]+)', href)
                    if jk_match:
                        data["job_number"] = jk_match.group(1)

            # サイト名
            data["site"] = "Indeed"

            # タイトルがあれば返す
            if data.get("title"):
                return data

            return None

        except Exception as e:
            logger.error(f"Error extracting card data: {e}")
            return None

    async def search_jobs(self, page: Page, keyword: str, area: str, max_pages: int = 5) -> List[Dict[str, Any]]:
        """
        求人検索を実行し、結果を返す
        """
        all_jobs = []

        for page_num in range(1, max_pages + 1):
            url = self.generate_search_url(keyword, area, page_num)
            logger.info(f"Fetching page {page_num}: {url}")

            try:
                response = await page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=30000
                )

                # ページロード後の待機（JS描画用）
                await page.wait_for_timeout(3000)

                if response and response.status == 403:
                    logger.error(f"Access blocked (403): {url}")
                    # 403でも既に取得したデータは返す
                    return all_jobs

                # 求人カードを取得
                card_selector = self.selectors.get("job_cards", ".job_seen_beacon")

                # カードが描画されるまで待機
                try:
                    await page.wait_for_selector(card_selector, timeout=10000)
                except PlaywrightTimeoutError:
                    logger.warning(f"No job cards found on page {page_num}")
                    break

                job_cards = await page.query_selector_all(card_selector)
                logger.info(f"Found {len(job_cards)} jobs on page {page_num}")

                if len(job_cards) == 0:
                    logger.info(f"No more jobs found at page {page_num}")
                    break

                for card in job_cards:
                    try:
                        job_data = await self._extract_card_data(card)
                        if job_data:
                            all_jobs.append(job_data)
                    except Exception as e:
                        logger.error(f"Error extracting job card: {e}")
                        continue

                # 次のページへの待機
                await page.wait_for_timeout(random.uniform(1000, 2000))

            except Exception as e:
                logger.error(f"Error fetching page {page_num}: {e}")
                break

        return all_jobs

    async def scrape_single_page(
        self,
        browser: Browser,
        keyword: str,
        area: str,
        page_num: int,
        task_idx: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Indeed用: 1ページを並列用にスクレイピング
        """
        # タスク開始前にスタッガード遅延（同時アクセスを避ける）
        stagger_delay = task_idx * 2.0 + random.uniform(1.0, 2.0)
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

            # Indeed用の1ページ取得処理
            jobs = await self._scrape_single_page_impl(page, keyword, area, page_num)

            self.performance_monitor.record_item(len(jobs))

        except Exception as e:
            logger.error(f"Error scraping page {page_num}: {e}", exc_info=True)

        finally:
            await context.close()

        return jobs

    async def _check_no_results(self, page: Page) -> bool:
        """
        検索結果が0件かどうかをチェック
        早期にリターンしてセレクタタイムアウトを避ける
        """
        try:
            no_results = await page.evaluate("""() => {
                const body = document.body.innerText;
                return body.includes('求人が見つかりませんでした') ||
                       body.includes('に一致する求人はありません') ||
                       body.includes('検索条件に一致する求人がありません') ||
                       body.includes('該当する求人がありません') ||
                       body.includes('No results found') ||
                       body.includes('0件の求人');
            }""")
            return no_results
        except Exception as e:
            logger.debug(f"[Indeed] 0件チェックエラー（続行）: {e}")
            return False

    async def _scrape_single_page_impl(
        self,
        page: Page,
        keyword: str,
        area: str,
        page_num: int
    ) -> List[Dict[str, Any]]:
        """
        Indeed: 1ページ分の求人を取得する実装
        """
        jobs = []
        url = self.generate_search_url(keyword, area, page_num)
        logger.info(f"Fetching page {page_num}: {url}")

        try:
            response = await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=60000
            )

            # DOMロード後に追加で待機（JSレンダリング用）
            await page.wait_for_timeout(3000)

            if response and response.status == 403:
                logger.error(f"Access blocked (403): {url}")
                return jobs

            if response and response.status == 404:
                logger.warning(f"Page not found: {url}")
                return jobs

            # ★ 検索結果0件の早期検出（10秒タイムアウトを避けて次のエリアに進む）
            no_results_detected = await self._check_no_results(page)
            if no_results_detected:
                logger.info(f"[Indeed] 検索結果0件を検出 - {area} × {keyword} (ページ{page_num})")
                return jobs  # 空リストを返して次のエリアへ

            card_selector = self.selectors.get("job_cards", ".job_seen_beacon")

            # カードが描画されるまで待機
            try:
                await page.wait_for_selector(card_selector, timeout=10000)
            except PlaywrightTimeoutError:
                logger.warning(f"Job cards not found on page {page_num}")
                return jobs

            job_cards = await page.query_selector_all(card_selector)
            logger.info(f"Found {len(job_cards)} jobs on page {page_num}")

            for card in job_cards:
                try:
                    job_data = await self._extract_card_data(card)
                    if job_data:
                        jobs.append(job_data)
                except Exception as e:
                    logger.error(f"Error extracting job card: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error fetching page {page_num}: {e}")

        return jobs

    async def extract_detail_info(self, page: Page, url: str) -> Dict[str, Any]:
        """
        詳細ページから追加情報を取得
        """
        detail_data = {}

        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(2000)

            # 求人説明文
            desc_elem = await page.query_selector("#jobDescriptionText")
            if desc_elem:
                desc_text = (await desc_elem.inner_text()).strip()
                detail_data["job_description"] = desc_text[:1000]  # 最大1000文字

            # 会社情報
            company_info = await page.query_selector(".jobsearch-CompanyInfoContainer")
            if company_info:
                info_text = (await company_info.inner_text()).strip()
                detail_data["company_info"] = info_text[:500]

        except Exception as e:
            logger.error(f"Error extracting detail info from {url}: {e}")

        return detail_data
