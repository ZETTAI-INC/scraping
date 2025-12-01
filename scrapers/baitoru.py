"""
バイトル専用スクレイパー
2024年12月版 - 新しいサイト構造に対応
"""
import asyncio
import random
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from playwright.async_api import Page, Browser, TimeoutError as PlaywrightTimeoutError
from .base_scraper import BaseScraper
from utils.user_agents import ua_rotator
from utils.proxy import proxy_rotator
from utils.stealth import StealthConfig, create_stealth_context
import logging
import re

logger = logging.getLogger(__name__)


class BaitoruScraper(BaseScraper):
    """バイトル用スクレイパー"""

    def __init__(self):
        super().__init__(site_name="baitoru")

    def generate_search_url(self, keyword: str, area: str, page: int = 1) -> str:
        """
        バイトル用の検索URL生成

        バイトルのURL構造:
        - カテゴリなし: https://www.baitoru.com/{region}/jlist/{prefecture}/{area}/
        - カテゴリあり（新着順）: https://www.baitoru.com/{region}/jlist/{prefecture}/{area}/{category}/srt2/
        - キーワード検索（新着順）: https://www.baitoru.com/{region}/jlist/{prefecture}/{area}/kw{keyword}/srt2/
        - ページ指定: 上記 + page{page}/

        Args:
            keyword: 職種カテゴリ名（販売, 飲食, 事務 など）またはキーワード
            area: エリア名（東京, 大阪 など）
            page: ページ番号
        """
        area_codes = self.site_config.get("area_codes", {})
        area_config = area_codes.get(area, area_codes.get("東京"))

        if isinstance(area_config, dict):
            region = area_config.get("region", "kanto")
            prefecture = area_config.get("prefecture", "tokyo")
            area_path = area_config.get("area", "23ku")
        else:
            # フォールバック: 東京23区
            region = "kanto"
            prefecture = "tokyo"
            area_path = "23ku"

        # 職種カテゴリコードを取得
        job_categories = self.site_config.get("job_categories", {})
        category = job_categories.get(keyword, "") if keyword else ""

        if category:
            # カテゴリ指定あり（新着順 srt2）
            if page == 1:
                url_pattern = self.site_config.get("search_url_pattern_category")
                url = url_pattern.format(
                    region=region, prefecture=prefecture,
                    area=area_path, category=category
                )
            else:
                url_pattern = self.site_config.get("search_url_pattern_category_page")
                url = url_pattern.format(
                    region=region, prefecture=prefecture,
                    area=area_path, category=category, page=page
                )
        elif keyword:
            # カテゴリが見つからない場合はキーワード検索にフォールバック
            # バイトルのキーワード検索URL: https://www.baitoru.com/kw/{keyword}/srt2/
            # 注意: キーワード検索は全国検索となり、エリア絞り込みはできない
            logger.info(f"Category not found for '{keyword}', using keyword search (nationwide)")
            from urllib.parse import quote
            encoded_keyword = quote(keyword, safe='')
            if page == 1:
                url = f"https://www.baitoru.com/kw/{encoded_keyword}/srt2/"
            else:
                url = f"https://www.baitoru.com/kw/{encoded_keyword}/srt2/page{page}/"
        else:
            # キーワードなし
            if page == 1:
                url_pattern = self.site_config.get("search_url_pattern")
                url = url_pattern.format(region=region, prefecture=prefecture, area=area_path)
            else:
                url_pattern = self.site_config.get("search_url_pattern_page")
                url = url_pattern.format(region=region, prefecture=prefecture, area=area_path, page=page)

        logger.info(f"Generated URL: {url}")
        return url

    async def _is_pr_card(self, card, is_first: bool, is_last: bool) -> bool:
        """
        PRカード（広告）かどうかを判定

        PRカードの法則:
        1. 最初のカード（list-listingの直後）
        2. DIV（list-infoArea）の直後のカード
        3. 中間のlist-listingの直後のカード
        4. 最後のカード

        判定方法: 前の兄弟要素がlist-jobListDetailでなければPR
        """
        # 最初と最後はPR
        if is_first or is_last:
            return True

        # 前の兄弟要素をチェック
        try:
            prev_info = await card.evaluate("""(el) => {
                const prev = el.previousElementSibling;
                if (!prev) return { hasPrev: false };
                return {
                    hasPrev: true,
                    isJobDetail: prev.classList.contains('list-jobListDetail'),
                    tagName: prev.tagName,
                    className: prev.className
                };
            }""")

            if not prev_info.get('hasPrev'):
                return True  # 前の要素がない場合はPR扱い

            # 前の要素がlist-jobListDetailでなければPR
            if not prev_info.get('isJobDetail'):
                return True

        except Exception as e:
            logger.warning(f"Error checking PR status: {e}")

        return False

    async def _extract_card_data(self, card) -> Optional[Dict[str, Any]]:
        """
        求人カードからデータを抽出
        """
        try:
            data = {}

            # 詳細ページへのリンク
            link_elem = await card.query_selector(".pt02b .ul01 .li01 h3 a")
            if link_elem:
                href = await link_elem.get_attribute("href")
                if href:
                    if href.startswith("/"):
                        href = f"https://www.baitoru.com{href}"
                    data["page_url"] = href

                    # 求人IDを抽出 (例: job151221507)
                    match = re.search(r"job(\d+)", href)
                    if match:
                        data["job_number"] = match.group(1)

            # タイトル
            title_elem = await card.query_selector(".pt02b .ul01 .li01 h3 a span")
            if title_elem:
                title_text = await title_elem.inner_text()
                data["title"] = title_text.strip() if title_text else ""

            # 会社名
            company_elem = await card.query_selector(".pt02b > p")
            if company_elem:
                company_text = await company_elem.inner_text()
                data["company_name"] = company_text.strip() if company_text else ""

            # 給与
            salary_elem = await card.query_selector(".pt03 dl:nth-child(2) dd li em")
            if salary_elem:
                salary_text = await salary_elem.inner_text()
                data["salary"] = salary_text.strip() if salary_text else ""

            # 勤務地
            location_elem = await card.query_selector(".pt02b .ul02 li")
            if location_elem:
                location_text = await location_elem.inner_text()
                # "[勤務地・面接地] " プレフィックスを除去
                location_text = re.sub(r"^\[勤務地.*?\]\s*", "", location_text.strip())
                data["location"] = location_text

            # 雇用形態
            employment_elem = await card.query_selector(".pt01a .ul01 li:first-child")
            if employment_elem:
                employment_text = await employment_elem.inner_text()
                data["employment_type"] = employment_text.strip() if employment_text else ""

            # 職種
            job_type_elem = await card.query_selector(".pt03 dl:first-child dd li")
            if job_type_elem:
                job_type_text = await job_type_elem.inner_text()
                # [ア・パ] などのプレフィックスを除去
                job_type_text = re.sub(r"^\[.*?\]\s*", "", job_type_text.strip())
                data["job_type"] = job_type_text

            # 勤務時間
            hours_elem = await card.query_selector(".pt03 dl:nth-child(3) dd li")
            if hours_elem:
                hours_text = await hours_elem.inner_text()
                # [ア・パ] などのプレフィックスを除去
                hours_text = re.sub(r"^\[.*?\]\s*", "", hours_text.strip())
                data["working_hours"] = hours_text

            # 特徴タグ
            tags = []
            tag_elems = await card.query_selector_all(".pt04 ul li em")
            for tag_elem in tag_elems:
                tag_text = await tag_elem.inner_text()
                if tag_text:
                    tags.append(tag_text.strip())
            if tags:
                data["tags"] = ", ".join(tags)

            # 仕事番号
            job_no_elem = await card.query_selector(".pt09 .p06")
            if job_no_elem:
                job_no_text = await job_no_elem.inner_text()
                data["job_number_display"] = job_no_text.strip() if job_no_text else ""

            return data if data.get("page_url") else None

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

            success = False
            for attempt in range(2):
                try:
                    response = await page.goto(
                        url,
                        wait_until="domcontentloaded",
                        timeout=30000 if attempt == 0 else 40000
                    )

                    if response and response.status == 404:
                        logger.warning(f"Page not found: {url}")
                        break

                    # ページ読み込み待機
                    await page.wait_for_timeout(2000)

                    card_selector = self.selectors.get("job_cards", "article.list-jobListDetail")

                    # カードが描画されるまで待機
                    selector_ready = False
                    for sel_attempt in range(4):
                        try:
                            await page.wait_for_selector(card_selector, timeout=3000 + 500 * sel_attempt)
                            selector_ready = True
                            break
                        except PlaywrightTimeoutError:
                            logger.warning(
                                f"Job cards selector timeout on page {page_num} (attempt {sel_attempt + 1}/4)"
                            )
                            await page.wait_for_timeout(600 + 200 * sel_attempt)

                    if not selector_ready:
                        logger.warning(f"Job cards selector not ready on page {page_num}")
                        if attempt == 0:
                            continue

                    # 求人カードを取得
                    job_cards = await page.query_selector_all(card_selector)

                    if len(job_cards) == 0:
                        await page.wait_for_timeout(1000)
                        job_cards = await page.query_selector_all(card_selector)

                    if len(job_cards) == 0:
                        logger.warning(f"No job cards found on page {page_num}")
                        if attempt == 0:
                            await page.wait_for_timeout(1200)
                            continue
                        else:
                            logger.info(f"No jobs on page {page_num} after retries")
                            success = True
                            break

                    logger.info(f"Found {len(job_cards)} jobs on page {page_num}")

                    # キーワード検索（/kw/）の場合はPRスキップなし
                    # カテゴリ検索の場合はPRカードをスキップ
                    is_keyword_search = "/kw/" in url
                    if is_keyword_search:
                        logger.info("Keyword search - not skipping PR cards")
                        for card in job_cards:
                            try:
                                job_data = await self._extract_card_data(card)
                                if job_data:
                                    all_jobs.append(job_data)
                            except Exception as e:
                                logger.error(f"Error extracting job card: {e}")
                                continue
                    else:
                        # カテゴリ検索: PRカードをスキップ
                        pr_count = 0
                        total_cards = len(job_cards)
                        for idx, card in enumerate(job_cards):
                            is_first = (idx == 0)
                            is_last = (idx == total_cards - 1)

                            try:
                                if await self._is_pr_card(card, is_first, is_last):
                                    pr_count += 1
                                    continue

                                job_data = await self._extract_card_data(card)
                                if job_data:
                                    all_jobs.append(job_data)
                            except Exception as e:
                                logger.error(f"Error extracting job card: {e}")
                                continue

                        if pr_count > 0:
                            logger.info(f"Category search - skipped {pr_count} PR card(s)")

                    success = True
                    break

                except Exception as e:
                    logger.error(f"Error fetching page {page_num} (attempt {attempt + 1}/2): {e}")
                    if attempt == 0:
                        await page.wait_for_timeout(1500)
                        continue
                    else:
                        break

            if not success:
                break

            # ページネーションの確認
            await page.wait_for_timeout(500)

        return all_jobs

    async def extract_detail_info(self, page: Page, url: str) -> Dict[str, Any]:
        """
        詳細ページから追加情報を取得
        """
        detail_data = {}

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

            body_text = await page.inner_text("body")

            # 郵便番号と住所の抽出
            postal_match = re.search(r"(\d{3})-?(\d{4})(東京都|大阪府|北海道|京都府|.{2,3}県)(.+?)(?=\n|交通|地図|※)", body_text)
            if postal_match:
                detail_data["postal_code"] = postal_match.group(1) + postal_match.group(2)
                detail_data["address"] = postal_match.group(3) + postal_match.group(4).strip()

            # 電話番号の抽出
            phone_match = re.search(r"(\d{2,4}[-]?\d{2,4}[-]?\d{3,4})", body_text)
            if phone_match:
                detail_data["phone"] = phone_match.group(1).replace("-", "")

            # 事業内容
            business_match = re.search(r"事業内容\s*[\n\r]*(.+?)(?=\n所在|$)", body_text)
            if business_match:
                detail_data["business_content"] = business_match.group(1).strip()

            # 仕事内容
            desc_match = re.search(r"仕事内容\s*[\n\r]*(.+?)(?=\n勤務地|$)", body_text, re.DOTALL)
            if desc_match:
                detail_data["job_description"] = desc_match.group(1).strip()[:500]

        except Exception as e:
            logger.error(f"Error extracting detail info from {url}: {e}")

        return detail_data

    async def scrape_with_details(self, page: Page, keyword: str, area: str,
                                   max_pages: int = 5, fetch_details: bool = True) -> List[Dict[str, Any]]:
        """
        求人検索と詳細情報取得を実行
        """
        jobs = await self.search_jobs(page, keyword, area, max_pages)

        if not fetch_details:
            return jobs

        for i, job in enumerate(jobs):
            if job.get("page_url"):
                logger.info(f"Fetching detail {i+1}/{len(jobs)}: {job['page_url']}")
                try:
                    detail_data = await self.extract_detail_info(page, job["page_url"])
                    job.update(detail_data)
                    await page.wait_for_timeout(1000)
                except Exception as e:
                    logger.error(f"Error fetching detail for job {i+1}: {e}")

        return jobs

    async def scrape_single_page(
        self,
        browser: Browser,
        keyword: str,
        area: str,
        page_num: int,
        task_idx: int = 0
    ) -> List[Dict[str, Any]]:
        """
        バイトル用: 1ページを並列用にスクレイピング
        """
        stagger_delay = task_idx * 1.5 + random.uniform(0.5, 1.5)
        logger.info(f"[タスク{task_idx+1}] {stagger_delay:.1f}秒後に開始...")
        await asyncio.sleep(stagger_delay)

        user_agent = ua_rotator.get_random()

        proxy_config = None
        if proxy_rotator.is_enabled():
            proxy = proxy_rotator.get_random()
            if proxy:
                proxy_config = proxy.to_playwright_format()

        context = await create_stealth_context(
            browser,
            user_agent=user_agent,
            proxy=proxy_config
        )

        jobs = []
        try:
            page = await context.new_page()
            await StealthConfig.apply_stealth_scripts(page)

            jobs = await self._scrape_single_page_impl(page, keyword, area, page_num)
            self.performance_monitor.record_item(len(jobs))

        except Exception as e:
            logger.error(f"Error scraping page {page_num}: {e}", exc_info=True)

        finally:
            await context.close()

        return jobs

    async def _scrape_single_page_impl(
        self,
        page: Page,
        keyword: str,
        area: str,
        page_num: int
    ) -> List[Dict[str, Any]]:
        """
        バイトル: 1ページ分の求人を取得する実装
        """
        jobs = []
        url = self.generate_search_url(keyword, area, page_num)
        logger.info(f"Fetching page {page_num}: {url}")

        for attempt in range(2):
            try:
                response = await page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=60000
                )
                await page.wait_for_timeout(2000)

                if response and response.status == 404:
                    logger.warning(f"Page not found: {url}")
                    return jobs

                if response and response.status == 403:
                    logger.error(f"Access blocked (403): {url}")
                    return jobs

                card_selector = self.selectors.get("job_cards", "article.list-jobListDetail")

                selector_ready = False
                for sel_attempt in range(4):
                    try:
                        await page.wait_for_selector(card_selector, timeout=3000 + 500 * sel_attempt)
                        selector_ready = True
                        break
                    except PlaywrightTimeoutError:
                        logger.warning(
                            f"Job cards selector timeout on page {page_num} (attempt {sel_attempt + 1}/4)"
                        )
                        await page.wait_for_timeout(600 + 200 * sel_attempt)

                if not selector_ready:
                    logger.warning(f"Job cards selector not ready on page {page_num}")
                    if attempt == 0:
                        continue

                job_cards = await page.query_selector_all(card_selector)

                if len(job_cards) == 0:
                    await page.wait_for_timeout(1000)
                    job_cards = await page.query_selector_all(card_selector)

                if len(job_cards) == 0:
                    logger.warning(f"No job cards found on page {page_num}")
                    if attempt == 0:
                        await page.wait_for_timeout(1200)
                        continue
                    else:
                        return jobs

                logger.info(f"Found {len(job_cards)} jobs on page {page_num}")

                # キーワード検索（/kw/）の場合はPRスキップなし
                # カテゴリ検索の場合はPRカードをスキップ
                is_keyword_search = "/kw/" in url
                if is_keyword_search:
                    logger.info("Keyword search - not skipping PR cards")
                    for card in job_cards:
                        try:
                            job_data = await self._extract_card_data(card)
                            if job_data:
                                jobs.append(job_data)
                        except Exception as e:
                            logger.error(f"Error extracting job card: {e}")
                            continue
                else:
                    # カテゴリ検索: PRカードをスキップ
                    pr_count = 0
                    total_cards = len(job_cards)
                    for idx, card in enumerate(job_cards):
                        is_first = (idx == 0)
                        is_last = (idx == total_cards - 1)

                        try:
                            if await self._is_pr_card(card, is_first, is_last):
                                pr_count += 1
                                continue

                            job_data = await self._extract_card_data(card)
                            if job_data:
                                jobs.append(job_data)
                        except Exception as e:
                            logger.error(f"Error extracting job card: {e}")
                            continue

                    if pr_count > 0:
                        logger.info(f"Category search - skipped {pr_count} PR card(s)")

                return jobs

            except Exception as e:
                logger.error(f"Error fetching page {page_num} (attempt {attempt + 1}/2): {e}")
                if attempt == 0:
                    await page.wait_for_timeout(1500)
                    continue
                else:
                    return jobs

        return jobs
