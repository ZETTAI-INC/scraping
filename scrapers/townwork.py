"""
タウンワーク専用スクレイパー
2024年更新版 - 新しいサイト構造に対応
"""
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from playwright.async_api import Page, Browser
from .base_scraper import BaseScraper
import logging
import re

logger = logging.getLogger(__name__)


class TownworkScraper(BaseScraper):
    """タウンワーク用スクレイパー"""

    def __init__(self):
        super().__init__(site_name="townwork")

    async def extract_job_card(self, card_element, page: Page) -> Dict[str, Any]:
        """
        タウンワーク用の求人カード情報抽出
        新しいCSS Module形式のクラス名に対応
        """
        job_data = {
            "site": "タウンワーク",
            "title": "",
            "company": "",
            "location": "",
            "salary": "",
            "employment_type": "",
            "url": "",
        }

        try:
            # 詳細ページへのリンク（カード自体がリンクの場合）
            href = await card_element.get_attribute("href")
            if href:
                if href.startswith("/"):
                    href = f"https://townwork.net{href}"
                job_data["url"] = href

                # 求人IDを抽出
                match = re.search(r"jobid_([a-f0-9]+)", href)
                if match:
                    job_data["job_id"] = match.group(1)

            # タイトル
            title_elem = await card_element.query_selector("[class*='title__']")
            if title_elem:
                job_data["title"] = (await title_elem.inner_text()).strip()

            # 会社名
            company_elem = await card_element.query_selector("[class*='employerName']")
            if company_elem:
                job_data["company"] = (await company_elem.inner_text()).strip()

            # 給与
            salary_elem = await card_element.query_selector("[class*='salaryText']")
            if salary_elem:
                job_data["salary"] = (await salary_elem.inner_text()).strip()

            # アクセス・勤務地
            access_elem = await card_element.query_selector("[class*='accessText']")
            if access_elem:
                access_text = (await access_elem.inner_text()).strip()
                # "交通・アクセス " プレフィックスを除去
                job_data["location"] = re.sub(r"^交通・アクセス\s*", "", access_text)

            # 雇用形態
            job_type_elem = await card_element.query_selector("[class*='jobType']")
            if job_type_elem:
                job_data["employment_type"] = (await job_type_elem.inner_text()).strip()

        except Exception as e:
            logger.error(f"Error extracting job card: {e}")

        return job_data

    def generate_search_url(self, keyword: str, area: str, page: int = 1) -> str:
        """
        タウンワーク用の検索URL生成
        新しいURL形式: https://townwork.net/prefectures/{area}/job_search/?keyword={keyword}&page={page}
        """
        # エリア名をローマ字に変換
        area_codes = self.site_config.get("area_codes", {})
        area_code = area_codes.get(area, area.lower())

        url_pattern = self.site_config.get("search_url_pattern")
        base_url = url_pattern.format(area=area_code, keyword=keyword, page=page)

        return base_url

    async def search_jobs(self, page: Page, keyword: str, area: str, max_pages: int = 5) -> List[Dict[str, Any]]:
        """
        求人検索を実行し、結果を返す
        """
        all_jobs = []

        for page_num in range(1, max_pages + 1):
            url = self.generate_search_url(keyword, area, page_num)
            logger.info(f"Fetching page {page_num}: {url}")

            try:
                response = await page.goto(url, wait_until="networkidle", timeout=30000)

                if response and response.status == 404:
                    logger.warning(f"Page not found: {url}")
                    break

                await page.wait_for_timeout(2000)  # ページ読み込み待機

                # 求人カードを取得
                job_cards = await page.query_selector_all(self.selectors.get("job_cards", "[class*='jobCard']"))

                if not job_cards:
                    logger.info(f"No more jobs found on page {page_num}")
                    break

                logger.info(f"Found {len(job_cards)} jobs on page {page_num}")

                for card in job_cards:
                    try:
                        job_data = await self._extract_card_data(card)
                        if job_data:
                            all_jobs.append(job_data)
                    except Exception as e:
                        logger.error(f"Error extracting job card: {e}")
                        continue

                # 次のページがあるか確認
                next_page = await page.query_selector(f"[class*='pageButton']:has-text('{page_num + 1}')")
                if not next_page:
                    logger.info("No more pages available")
                    break

            except Exception as e:
                logger.error(f"Error fetching page {page_num}: {e}")
                break

        return all_jobs

    async def _extract_card_data(self, card) -> Optional[Dict[str, Any]]:
        """
        求人カードからデータを抽出
        """
        try:
            data = {}

            # 詳細ページへのリンク
            href = await card.get_attribute("href")
            if href:
                if href.startswith("/"):
                    href = f"https://townwork.net{href}"
                data["page_url"] = href

                # 求人IDを抽出
                match = re.search(r"jobid_([a-f0-9]+)", href)
                if match:
                    data["job_number"] = match.group(1)

            # タイトル
            title_elem = await card.query_selector("[class*='title__']")
            if title_elem:
                data["title"] = (await title_elem.inner_text()).strip()

            # 会社名
            company_elem = await card.query_selector("[class*='employerName']")
            if company_elem:
                data["company_name"] = (await company_elem.inner_text()).strip()

            # 給与
            salary_elem = await card.query_selector("[class*='salaryText']")
            if salary_elem:
                data["salary"] = (await salary_elem.inner_text()).strip()

            # アクセス・勤務地
            access_elem = await card.query_selector("[class*='accessText']")
            if access_elem:
                access_text = (await access_elem.inner_text()).strip()
                # "交通・アクセス " プレフィックスを除去
                data["location"] = re.sub(r"^交通・アクセス\s*", "", access_text)

            # 雇用形態
            job_type_elem = await card.query_selector("[class*='jobType']")
            if job_type_elem:
                data["employment_type"] = (await job_type_elem.inner_text()).strip()

            return data if data.get("page_url") else None

        except Exception as e:
            logger.error(f"Error extracting card data: {e}")
            return None

    async def extract_detail_info(self, page: Page, url: str) -> Dict[str, Any]:
        """
        詳細ページから追加情報を取得

        取得項目:
        - 会社名カナ
        - 郵便番号
        - 住所詳細
        - 電話番号
        - FAX番号
        - 担当者名
        - 求人番号
        - 事業内容
        - 従業員数
        """
        detail_data = {}

        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(2000)

            # ページ全体のテキストを取得して解析
            body_text = await page.inner_text("body")

            # 郵便番号と住所の抽出
            # パターン: 郵便番号 + 都道府県から始まる住所
            postal_match = re.search(r"(\d{3})-?(\d{4})(東京都|大阪府|北海道|京都府|.{2,3}県)(.+?)(?=\n|交通|地図|※)", body_text)
            if postal_match:
                detail_data["postal_code"] = postal_match.group(1) + postal_match.group(2)
                detail_data["address"] = postal_match.group(3) + postal_match.group(4).strip()
            else:
                # 別のパターン：郵便番号なしで都道府県から始まる
                addr_match = re.search(r"(東京都|大阪府|北海道|京都府|.{2,3}県)(.{5,50}?)(?=\n|交通|地図|※)", body_text)
                if addr_match:
                    detail_data["address"] = addr_match.group(1) + addr_match.group(2).strip()

            # 電話番号の抽出（代表電話番号）
            phone_match = re.search(r"代表電話番号\s*[\n\r]*(\d{10,11})", body_text)
            if phone_match:
                detail_data["phone"] = phone_match.group(1)
            else:
                # 別のパターン
                phone_match2 = re.search(r"電話番号[：:\s]*(\d{2,4}[-]?\d{2,4}[-]?\d{3,4})", body_text)
                if phone_match2:
                    detail_data["phone"] = phone_match2.group(1).replace("-", "")

            # 会社名
            company_elem = await page.query_selector("[class*='companyName'], [class*='employerName']")
            if company_elem:
                detail_data["company_name"] = (await company_elem.inner_text()).strip()

            # 事業内容
            business_match = re.search(r"事業内容\s*[\n\r]*(.+?)(?=\n所在|$)", body_text)
            if business_match:
                detail_data["business_content"] = business_match.group(1).strip()

            # 原稿ID（求人番号）
            job_id_match = re.search(r"原稿ID[：:\s]*([a-f0-9]+)", body_text)
            if job_id_match:
                detail_data["job_number"] = job_id_match.group(1)

            # 仕事内容
            desc_match = re.search(r"仕事内容\s*[\n\r]*(.+?)(?=\n勤務地|$)", body_text, re.DOTALL)
            if desc_match:
                detail_data["job_description"] = desc_match.group(1).strip()[:500]  # 最大500文字

            # 勤務時間
            time_match = re.search(r"勤務時間詳細\s*[\n\r]*勤務時間\s*[\n\r]*(.+?)(?=\n|$)", body_text)
            if time_match:
                detail_data["working_hours"] = time_match.group(1).strip()

            # 休日休暇
            holiday_match = re.search(r"休日休暇\s*[\n\r]*(.+?)(?=\n職場|$)", body_text)
            if holiday_match:
                detail_data["holidays"] = holiday_match.group(1).strip()

            # 応募資格
            qualification_match = re.search(r"求めている人材\s*[\n\r]*(.+?)(?=\n試用|$)", body_text, re.DOTALL)
            if qualification_match:
                detail_data["qualifications"] = qualification_match.group(1).strip()[:300]

        except Exception as e:
            logger.error(f"Error extracting detail info from {url}: {e}")

        return detail_data

    async def scrape_with_details(self, page: Page, keyword: str, area: str,
                                   max_pages: int = 5, fetch_details: bool = True) -> List[Dict[str, Any]]:
        """
        求人検索と詳細情報取得を実行
        """
        # まず検索結果を取得
        jobs = await self.search_jobs(page, keyword, area, max_pages)

        if not fetch_details:
            return jobs

        # 各求人の詳細情報を取得
        for i, job in enumerate(jobs):
            if job.get("page_url"):
                logger.info(f"Fetching detail {i+1}/{len(jobs)}: {job['page_url']}")
                try:
                    detail_data = await self.extract_detail_info(page, job["page_url"])
                    job.update(detail_data)
                    await page.wait_for_timeout(1000)  # サーバーに負荷をかけないよう待機
                except Exception as e:
                    logger.error(f"Error fetching detail for job {i+1}: {e}")

        return jobs
