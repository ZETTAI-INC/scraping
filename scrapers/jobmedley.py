"""
ジョブメドレー専用スクレイパー
https://job-medley.com/

JSON-LD構造化データを活用したスクレイピング
"""
import asyncio
import random
import re
import json
from typing import Dict, Any, List, Optional
from playwright.async_api import Page, Browser, TimeoutError as PlaywrightTimeoutError
from .base_scraper import BaseScraper
from utils.stealth import StealthConfig, create_stealth_context
import logging

logger = logging.getLogger(__name__)


class JobmedleyScraper(BaseScraper):
    """ジョブメドレー用スクレイパー"""

    # 都道府県ID (job-medley.com独自)
    PREFECTURE_IDS = {
        "北海道": 1,
        "青森": 2, "岩手": 3, "宮城": 4, "秋田": 5, "山形": 6, "福島": 7,
        "茨城": 8, "栃木": 9, "群馬": 10, "埼玉": 11, "千葉": 12, "東京": 13, "神奈川": 14,
        "新潟": 15, "富山": 16, "石川": 17, "福井": 18, "山梨": 19, "長野": 20,
        "岐阜": 21, "静岡": 22, "愛知": 23, "三重": 24,
        "滋賀": 25, "京都": 26, "大阪": 27, "兵庫": 28, "奈良": 29, "和歌山": 30,
        "鳥取": 31, "島根": 32, "岡山": 33, "広島": 34, "山口": 35,
        "徳島": 36, "香川": 37, "愛媛": 38, "高知": 39,
        "福岡": 40, "佐賀": 41, "長崎": 42, "熊本": 43, "大分": 44, "宮崎": 45, "鹿児島": 46, "沖縄": 47,
    }

    # 職種カテゴリコード
    CATEGORY_CODES = {
        # 営業
        "営業": "sr",

        # 介護系
        "介護": "hh",
        "介護職": "hh",
        "ヘルパー": "hh",
        "介護福祉士": "hh",
        "ケアマネ": "cm",
        "ケアマネジャー": "cm",

        # 医療系
        "看護": "ans",
        "看護師": "ans",
        "准看護師": "ans",
        "薬剤師": "apo",
        "理学療法士": "pt",
        "作業療法士": "ot",
        "言語聴覚士": "st",

        # 歯科系
        "歯科衛生士": "dh",
        "歯科医師": "dds",
        "歯科助手": "da",

        # 保育系
        "保育士": "cw",
        "保育": "cw",

        # 事務系
        "事務": "mc",  # 医療事務
        "医療事務": "mc",
        "介護事務": "mc",

        # リハビリ系
        "リハビリ": "pt",

        # 栄養系
        "栄養士": "rd",
        "管理栄養士": "rd",

        # 調理系
        "調理": "ck",
        "調理師": "ck",
    }

    # キーワードからカテゴリ名へのマッピング
    KEYWORD_TO_CATEGORY = {
        "営業": "営業",
        "介護": "介護",
        "介護職": "介護",
        "ヘルパー": "介護",
        "介護福祉士": "介護",
        "ケアマネ": "介護",
        "ケアマネジャー": "介護",
        "看護": "看護",
        "看護師": "看護",
        "准看護師": "看護",
        "事務": "事務",
        "医療事務": "事務",
        "介護事務": "事務",
        "保育士": "保育",
        "保育": "保育",
        # その他は「医療・介護」カテゴリ
    }

    def __init__(self):
        super().__init__(site_name="jobmedley")
        self._realtime_callback = None

    def set_realtime_callback(self, callback):
        """リアルタイム件数コールバックを設定"""
        self._realtime_callback = callback

    def _report_count(self, count: int):
        """件数を報告"""
        if self._realtime_callback:
            self._realtime_callback(count)

    def _get_prefecture_id(self, area: str) -> Optional[int]:
        """エリア名から都道府県IDを取得"""
        area_clean = area.rstrip("都府県")
        return self.PREFECTURE_IDS.get(area_clean, self.PREFECTURE_IDS.get(area))

    def _get_category_code(self, keyword: str) -> str:
        """キーワードから職種カテゴリコードを取得"""
        return self.CATEGORY_CODES.get(keyword, "sr")  # デフォルトは営業

    def _get_category(self, keyword: str) -> str:
        """キーワードからカテゴリ名を取得"""
        return self.KEYWORD_TO_CATEGORY.get(keyword, "医療・介護")

    def generate_search_url(self, keyword: str, area: str, page: int = 1) -> str:
        """
        ジョブメドレー用の検索URL生成

        URL形式:
        - https://job-medley.com/{category_code}/pref{pref_id}/?order=2
        - order=2 は新着順
        - ページ指定: page={page}
        """
        pref_id = self._get_prefecture_id(area)
        if not pref_id:
            logger.warning(f"[ジョブメドレー] 未知の都道府県: {area}")
            pref_id = 13  # デフォルト: 東京

        category_code = self._get_category_code(keyword)

        base_url = f"https://job-medley.com/{category_code}/pref{pref_id}/"

        # クエリパラメータ
        params = ["order=2"]  # 新着順
        if page > 1:
            params.append(f"page={page}")

        url = f"{base_url}?{'&'.join(params)}"
        logger.info(f"[ジョブメドレー] 生成URL: {url}")
        return url

    async def search_jobs(
        self,
        page: Page,
        keyword: str,
        area: str,
        max_pages: int = 3,
        seen_job_ids: set = None
    ) -> List[Dict[str, Any]]:
        """
        求人検索を実行

        Args:
            page: Playwrightのページオブジェクト
            keyword: 検索キーワード
            area: 都道府県名
            max_pages: 最大ページ数
            seen_job_ids: 既に取得済みのjob_idセット

        Returns:
            求人データのリスト
        """
        all_jobs = []
        seen_job_ids = seen_job_ids or set()
        category = self._get_category(keyword)
        category_code = self._get_category_code(keyword)

        for page_num in range(1, max_pages + 1):
            try:
                url = self.generate_search_url(keyword, area, page_num)
                logger.info(f"[ジョブメドレー] ページ {page_num} を取得中: {url}")

                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(random.randint(2000, 4000))

                # 検索結果が0件かチェック
                no_results = await self._check_no_results(page)
                if no_results:
                    logger.info(f"[ジョブメドレー] 検索結果が0件です")
                    break

                # 求人カードを取得
                job_cards = await page.query_selector_all('a[href*="/' + category_code + '/"]')

                if not job_cards:
                    logger.info(f"[ジョブメドレー] ページ {page_num} に求人がありません")
                    break

                page_jobs = []
                for card in job_cards:
                    try:
                        href = await card.get_attribute("href")
                        if not href:
                            continue

                        # 詳細ページへのリンクのみ（/sr/12345/ 形式）
                        match = re.search(rf'/{category_code}/(\d+)/?', href)
                        if not match:
                            continue

                        job_id = match.group(1)

                        # 重複チェック
                        if job_id in seen_job_ids:
                            continue
                        seen_job_ids.add(job_id)

                        # 基本情報を取得
                        job_data = {
                            "job_id": f"jobmedley_{job_id}",
                            "source_job_id": job_id,
                            "url": f"https://job-medley.com{href}" if href.startswith("/") else href,
                            "site": "jobmedley",
                            "keyword": keyword,
                            "category": category,
                            "area": area,
                        }

                        page_jobs.append(job_data)

                    except Exception as e:
                        logger.debug(f"[ジョブメドレー] カード解析エラー: {e}")
                        continue

                # 重複を除去
                unique_jobs = []
                seen_in_page = set()
                for job in page_jobs:
                    if job["job_id"] not in seen_in_page:
                        seen_in_page.add(job["job_id"])
                        unique_jobs.append(job)

                all_jobs.extend(unique_jobs)
                self._report_count(len(all_jobs))
                logger.info(f"[ジョブメドレー] ページ {page_num}: {len(unique_jobs)} 件取得 (累計: {len(all_jobs)} 件)")

                # 次のページがあるかチェック
                has_next = await self._has_next_page(page, page_num)
                if not has_next:
                    logger.info(f"[ジョブメドレー] 最終ページに到達")
                    break

                # 次のページ前に待機
                await page.wait_for_timeout(random.randint(1000, 2000))

            except PlaywrightTimeoutError:
                logger.warning(f"[ジョブメドレー] ページ {page_num} タイムアウト")
                break
            except Exception as e:
                logger.error(f"[ジョブメドレー] ページ {page_num} エラー: {e}")
                break

        return all_jobs

    async def _check_no_results(self, page: Page) -> bool:
        """検索結果が0件かチェック"""
        try:
            # 0件表示のチェック
            no_result_elem = await page.query_selector('text="該当する求人がありません"')
            if no_result_elem:
                return True

            no_result_elem = await page.query_selector('text="0件"')
            if no_result_elem:
                return True

            return False
        except Exception:
            return False

    async def _has_next_page(self, page: Page, current_page: int) -> bool:
        """次のページがあるかチェック"""
        try:
            # 次へボタンまたはページネーション
            next_button = await page.query_selector('a[rel="next"]')
            if next_button:
                return True

            # ページ番号リンク
            next_page_link = await page.query_selector(f'a[href*="page={current_page + 1}"]')
            if next_page_link:
                return True

            return False
        except Exception:
            return False

    async def extract_detail_info(self, page: Page, url: str) -> Dict[str, Any]:
        """詳細ページから追加情報を取得"""
        detail_data = {}

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(random.randint(1500, 3000))

            # __NEXT_DATA__ からJSON取得を試みる
            next_data = await self._extract_next_data(page)
            if next_data:
                detail_data = self._parse_next_data(next_data)
                if detail_data:
                    logger.info(f"[ジョブメドレー] __NEXT_DATA__から情報取得成功")
                    return detail_data

            # JSON-LDから取得を試みる
            json_ld_data = await self._extract_json_ld(page)
            if json_ld_data:
                detail_data = self._parse_json_ld(json_ld_data)
                if detail_data:
                    logger.info(f"[ジョブメドレー] JSON-LDから情報取得成功")
                    return detail_data

            # HTMLから直接取得
            detail_data = await self._extract_from_html(page)

        except Exception as e:
            logger.error(f"[ジョブメドレー] 詳細取得エラー {url}: {e}")

        return detail_data

    async def _extract_next_data(self, page: Page) -> Optional[Dict]:
        """__NEXT_DATA__スクリプトからデータ取得"""
        try:
            script = await page.query_selector('script#__NEXT_DATA__')
            if script:
                content = await script.inner_text()
                return json.loads(content)
        except Exception as e:
            logger.debug(f"[ジョブメドレー] __NEXT_DATA__取得エラー: {e}")
        return None

    def _parse_next_data(self, data: Dict) -> Dict[str, Any]:
        """__NEXT_DATA__からジョブ情報を抽出"""
        result = {}
        try:
            props = data.get("props", {}).get("pageProps", {})
            job_offer = props.get("jobOffer", {})
            facility = props.get("facility", {})

            if job_offer:
                # タイトル
                result["title"] = job_offer.get("title", "")

                # 会社名/施設名
                result["company_name"] = facility.get("name", "")

                # 住所
                address_parts = []
                if facility.get("prefecture"):
                    address_parts.append(facility.get("prefecture", {}).get("name", ""))
                if facility.get("city"):
                    address_parts.append(facility.get("city", ""))
                if facility.get("address"):
                    address_parts.append(facility.get("address", ""))
                result["address"] = "".join(address_parts)

                # 給与
                salaries = job_offer.get("jobOfferSalaries", [])
                if salaries:
                    salary = salaries[0]
                    salary_type = salary.get("salaryType", {}).get("name", "")
                    salary_bottom = salary.get("salaryBottom", 0)
                    salary_top = salary.get("salaryTop", 0)
                    if salary_bottom and salary_top:
                        result["salary"] = f"{salary_type} {salary_bottom:,}円 〜 {salary_top:,}円"
                    elif salary_bottom:
                        result["salary"] = f"{salary_type} {salary_bottom:,}円〜"

                # 雇用形態
                emp_types = job_offer.get("employmentTypes", [])
                if emp_types:
                    result["employment_type"] = emp_types[0].get("name", "")

                # 仕事内容
                result["job_description"] = job_offer.get("jobContent", "")

                # 事業内容
                result["business_content"] = facility.get("description", "") or job_offer.get("appealBody", "")

                # 電話番号（通常は非表示）
                result["phone_number"] = facility.get("tel", "")

                # 郵便番号
                result["postal_code"] = facility.get("postalCode", "")

            # Google Job Postingデータがあれば追加
            google_posting = props.get("googleJobPosting", {})
            if google_posting and not result.get("title"):
                result["title"] = google_posting.get("title", "")

                location = google_posting.get("jobLocation", {}).get("address", {})
                if location and not result.get("address"):
                    result["address"] = location.get("streetAddress", "")

        except Exception as e:
            logger.debug(f"[ジョブメドレー] __NEXT_DATA__解析エラー: {e}")

        return result

    async def _extract_json_ld(self, page: Page) -> Optional[Dict]:
        """JSON-LDスクリプトからデータ取得"""
        try:
            scripts = await page.query_selector_all('script[type="application/ld+json"]')
            for script in scripts:
                content = await script.inner_text()
                data = json.loads(content)
                if data.get("@type") == "JobPosting":
                    return data
        except Exception as e:
            logger.debug(f"[ジョブメドレー] JSON-LD取得エラー: {e}")
        return None

    def _parse_json_ld(self, data: Dict) -> Dict[str, Any]:
        """JSON-LDからジョブ情報を抽出"""
        result = {}
        try:
            result["title"] = data.get("title", "")

            # 会社名
            hiring_org = data.get("hiringOrganization", {})
            result["company_name"] = hiring_org.get("name", "")

            # 住所
            location = data.get("jobLocation", {}).get("address", {})
            if location:
                addr_parts = [
                    location.get("addressRegion", ""),
                    location.get("addressLocality", ""),
                    location.get("streetAddress", "")
                ]
                result["address"] = "".join(addr_parts)

            # 給与
            salary = data.get("baseSalary", {})
            if salary:
                value = salary.get("value", {})
                if isinstance(value, dict):
                    min_val = value.get("minValue", 0)
                    max_val = value.get("maxValue", 0)
                    unit = salary.get("currency", "JPY")
                    if min_val and max_val:
                        result["salary"] = f"{min_val:,}円 〜 {max_val:,}円"

            # 雇用形態
            emp_type = data.get("employmentType", "")
            if emp_type == "FULL_TIME":
                result["employment_type"] = "正職員"
            elif emp_type == "PART_TIME":
                result["employment_type"] = "パート"
            elif emp_type == "CONTRACTOR":
                result["employment_type"] = "契約社員"
            else:
                result["employment_type"] = emp_type

            # 仕事内容
            result["job_description"] = data.get("description", "")

        except Exception as e:
            logger.debug(f"[ジョブメドレー] JSON-LD解析エラー: {e}")

        return result

    async def _extract_from_html(self, page: Page) -> Dict[str, Any]:
        """HTMLから直接情報を取得"""
        result = {}
        try:
            # タイトル
            title_elem = await page.query_selector('h1')
            if title_elem:
                result["title"] = (await title_elem.inner_text()).strip()

            # 会社名/施設名
            company_elem = await page.query_selector('[data-testid="facility-name"], .facility-name')
            if company_elem:
                result["company_name"] = (await company_elem.inner_text()).strip()

            # 住所
            address_elem = await page.query_selector('[data-testid="address"], .address')
            if address_elem:
                result["address"] = (await address_elem.inner_text()).strip()

            # 給与
            salary_elem = await page.query_selector('[data-testid="salary"], .salary')
            if salary_elem:
                result["salary"] = (await salary_elem.inner_text()).strip()

            # 雇用形態
            emp_elem = await page.query_selector('[data-testid="employment-type"], .employment-type')
            if emp_elem:
                result["employment_type"] = (await emp_elem.inner_text()).strip()

        except Exception as e:
            logger.debug(f"[ジョブメドレー] HTML解析エラー: {e}")

        return result

    async def scrape_with_detail(
        self,
        browser: Browser,
        keyword: str,
        area: str,
        max_pages: int = 3,
        existing_job_ids: set = None
    ) -> List[Dict[str, Any]]:
        """
        検索と詳細取得を一括実行

        Args:
            browser: Playwrightのブラウザインスタンス
            keyword: 検索キーワード
            area: 都道府県名
            max_pages: 最大ページ数
            existing_job_ids: 既存のjob_idセット

        Returns:
            詳細情報付きの求人データリスト
        """
        existing_job_ids = existing_job_ids or set()

        # Stealth設定でコンテキスト作成
        stealth_config = StealthConfig()
        context = await create_stealth_context(browser, stealth_config)
        page = await context.new_page()

        try:
            # 検索実行
            jobs = await self.search_jobs(page, keyword, area, max_pages, existing_job_ids)
            logger.info(f"[ジョブメドレー] 検索完了: {len(jobs)} 件")

            # 詳細情報を取得
            detailed_jobs = []
            for i, job in enumerate(jobs):
                try:
                    logger.info(f"[ジョブメドレー] 詳細取得 {i+1}/{len(jobs)}: {job['url']}")

                    detail = await self.extract_detail_info(page, job["url"])
                    job.update(detail)
                    detailed_jobs.append(job)

                    self._report_count(len(detailed_jobs))

                    # 待機
                    await page.wait_for_timeout(random.randint(1000, 2000))

                except Exception as e:
                    logger.error(f"[ジョブメドレー] 詳細取得エラー: {e}")
                    detailed_jobs.append(job)

            return detailed_jobs

        finally:
            await page.close()
            await context.close()
