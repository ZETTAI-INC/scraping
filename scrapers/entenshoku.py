"""
エン転職専用スクレイパー
2024年12月更新 - 実際のサイト構造に対応
"""
import re
from typing import Dict, Any, List, Optional
from playwright.async_api import Page, Browser
from .base_scraper import BaseScraper
import logging

logger = logging.getLogger(__name__)


class EntenshokuScraper(BaseScraper):
    """エン転職用スクレイパー"""

    def __init__(self):
        super().__init__(site_name="entenshoku")
        self._current_search_area: Optional[str] = None

    def generate_search_url(self, keyword: str, area: str, page: int = 1) -> str:
        """
        エン転職用の検索URL生成

        URL形式:
        https://employment.en-japan.com/search/search_list/?areaid={areaid}&occupation={occupation}&refine=1&pagenum={page}

        例:
          東京+営業 → https://employment.en-japan.com/search/search_list/?areaid=23&occupation=100000&refine=1
        """
        # 現在検索中のエリアを保存
        self._current_search_area = area

        # エリアコードを取得
        area_codes = self.site_config.get("area_codes", {})
        areaid = area_codes.get(area, 23)  # デフォルトは東京

        # 職種コードを取得
        job_categories = self.site_config.get("job_categories", {})
        occupation = job_categories.get(keyword)

        if occupation:
            # 職種コードがある場合
            url = f"https://employment.en-japan.com/search/search_list/?areaid={areaid}&occupation={occupation}&refine=1&pagenum={page}"
        else:
            # 職種コードがない場合はエリアのみで検索
            url = f"https://employment.en-japan.com/search/search_list/?areaid={areaid}&refine=1&pagenum={page}"

        logger.info(f"[エン転職] 検索URL生成: {url}")
        return url

    async def extract_job_card(self, card_element, page: Page) -> Dict[str, Any]:
        """
        エン転職用の求人カード情報抽出
        """
        job_data = {
            "site": "エン転職",
            "title": "",
            "company_name": "",
            "location": "",
            "salary": "",
            "employment_type": "",
            "page_url": "",
        }

        try:
            # 詳細ページへのリンク
            href = await card_element.get_attribute("href")
            if href:
                if href.startswith("/"):
                    href = f"https://employment.en-japan.com{href}"
                job_data["page_url"] = href

                # 求人番号を抽出（例: /desc_1393025/ → 1393025）
                match = re.search(r"/desc_(\d+)", href)
                if match:
                    job_data["job_number"] = match.group(1)

            # カード内のテキストを取得
            card_text = await card_element.inner_text()
            lines = [line.strip() for line in card_text.split('\n') if line.strip()]

            # タイトル（通常は最初の方にある）- 会社名と職種を分離
            raw_title = ""
            title_elem = await card_element.query_selector("h2, h3, [class*='title'], [class*='Title']")
            if title_elem:
                raw_title = (await title_elem.inner_text()).strip()
            elif lines:
                # タイトル候補を探す（長い文字列で職種っぽいもの）
                for line in lines[:5]:
                    if len(line) > 5 and not any(x in line for x in ['NEW', '積極採用', 'プロ取材', '件']):
                        raw_title = line
                        break

            # 「会社名／職種タイトル」形式を分離
            if raw_title and "／" in raw_title:
                parts = raw_title.split("／", 1)
                if any(x in parts[0] for x in ['株式会社', '有限会社', '合同会社', '社団法人', '財団法人', '医療法人']):
                    job_data["company_name"] = parts[0].strip()
                    job_data["title"] = parts[1].strip() if len(parts) > 1 else raw_title
                else:
                    job_data["title"] = raw_title
            elif raw_title:
                job_data["title"] = raw_title

            # 会社名がまだ取得できていない場合
            if not job_data["company_name"]:
                company_elem = await card_element.query_selector("[class*='company'], [class*='Company']")
                if company_elem:
                    job_data["company_name"] = (await company_elem.inner_text()).strip()

            # 給与（月給、年収などを含む行を探す）
            for line in lines:
                if any(x in line for x in ['月給', '年収', '万円', '時給']):
                    job_data["salary"] = line
                    break

            # 勤務地
            location_elem = await card_element.query_selector("[class*='location'], [class*='area']")
            if location_elem:
                job_data["location"] = (await location_elem.inner_text()).strip()

        except Exception as e:
            logger.error(f"Error extracting job card: {e}")

        return job_data

    async def _extract_card_data(self, card) -> Optional[Dict[str, Any]]:
        """
        求人カードからデータを抽出（search_jobs用）
        エン転職は複雑なカード構造のため、基本情報のみ抽出し詳細は別途取得
        派遣社員・紹介予定派遣はスキップ
        """
        try:
            data = {}

            # 詳細ページへのリンク
            href = await card.get_attribute("href")
            if href:
                # クエリパラメータを除去してクリーンなURLを生成
                base_href = href.split('?')[0]
                if base_href.startswith("/"):
                    base_href = f"https://employment.en-japan.com{base_href}"
                data["page_url"] = base_href

                # 求人番号を抽出
                match = re.search(r"/desc_(\d+)", href)
                if match:
                    data["job_number"] = match.group(1)

            # カード内のテキストを取得
            card_text = await card.inner_text()
            lines = [line.strip() for line in card_text.split('\n') if line.strip()]

            # 派遣社員・紹介予定派遣をスキップ
            dispatch_keywords = ['派遣社員', '紹介予定派遣', '無期雇用派遣']
            card_text_joined = ' '.join(lines)
            for keyword in dispatch_keywords:
                if keyword in card_text_joined:
                    logger.debug(f"[エン転職] 派遣求人をスキップ: {data.get('job_number', 'unknown')} ({keyword})")
                    return None

            # PR記事（広告枠）をスキップ
            # 「30名以上」「100名」「あと3日」などで始まるものはPR記事
            if lines:
                first_line = lines[0]
                # 「XX名」「XX名以上」で始まるパターン
                if re.match(r'^\d+名', first_line):
                    logger.debug(f"[エン転職] PR記事をスキップ（人数表示）: {data.get('job_number', 'unknown')}")
                    return None
                # 「あとX日」で始まるパターン
                if re.match(r'^あと\d+日', first_line):
                    logger.debug(f"[エン転職] PR記事をスキップ（残り日数）: {data.get('job_number', 'unknown')}")
                    return None

            # 雇用形態を抽出
            employment_types = ['正社員', '契約社員', 'アルバイト', 'パート', '業務委託']
            for line in lines:
                for emp_type in employment_types:
                    if emp_type in line:
                        data["employment_type"] = emp_type
                        break
                if data.get("employment_type"):
                    break

            # タイトル（最初の意味のある長い文字列）
            for line in lines[:5]:
                if len(line) > 8 and not any(x in line for x in ['NEW', '積極採用', 'プロ取材', '件', '応募', '正社員', '職種未経験', '業種未経験']):
                    raw_title = line
                    # 「会社名／職種タイトル」形式を分離
                    if "／" in raw_title:
                        parts = raw_title.split("／", 1)
                        # 最初の部分が会社名（株式会社、有限会社などを含む場合）
                        if any(x in parts[0] for x in ['株式会社', '有限会社', '合同会社', '社団法人', '財団法人', '医療法人']):
                            data["company_name"] = parts[0].strip()
                            data["title"] = parts[1].strip() if len(parts) > 1 else raw_title
                        else:
                            data["title"] = raw_title
                    else:
                        data["title"] = raw_title
                    break

            # タイトルが取れなければリンクテキスト全体を使用
            if not data.get("title") and lines:
                # 全テキストから意味のある部分を抽出
                full_text = " ".join(lines[:3])
                if len(full_text) > 10:
                    raw_title = full_text[:100]
                    # 「会社名／職種タイトル」形式を分離
                    if "／" in raw_title:
                        parts = raw_title.split("／", 1)
                        if any(x in parts[0] for x in ['株式会社', '有限会社', '合同会社', '社団法人', '財団法人', '医療法人']):
                            data["company_name"] = parts[0].strip()
                            data["title"] = parts[1].strip() if len(parts) > 1 else raw_title
                        else:
                            data["title"] = raw_title
                    else:
                        data["title"] = raw_title

            # 給与
            for line in lines:
                if any(x in line for x in ['月給', '年収', '万円', '時給']):
                    data["salary"] = line
                    break

            return data if data.get("page_url") and data.get("job_number") else None

        except Exception as e:
            logger.error(f"Error extracting card data: {e}")
            return None

    async def search_jobs(self, page: Page, keyword: str, area: str, max_pages: int = 5) -> List[Dict[str, Any]]:
        """
        求人検索を実行し、結果を返す
        """
        all_jobs = []
        seen_job_numbers = set()  # 重複排除用

        for page_num in range(1, max_pages + 1):
            url = self.generate_search_url(keyword, area, page_num)
            logger.info(f"[エン転職] ページ{page_num}取得: {url}")

            try:
                response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(3000)

                if response and response.status >= 400:
                    logger.warning(f"[エン転職] HTTPエラー {response.status}: {url}")
                    break

                # 検索結果の件数を確認（0件の場合はおすすめ求人が表示されるため）
                page_text = await page.inner_text("body")

                # 「全X件を表示」または「- - -件」のパターンを確認
                result_count_match = re.search(r"全[_\s]*(\d+)[_\s]*件", page_text)
                no_result_match = re.search(r"(- - -|---)\s*件|求人情報がありませんでした", page_text)

                if no_result_match and not result_count_match:
                    logger.info(f"[エン転職] 検索結果0件のため終了")
                    break

                expected_count = None
                if result_count_match:
                    expected_count = int(result_count_match.group(1))
                    logger.info(f"[エン転職] 検索結果: 全{expected_count}件")
                    if expected_count == 0:
                        logger.info(f"[エン転職] 検索結果0件のため終了")
                        break

                # 求人カードを取得
                # /desc_XXXXXX/ または /desc_XXXXXX/?... 形式のリンクを取得
                card_selector = "a[href*='/desc_']"
                all_links = await page.query_selector_all(card_selector)

                # 求人番号でグループ化し、重複リンクを除外
                job_cards_dict = {}
                for link in all_links:
                    href = await link.get_attribute("href")
                    if href:
                        match = re.search(r"/desc_(\d+)", href)
                        if match:
                            job_num = match.group(1)
                            # まだ登録されていない求人番号のみ追加
                            if job_num not in job_cards_dict:
                                job_cards_dict[job_num] = link

                job_cards = list(job_cards_dict.values())

                if len(job_cards) == 0:
                    logger.info(f"[エン転職] ページ{page_num}で求人なし、終了")
                    break

                logger.info(f"[エン転職] ページ{page_num}で{len(job_cards)}件のリンクを発見")

                page_jobs = 0
                for card in job_cards:
                    try:
                        job_data = await self._extract_card_data(card)
                        if job_data and job_data.get("job_number"):
                            # 重複チェック
                            job_num = job_data["job_number"]
                            if job_num not in seen_job_numbers:
                                seen_job_numbers.add(job_num)
                                job_data["site"] = "エン転職"
                                all_jobs.append(job_data)
                                page_jobs += 1
                    except Exception as e:
                        logger.error(f"Error extracting job card: {e}")
                        continue

                logger.info(f"[エン転職] ページ{page_num}で{page_jobs}件の新規求人を追加（累計: {len(all_jobs)}件）")

                # リアルタイム件数報告
                self._report_count(len(all_jobs))

                # 次ページへの待機
                await page.wait_for_timeout(1500)

            except Exception as e:
                logger.error(f"Error fetching page {page_num}: {e}")
                break

        return all_jobs

    async def extract_detail_info(self, page: Page, url: str) -> Dict[str, Any]:
        """詳細ページから追加情報を取得"""
        detail_data = {}

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

            # ページ全体のテキストを取得
            body_text = await page.inner_text("body")

            # JSON-LDスキーマから会社名を取得（最も確実）
            import json
            try:
                json_ld_scripts = await page.query_selector_all('script[type="application/ld+json"]')
                logger.debug(f"[エン転職] JSON-LDスクリプト数: {len(json_ld_scripts)}")
                for script in json_ld_scripts:
                    script_content = await script.inner_text()
                    try:
                        ld_data = json.loads(script_content)
                        # 配列の場合は最初の要素を使用
                        if isinstance(ld_data, list) and len(ld_data) > 0:
                            ld_data = ld_data[0]
                        # hiringOrganization から会社名を取得
                        if isinstance(ld_data, dict):
                            if "hiringOrganization" in ld_data:
                                org = ld_data["hiringOrganization"]
                                if isinstance(org, dict) and "name" in org:
                                    detail_data["company_name"] = org["name"]
                                    logger.debug(f"[エン転職] JSON-LDから会社名取得: {org['name']}")
                                    break
                            elif "name" in ld_data and ld_data.get("@type") == "Organization":
                                detail_data["company_name"] = ld_data["name"]
                                logger.debug(f"[エン転職] JSON-LD Organizationから会社名取得: {ld_data['name']}")
                                break
                    except json.JSONDecodeError as e:
                        logger.debug(f"[エン転職] JSON-LDパースエラー: {e}")
            except Exception as e:
                logger.debug(f"[エン転職] JSON-LD取得エラー: {e}")

            # JSON-LDで取得できなかった場合、ページ上部の会社名要素から取得
            if not detail_data.get("company_name"):
                # エン転職の会社名は通常ページ上部にある
                company_selectors = [
                    "p.companyName",
                    ".companyName",
                    "[class*='company-name']",
                    "[class*='companyName']",
                    ".company a",
                    "h2 a"
                ]
                for selector in company_selectors:
                    try:
                        company_elem = await page.query_selector(selector)
                        if company_elem:
                            company_text = (await company_elem.inner_text()).strip()
                            if company_text and len(company_text) > 2:
                                detail_data["company_name"] = company_text
                                break
                    except Exception:
                        pass

            # タイトル（h1タグ）- 会社名と職種を分離
            title_elem = await page.query_selector("h1")
            if title_elem:
                raw_title = (await title_elem.inner_text()).strip()
                # 「会社名／職種タイトル」形式を分離
                if "／" in raw_title:
                    parts = raw_title.split("／", 1)
                    # 最初の部分が会社名（株式会社、有限会社などを含む場合）
                    if any(x in parts[0] for x in ['株式会社', '有限会社', '合同会社', '社団法人', '財団法人', '医療法人']):
                        if not detail_data.get("company_name"):
                            detail_data["company_name"] = parts[0].strip()
                        detail_data["title"] = parts[1].strip() if len(parts) > 1 else raw_title
                    else:
                        detail_data["title"] = raw_title
                else:
                    detail_data["title"] = raw_title

            # 会社名がまだ取得できていない場合、h2タグから取得
            if not detail_data.get("company_name"):
                company_elem = await page.query_selector("h2")
                if company_elem:
                    company_text = (await company_elem.inner_text()).strip()
                    if company_text and any(x in company_text for x in ['株式会社', '有限会社', '合同会社', '社団法人', '財団法人', '医療法人']):
                        detail_data["company_name"] = company_text

            # 会社名がまだ取得できていない場合、ページタイトルから取得
            # 「株式会社○○の転職・求人情報｜エン転職｜...」形式
            if not detail_data.get("company_name"):
                try:
                    page_title = await page.title()
                    if page_title and "の転職・求人情報" in page_title:
                        # 「会社名の転職・求人情報」から会社名を抽出
                        company_from_title = page_title.split("の転職・求人情報")[0].strip()
                        if company_from_title and len(company_from_title) > 2:
                            detail_data["company_name"] = company_from_title
                            logger.debug(f"ページタイトルから会社名を取得: {company_from_title}")
                except Exception as e:
                    logger.debug(f"ページタイトルからの会社名取得失敗: {e}")

            # 給与の抽出
            salary_match = re.search(r"(月給|年収|時給)[：:\s]*([0-9,万円～\-\s]+)", body_text)
            if salary_match:
                detail_data["salary"] = salary_match.group(0).strip()

            # 勤務地の抽出（「勤務地・交通」セクションから）
            # 「勤務地・交通」から次のセクション（交通、配属部署、等）までを抽出
            location_match = re.search(r"勤務地・交通\s*\n(.+?)(?=\n交通\n|\n配属部署|\n募集要項|\n会社概要|\n■[^\n]*\n[^\n]*都|\Z)", body_text, re.DOTALL)
            if location_match:
                location_text = location_match.group(1).strip()
                # 最初の住所情報を取得
                lines = [l.strip() for l in location_text.split('\n') if l.strip()]
                location_result = None

                # 都道府県を含む具体的な住所行を探す
                prefectures = ['北海道', '東京都', '大阪府', '京都府',
                              '青森県', '岩手県', '宮城県', '秋田県', '山形県', '福島県',
                              '茨城県', '栃木県', '群馬県', '埼玉県', '千葉県', '神奈川県',
                              '新潟県', '富山県', '石川県', '福井県', '山梨県', '長野県',
                              '岐阜県', '静岡県', '愛知県', '三重県', '滋賀県', '兵庫県',
                              '奈良県', '和歌山県', '鳥取県', '島根県', '岡山県', '広島県',
                              '山口県', '徳島県', '香川県', '愛媛県', '高知県', '福岡県',
                              '佐賀県', '長崎県', '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県']

                for line in lines[:10]:  # 最初の10行まで確認
                    # ■マーク付きの店舗名は住所として使わない
                    if line.startswith('■'):
                        continue
                    # 都道府県で始まる住所行
                    for pref in prefectures:
                        if line.startswith(pref):
                            location_result = line
                            break
                    if location_result:
                        break

                # 具体的住所が見つからない場合は概要行を取得
                if not location_result and lines:
                    for line in lines[:3]:
                        if not line.startswith('※') and len(line) > 3:
                            location_result = line
                            break

                if location_result:
                    detail_data["location"] = location_result[:200]

            # 雇用形態
            employment_match = re.search(r"雇用形態[：:\s]*(.+?)(?=\n|試用期間)", body_text)
            if employment_match:
                detail_data["employment_type"] = employment_match.group(1).strip()

            # 仕事内容
            job_desc_match = re.search(r"仕事内容[：:\s]*(.+?)(?=\n応募資格|\n募集要項)", body_text, re.DOTALL)
            if job_desc_match:
                detail_data["job_description"] = job_desc_match.group(1).strip()[:500]

            # 応募資格
            qualification_match = re.search(r"応募資格[：:\s]*(.+?)(?=\n募集|\n給与|\n勤務)", body_text, re.DOTALL)
            if qualification_match:
                detail_data["qualifications"] = qualification_match.group(1).strip()[:300]

            # 休日・休暇
            holiday_match = re.search(r"(休日|休暇)[：:\s]*(.+?)(?=\n福利|$)", body_text)
            if holiday_match:
                detail_data["holidays"] = holiday_match.group(2).strip()

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
                logger.info(f"[エン転職] 詳細取得 {i+1}/{len(jobs)}: {job['page_url']}")
                try:
                    detail_data = await self.extract_detail_info(page, job["page_url"])
                    job.update(detail_data)
                    await page.wait_for_timeout(1000)
                except Exception as e:
                    logger.error(f"Error fetching detail for job {i+1}: {e}")

        return jobs
