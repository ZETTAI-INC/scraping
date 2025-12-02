"""
タウンワーク専用スクレイパー
2024年更新版 - 新しいサイト構造に対応
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


class TownworkScraper(BaseScraper):
    """タウンワーク用スクレイパー"""

    def __init__(self):
        super().__init__(site_name="townwork")
        # 現在検索中のカテゴリパス（PR除外用）
        self._current_category_path: Optional[str] = None
        # 現在検索中のエリア（PR除外用）
        self._current_search_area: Optional[str] = None

    def set_search_category(self, keyword: str) -> None:
        """
        検索キーワードに対応するカテゴリパスを設定
        並列処理前に呼び出すことで、全タスクで同じカテゴリ判定ができる
        """
        job_categories = self.site_config.get("job_categories", {})
        self._current_category_path = job_categories.get(keyword)

    def _get_prefecture_from_area(self, area: str) -> str:
        """
        エリア名から都道府県名を取得
        例: "東京" -> "東京都", "大阪" -> "大阪府"
        """
        # 都道府県の正式名称マッピング
        prefecture_map = {
            "北海道": "北海道",
            "東京": "東京都",
            "大阪": "大阪府",
            "京都": "京都府",
        }
        # 上記以外は「県」を付ける
        if area in prefecture_map:
            return prefecture_map[area]
        return area + "県" if not area.endswith(("都", "府", "県", "道")) else area

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

    async def _get_search_result_cards(self, page: Page, card_selector: str) -> list:
        """
        検索結果のカードのみを取得（おすすめ求人セクションを除外）

        タウンワークのページ構造:
        - おすすめ求人: [class*='recommend'], [class*='Recommend'], [class*='pickup'] などのセクション内
        - 検索結果: [class*='searchResult'], [class*='JobList'], メインコンテンツ領域

        除外ロジック:
        1. おすすめ求人セクション内のカードを特定
        2. 検索結果セクション内のカードのみを返す
        """
        # 全ての求人カードを取得
        all_cards = await page.query_selector_all(card_selector)

        if len(all_cards) == 0:
            return []

        # 検索結果のカードのみをフィルタリング
        search_result_cards = []

        # おすすめ求人セクションを特定するセレクタのパターン
        recommend_section_patterns = [
            "[class*='recommend']",
            "[class*='Recommend']",
            "[class*='pickup']",
            "[class*='Pickup']",
            "[class*='banner']",
            "[class*='Banner']",
            "[class*='pr_']",
            "[class*='PR_']",
            "[class*='sponsored']",
            "[class*='Sponsored']",
            "[class*='ad_']",
            "[class*='Ad_']",
            "[class*='promotion']",
            "[class*='Promotion']",
            "[class*='feature']",
            "[class*='Feature']",
            "[class*='highlight']",
            "[class*='Highlight']",
        ]

        for card in all_cards:
            is_recommend = False

            # カードがおすすめセクション内にあるかチェック
            for pattern in recommend_section_patterns:
                # 親要素のクラスをチェック（祖先を5階層まで遡る）
                ancestor_check = await card.evaluate(f"""
                    (el) => {{
                        let current = el;
                        for (let i = 0; i < 5; i++) {{
                            if (!current.parentElement) break;
                            current = current.parentElement;
                            const className = current.className || '';
                            const pattern = '{pattern.replace("[class*='", "").replace("']", "")}';
                            if (className.toLowerCase().includes(pattern.toLowerCase())) {{
                                return true;
                            }}
                        }}
                        return false;
                    }}
                """)

                if ancestor_check:
                    is_recommend = True
                    break

            # おすすめセクション内でなければ検索結果として追加
            if not is_recommend:
                search_result_cards.append(card)

        logger.info(f"フィルタリング結果: 全{len(all_cards)}件中、検索結果{len(search_result_cards)}件を抽出（おすすめ{len(all_cards) - len(search_result_cards)}件除外）")

        return search_result_cards

    # 全都道府県リスト（エリア判定用）
    ALL_PREFECTURES = [
        "北海道",
        "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
        "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
        "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
        "岐阜県", "静岡県", "愛知県", "三重県",
        "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
        "鳥取県", "島根県", "岡山県", "広島県", "山口県",
        "徳島県", "香川県", "愛媛県", "高知県",
        "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
    ]

    def _extract_prefecture_from_text(self, text: str) -> Optional[str]:
        """
        テキストから都道府県名を抽出
        最初に見つかった都道府県名を返す
        """
        for pref in self.ALL_PREFECTURES:
            if pref in text:
                return pref
            # 「県」「府」「都」なしの短縮形もチェック
            short_name = pref.rstrip("都府県")
            if short_name != "北海道" and short_name in text:
                # 短縮形の場合は完全一致に近い形でチェック（「東京」が「東京都」の一部として出現）
                return pref
        return None

    async def _is_matching_area(self, page: Page) -> bool:
        """
        詳細ページの勤務地が検索エリアと一致するかをチェック

        PR案件は検索エリアと異なる都道府県の求人が表示されることがあるため、
        勤務地に明示的に異なる都道府県が記載されている場合のみ除外する。
        都道府県名が記載されていない場合は許可する（誤除外を防ぐ）。
        """
        if not self._current_search_area:
            return True

        # 検索エリアの都道府県名を取得
        search_prefecture = self._get_prefecture_from_area(self._current_search_area)

        try:
            # 詳細ページから勤務地情報を取得
            # 複数のセレクタを試す
            location_selectors = [
                "[class*='accessText']",
                "[class*='locationText']",
                "[class*='workLocation']",
                "[class*='address']",
            ]

            location_text = ""
            for selector in location_selectors:
                location_elem = await page.query_selector(selector)
                if location_elem:
                    location_text = await location_elem.inner_text()
                    if location_text:
                        break

            # 勤務地が取得できない場合はページ全体から探す
            if not location_text:
                body_text = await page.inner_text("body")
                # 勤務地セクションを探す
                location_match = re.search(r"勤務地[：:\s]*(.+?)(?=\n|給与|時給|月給)", body_text)
                if location_match:
                    location_text = location_match.group(1)

            if not location_text:
                # 勤務地が取得できない場合は許可（誤除外を防ぐ）
                return True

            # 勤務地から都道府県名を抽出
            found_prefecture = self._extract_prefecture_from_text(location_text)

            if not found_prefecture:
                # 都道府県名が記載されていない場合は許可（駅名や市区町村のみの記載）
                logger.debug(f"都道府県名なし、許可: {location_text[:50]}")
                return True

            # 検索エリアの都道府県と一致するかチェック
            if found_prefecture == search_prefecture:
                return True

            # 短縮形での一致もチェック（「東京」で検索して「東京都」が見つかった場合など）
            search_short = self._current_search_area
            found_short = found_prefecture.rstrip("都府県")
            if search_short == found_short or search_short == found_prefecture:
                return True

            # 明示的に異なる都道府県が記載されている場合のみ除外
            logger.info(f"エリア不一致で除外: 検索={search_prefecture}, 勤務地={found_prefecture} ({location_text[:50]})")
            return False

        except Exception as e:
            logger.warning(f"エリア判定エラー: {e}")
            return True  # エラー時は許可

    def generate_search_url(self, keyword: str, area: str, page: int = 1) -> str:
        """
        タウンワーク用の検索URL生成

        職種カテゴリが存在する場合:
          https://townwork.net/prefectures/{area}/job_search/{category_path}?sc=new&page={page}
        キーワード検索の場合:
          https://townwork.net/prefectures/{area}/job_search/?keyword={keyword}&page={page}&sort=1
        """
        # エリア名をローマ字に変換
        area_codes = self.site_config.get("area_codes", {})
        area_code = area_codes.get(area, area.lower())

        # 職種カテゴリが存在するかチェック
        job_categories = self.site_config.get("job_categories", {})
        category_path = job_categories.get(keyword)

        # 現在検索中のカテゴリパスとエリアを保存（PR除外用）
        self._current_category_path = category_path
        self._current_search_area = area

        if category_path:
            # 職種カテゴリ形式のURL
            url_pattern = self.site_config.get("search_url_pattern")
            base_url = url_pattern.format(area=area_code, category_path=category_path, page=page)
            logger.info(f"Using category URL: {base_url}")
        else:
            # キーワード検索形式のURL（フォールバック）
            url_pattern = self.site_config.get("search_url_pattern_keyword")
            if not url_pattern:
                # 後方互換性のため
                url_pattern = "https://townwork.net/prefectures/{area}/job_search/?keyword={keyword}&page={page}"
            base_url = url_pattern.format(area=area_code, keyword=keyword, page=page)

            # キーワード検索の場合は新着順パラメータを付与
            from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
            parsed = urlparse(base_url)
            query = dict(parse_qsl(parsed.query))
            query["sort"] = "1"
            new_query = urlencode(query)
            base_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
            logger.info(f"Using keyword URL: {base_url}")

        return base_url

    async def search_jobs(self, page: Page, keyword: str, area: str, max_pages: int = 5) -> List[Dict[str, Any]]:
        """
        求人検索を実行し、結果を返す
        """
        all_jobs = []

        for page_num in range(1, max_pages + 1):
            url = self.generate_search_url(keyword, area, page_num)
            logger.info(f"Fetching page {page_num}: {url}")

            success = False
            # ページ取得・抽出をリトライ（最大2回）し、取りこぼしを減らす
            for attempt in range(2):
                try:
                    response = await page.goto(
                        url,
                        wait_until="networkidle",
                        timeout=30000 if attempt == 0 else 40000  # 2回目は少し長めに待つ
                    )

                    if response and response.status == 404:
                        logger.warning(f"Page not found: {url}")
                        break

                    card_selector = self.selectors.get("job_cards", "[class*='jobCard']")

                    # カードが描画されるまで数回リトライし、描画遅延による取りこぼしを減らす
                    selector_ready = False
                    for sel_attempt in range(4):
                        try:
                            await page.wait_for_selector(card_selector, timeout=2000 + 500 * sel_attempt)
                            selector_ready = True
                            break
                        except PlaywrightTimeoutError:
                            logger.warning(
                                f"Job cards selector timeout on page {page_num} (attempt {sel_attempt + 1}/4). Retrying after short wait."
                            )
                            await page.wait_for_timeout(600 + 200 * sel_attempt)

                    if not selector_ready:
                        logger.warning(
                            f"Job cards selector not ready on page {page_num}; attempt {attempt + 1}/2. Retrying page if attempts remain."
                        )
                        if attempt == 0:
                            continue  # もう一度このページをやり直す

                    # 求人カードを取得（おすすめ求人セクションを除外）
                    job_cards = await self._get_search_result_cards(page, card_selector)

                    # 0件の場合は短い待機のあと再取得（描画遅延対策）
                    if len(job_cards) == 0:
                        await page.wait_for_timeout(1000)
                        job_cards = await self._get_search_result_cards(page, card_selector)

                    # それでも0件なら別のリトライ機会があればやり直す
                    if len(job_cards) == 0:
                        logger.warning(f"No job cards found on page {page_num} (attempt {attempt + 1}/2).")
                        if attempt == 0:
                            await page.wait_for_timeout(1200)
                            continue
                        else:
                            logger.info(f"No jobs on page {page_num} after retries; stopping.")
                            success = True  # これ以上ないので終了扱い
                            break

                    logger.info(f"Found {len(job_cards)} jobs on page {page_num} (attempt {attempt + 1})")

                    for card in job_cards:
                        try:
                            job_data = await self._extract_card_data(card)
                            if job_data:
                                all_jobs.append(job_data)
                        except Exception as e:
                            logger.error(f"Error extracting job card: {e}")
                            continue

                    success = True
                    break  # ページ処理成功

                except Exception as e:
                    logger.error(f"Error fetching page {page_num} (attempt {attempt + 1}/2): {e}")
                    if attempt == 0:
                        await page.wait_for_timeout(1500)
                        continue
                    else:
                        break

            if not success:
                break

            # 次のページがあるか確認（見えなくても最大ページ数までは試行し、取りこぼしを減らす）
            next_page = await page.query_selector(f"[class*='pageButton']:has-text('{page_num + 1}')")
            if not next_page and page_num < max_pages:
                logger.warning(
                    f"Next page button not found for page {page_num}, but continuing to page {page_num + 1} to avoid missing results."
                )
                continue
            if not next_page:
                logger.info("No more pages available")
                break

        return all_jobs

    async def _extract_card_data(self, card) -> Optional[Dict[str, Any]]:
        """
        求人カードからデータを抽出
        """
        try:
            data = {}

            # 詳細ページへのリンク（カード自体 or 内部のanchor）
            href = await card.get_attribute("href")
            if not href:
                link_elem = await card.query_selector("a[href*='jobid_'], a[href^='/jobid_'], a[href*='job/'], a[href]")
                if link_elem:
                    href = await link_elem.get_attribute("href")
            if href:
                if href.startswith("/"):
                    href = f"https://townwork.net{href}"
                # クエリやフラグメントで差分が出ないよう正規化
                href = self._normalize_url(href)
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

            # 勤務地が空の求人はおすすめ求人の可能性が高いため除外
            if not data.get("location"):
                logger.debug(f"Skipping job with empty location: {data.get('page_url', 'N/A')}")
                return None


            return data if data.get("page_url") else None

        except Exception as e:
            logger.error(f"Error extracting card data: {e}")
            return None

    def _normalize_url(self, url: str) -> str:
        """クエリ・フラグメントを除去して末尾スラッシュを揃える"""
        if not url:
            return ""
        from urllib.parse import urlparse, urlunparse

        parsed = urlparse(url)
        path = parsed.path or "/"
        path = path.rstrip("/") or "/"
        return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))

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

        カテゴリ不一致の場合は{"_skip": True}を返す
        """
        detail_data = {}

        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(2000)

            # エリアマッチングチェック（PR案件除外）
            # 検索エリアと勤務地の都道府県が一致しない場合はPR案件として除外
            if not await self._is_matching_area(page):
                return {"_skip": True}

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

            # 掲載日（掲載開始日、掲載期間などから抽出）
            # パターン1: 掲載期間 2024/01/01〜2024/01/31
            published_match = re.search(r"掲載期間[：:\s]*(\d{4}[/\-]\d{1,2}[/\-]\d{1,2})", body_text)
            if published_match:
                detail_data["published_date"] = published_match.group(1)
            else:
                # パターン2: 掲載開始日
                published_match2 = re.search(r"掲載開始[日]?[：:\s]*(\d{4}[/\-]\d{1,2}[/\-]\d{1,2})", body_text)
                if published_match2:
                    detail_data["published_date"] = published_match2.group(1)
                else:
                    # パターン3: 更新日・登録日
                    published_match3 = re.search(r"(更新日|登録日)[：:\s]*(\d{4}[/\-]\d{1,2}[/\-]\d{1,2})", body_text)
                    if published_match3:
                        detail_data["published_date"] = published_match3.group(2)

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
        filtered_jobs = []
        skipped_count = 0

        for i, job in enumerate(jobs):
            if job.get("page_url"):
                logger.info(f"Fetching detail {i+1}/{len(jobs)}: {job['page_url']}")
                try:
                    detail_data = await self.extract_detail_info(page, job["page_url"])

                    # PR案件（カテゴリ不一致）の場合はスキップ
                    if detail_data.get("_skip"):
                        skipped_count += 1
                        continue

                    job.update(detail_data)
                    filtered_jobs.append(job)
                    await page.wait_for_timeout(1000)  # サーバーに負荷をかけないよう待機
                except Exception as e:
                    logger.error(f"Error fetching detail for job {i+1}: {e}")
                    filtered_jobs.append(job)  # エラー時は除外しない
            else:
                filtered_jobs.append(job)

        if skipped_count > 0:
            logger.info(f"PR案件除外: {skipped_count}件をカテゴリ不一致として除外")

        return filtered_jobs

    async def scrape_single_page(
        self,
        browser: Browser,
        keyword: str,
        area: str,
        page_num: int,
        task_idx: int = 0
    ) -> List[Dict[str, Any]]:
        """
        タウンワーク用: 1ページを並列用にスクレイピング
        base_scraperのメソッドをオーバーライド
        """
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

            # タウンワーク専用の1ページ取得処理
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
        タウンワーク: 1ページ分の求人を取得する実装
        """
        jobs = []
        url = self.generate_search_url(keyword, area, page_num)
        logger.info(f"Fetching page {page_num}: {url}")

        # ページ取得・抽出をリトライ（最大2回）
        for attempt in range(2):
            try:
                response = await page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=60000
                )
                # DOMロード後に追加で待機（JSレンダリング用）
                await page.wait_for_timeout(2000)

                if response and response.status == 404:
                    logger.warning(f"Page not found: {url}")
                    return jobs

                if response and response.status == 403:
                    logger.error(f"Access blocked (403): {url}")
                    return jobs

                card_selector = self.selectors.get("job_cards", "[class*='jobCard']")

                # カードが描画されるまで数回リトライ
                selector_ready = False
                for sel_attempt in range(4):
                    try:
                        await page.wait_for_selector(card_selector, timeout=2000 + 500 * sel_attempt)
                        selector_ready = True
                        break
                    except PlaywrightTimeoutError:
                        logger.warning(
                            f"Job cards selector timeout on page {page_num} (attempt {sel_attempt + 1}/4). Retrying after short wait."
                        )
                        await page.wait_for_timeout(600 + 200 * sel_attempt)

                if not selector_ready:
                    logger.warning(
                        f"Job cards selector not ready on page {page_num}; attempt {attempt + 1}/2."
                    )
                    if attempt == 0:
                        continue

                # 求人カードを取得（おすすめ求人セクションを除外）
                job_cards = await self._get_search_result_cards(page, card_selector)

                # 0件の場合は短い待機のあと再取得
                if len(job_cards) == 0:
                    await page.wait_for_timeout(1000)
                    job_cards = await self._get_search_result_cards(page, card_selector)

                if len(job_cards) == 0:
                    logger.warning(f"No job cards found on page {page_num} (attempt {attempt + 1}/2).")
                    if attempt == 0:
                        await page.wait_for_timeout(1200)
                        continue
                    else:
                        return jobs

                logger.info(f"Found {len(job_cards)} jobs on page {page_num}")

                for card in job_cards:
                    try:
                        job_data = await self._extract_card_data(card)
                        if job_data:
                            jobs.append(job_data)
                    except Exception as e:
                        logger.error(f"Error extracting job card: {e}")
                        continue

                return jobs

            except Exception as e:
                logger.error(f"Error fetching page {page_num} (attempt {attempt + 1}/2): {e}")
                if attempt == 0:
                    await page.wait_for_timeout(1500)
                    continue
                else:
                    return jobs

        return jobs
