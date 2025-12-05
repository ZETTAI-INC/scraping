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
        # リアルタイム件数コールバック
        self._realtime_callback = None

    def set_realtime_callback(self, callback):
        """リアルタイム件数コールバックを設定"""
        self._realtime_callback = callback

    def _report_count(self, count: int):
        """件数を報告"""
        if self._realtime_callback:
            self._realtime_callback(count)

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

    # エリア名の日本語表記マッピング
    AREA_NAMES = {
        "北海道": "北海道",
        "青森": "青森県", "岩手": "岩手県", "宮城": "宮城県", "秋田": "秋田県", "山形": "山形県", "福島": "福島県",
        "茨城": "茨城県", "栃木": "栃木県", "群馬": "群馬県", "埼玉": "埼玉県", "千葉": "千葉県", "東京": "東京都", "神奈川": "神奈川県",
        "新潟": "新潟県", "富山": "富山県", "石川": "石川県", "福井": "福井県", "山梨": "山梨県", "長野": "長野県",
        "岐阜": "岐阜県", "静岡": "静岡県", "愛知": "愛知県", "三重": "三重県",
        "滋賀": "滋賀県", "京都": "京都府", "大阪": "大阪府", "兵庫": "兵庫県", "奈良": "奈良県", "和歌山": "和歌山県",
        "鳥取": "鳥取県", "島根": "島根県", "岡山": "岡山県", "広島": "広島県", "山口": "山口県",
        "徳島": "徳島県", "香川": "香川県", "愛媛": "愛媛県", "高知": "高知県",
        "福岡": "福岡県", "佐賀": "佐賀県", "長崎": "長崎県", "熊本": "熊本県", "大分": "大分県", "宮崎": "宮崎県", "鹿児島": "鹿児島県", "沖縄": "沖縄県",
    }

    # 都道府県のローマ字マッピング（カテゴリ検索URL用）
    PREF_ROMAN = {
        "北海道": "hokkaidou",
        "青森": "aomori", "岩手": "iwate", "宮城": "miyagi", "秋田": "akita", "山形": "yamagata", "福島": "fukushima",
        "茨城": "ibaraki", "栃木": "tochigi", "群馬": "gunma", "埼玉": "saitama", "千葉": "chiba", "東京": "tokyo", "神奈川": "kanagawa",
        "新潟": "niigata", "富山": "toyama", "石川": "ishikawa", "福井": "fukui", "山梨": "yamanashi", "長野": "nagano",
        "岐阜": "gifu", "静岡": "shizuoka", "愛知": "aichi", "三重": "mie",
        "滋賀": "shiga", "京都": "kyouto", "大阪": "oosaka", "兵庫": "hyougo", "奈良": "nara", "和歌山": "wakayama",
        "鳥取": "tottori", "島根": "shimane", "岡山": "okayama", "広島": "hiroshima", "山口": "yamaguchi",
        "徳島": "tokushima", "香川": "kagawa", "愛媛": "ehime", "高知": "kouchi",
        "福岡": "fukuoka", "佐賀": "saga", "長崎": "nagasaki", "熊本": "kumamoto", "大分": "ooita", "宮崎": "miyazaki", "鹿児島": "kagoshima", "沖縄": "okinawa",
    }

    # 職種カテゴリマッピング（キーワード → カテゴリコード）
    # 形式: "キーワード": ("大カテゴリ", "小カテゴリ" or None)
    JOB_CATEGORIES = {
        # IT・Web・ゲームエンジニア (oc-013)
        "SE": ("oc-013", "omc-0102"),
        "システムエンジニア": ("oc-013", "omc-0102"),
        "エンジニア": ("oc-013", None),
        "IT": ("oc-013", None),
        "プログラマー": ("oc-013", "omc-0103"),
        "プログラマ": ("oc-013", "omc-0103"),
        "Web": ("oc-013", None),
        "ゲーム": ("oc-013", None),
        # 飲食・フードサービス (oc-001)
        "飲食": ("oc-001", None),
        "調理": ("oc-001", None),
        "ホール": ("oc-001", None),
        "キッチン": ("oc-001", None),
        # 営業・販売 (oc-002)
        # 営業は複数のomcをクエリパラメータで指定
        "営業": ("query", ["0011", "0012", "0013"]),
        "販売": ("oc-002", None),
        "接客": ("oc-002", None),
        "店長": ("oc-002", None),
        "コンビニ": ("oc-002", "omc-0014"),
        # 旅行・レジャー・イベント (oc-003)
        "イベント": ("oc-003", None),
        "レジャー": ("oc-003", None),
        "旅行": ("oc-003", None),
        # 倉庫・物流管理 (oc-004)
        "物流": ("oc-004", None),
        "倉庫": ("oc-004", None),
        "軽作業": ("oc-004", None),
        # 警備・保安 (oc-005)
        "警備": ("oc-005", None),
        # 経営・事業企画・人事・事務 (oc-006)
        "事務": ("oc-006", None),
        "経理": ("oc-006", None),
        "総務": ("oc-006", None),
        "人事": ("oc-006", None),
        "秘書": ("oc-006", None),
        "受付": ("oc-006", None),
        "データ入力": ("oc-006", "omc-0040"),
        # マーケティング・広告・宣伝 (oc-007)
        "マーケティング": ("oc-007", None),
        "企画": ("oc-007", None),
        # 保育士・教員・講師 (oc-008)
        "保育": ("oc-008", None),
        "教育": ("oc-008", None),
        "講師": ("oc-008", None),
        # ドライバー・引越し・配送 (oc-009)
        "ドライバー": ("oc-009", None),
        "配送": ("oc-009", None),
        # 介護・福祉 (oc-010)
        "介護": ("oc-010", None),
        # 医療・看護師・薬剤師 (oc-011)
        "医療": ("oc-011", None),
        "看護": ("oc-011", None),
        "薬剤師": ("oc-011", None),
        # メディア・クリエイター (oc-012)
        "デザイン": ("oc-012", None),
        # 清掃・美化 (oc-016)
        "清掃": ("oc-016", None),
        # 美容師 (oc-017)
        "美容": ("oc-017", None),
        # 建設・土木・施工 (oc-018)
        "建設": ("oc-018", None),
        "土木": ("oc-018", None),
        # 製造・工場 (oc-019)
        "製造": ("oc-019", None),
        "工場": ("oc-019", None),
        # コールセンター (事務系として分類)
        "コールセンター": ("oc-006", None),
    }

    def generate_search_url(self, keyword: str, area: str, page: int = 1) -> str:
        """
        タウンワーク用の検索URL生成

        カテゴリ対応版:
          キーワードがJOB_CATEGORIESにある場合:
            https://townwork.net/prefectures/{都道府県ローマ字}/job_search/oc-{大カテゴリ}/omc-{小カテゴリ}/?sc=new
          それ以外（フリーキーワード検索）:
            https://townwork.net/job_search/kw/{エリア}+{キーワード}/?sc=new

        ※ sc=new は新着順ソート用パラメータ

        例:
          石川+SE → https://townwork.net/prefectures/ishikawa/job_search/oc-013/omc-0102/?sc=new
          東京+システム → https://townwork.net/job_search/kw/東京都+システム/?sc=new
        """
        from urllib.parse import quote

        # 現在検索中のエリアを保存（PR除外用）
        self._current_search_area = area

        # 都道府県のローマ字を取得
        pref_roman = self.PREF_ROMAN.get(area)

        # キーワードがカテゴリマッピングにあるかチェック
        category_info = self.JOB_CATEGORIES.get(keyword)

        if pref_roman and category_info:
            # カテゴリ検索URLを生成
            oc_code, omc_code = category_info
            self._current_category_path = oc_code

            if oc_code == "query" and isinstance(omc_code, list):
                # クエリパラメータ形式（複数omc指定）
                omc_params = "&".join([f"omc={code}" for code in omc_code])
                # emp=01,02,03,05 + act=true (新着)
                if page > 1:
                    base_url = f"https://townwork.net/prefectures/{pref_roman}/job_search/?{omc_params}&emp=01&emp=02&emp=03&emp=05&act=true&page={page}"
                else:
                    base_url = f"https://townwork.net/prefectures/{pref_roman}/job_search/?{omc_params}&emp=01&emp=02&emp=03&emp=05&act=true"
                logger.info(f"[タウンワーク] クエリパラメータ形式URL: {base_url}")
            elif omc_code:
                # 小カテゴリあり
                category_path = f"{oc_code}/{omc_code}"
                # ページパラメータ + 新着順ソート
                if page > 1:
                    base_url = f"https://townwork.net/prefectures/{pref_roman}/job_search/{category_path}/?page={page}&sc=new"
                else:
                    base_url = f"https://townwork.net/prefectures/{pref_roman}/job_search/{category_path}/?sc=new"
                logger.info(f"[タウンワーク] カテゴリ検索URL: {base_url}")
            else:
                # 大カテゴリのみ
                category_path = oc_code
                # ページパラメータ + 新着順ソート
                if page > 1:
                    base_url = f"https://townwork.net/prefectures/{pref_roman}/job_search/{category_path}/?page={page}&sc=new"
                else:
                    base_url = f"https://townwork.net/prefectures/{pref_roman}/job_search/{category_path}/?sc=new"
                logger.info(f"[タウンワーク] カテゴリ検索URL: {base_url}")
        else:
            # フリーキーワード検索にフォールバック
            self._current_category_path = None

            # エリア名を正式名称に変換（東京→東京都）
            area_name = self.AREA_NAMES.get(area, area)

            # エリア+キーワードをURLエンコード（+は%2Bではなく+のまま）
            search_query = f"{area_name}+{keyword}"
            encoded_query = quote(search_query, safe='+')

            # ページパラメータ + 新着順ソート
            if page > 1:
                base_url = f"https://townwork.net/job_search/kw/{encoded_query}/?page={page}&sc=new"
            else:
                base_url = f"https://townwork.net/job_search/kw/{encoded_query}/?sc=new"

            logger.info(f"[タウンワーク] キーワード検索URL: {base_url}")

        return base_url

    async def _check_no_results(self, page: Page) -> bool:
        """
        検索結果が0件かどうかをチェック

        タウンワークで0件の場合に表示されるメッセージを検出して早期リターンする。
        これにより、セレクタ検出のリトライを避けて次のエリアに進める。
        """
        try:
            # ページのテキストを取得して0件メッセージを検出
            body_text = await page.inner_text("body")

            # タウンワークの0件メッセージパターン
            no_results_patterns = [
                "条件に合う求人がありませんでした",
                "該当する求人は見つかりませんでした",
                "検索結果がありません",
                "求人が見つかりませんでした",
                "条件に該当する求人はありません",
                "0件",
            ]

            for pattern in no_results_patterns:
                if pattern in body_text:
                    return True

            # 別の検出方法: 特定のクラスを持つ要素をチェック
            no_result_selectors = [
                "[class*='noResult']",
                "[class*='no-result']",
                "[class*='empty']",
                "[class*='notFound']",
            ]

            for selector in no_result_selectors:
                elem = await page.query_selector(selector)
                if elem:
                    is_visible = await elem.is_visible()
                    if is_visible:
                        return True

            return False

        except Exception as e:
            logger.debug(f"[タウンワーク] 0件チェックエラー（続行）: {e}")
            return False

    async def _establish_session(self, page: Page) -> bool:
        """
        タウンワークのセッションを確立（ボット検出回避）
        トップページを訪問してCookieを取得
        """
        try:
            logger.info("[タウンワーク] セッション確立中...")
            response = await page.goto(
                "https://townwork.net/",
                wait_until="domcontentloaded",
                timeout=30000
            )
            if response and response.status == 200:
                await page.wait_for_timeout(random.randint(2000, 3000))
                logger.info("[タウンワーク] セッション確立成功")
                return True
            else:
                logger.warning(f"[タウンワーク] セッション確立失敗: status={response.status if response else 'None'}")
                return False
        except Exception as e:
            logger.warning(f"[タウンワーク] セッション確立エラー: {e}")
            return False

    async def search_jobs(self, page: Page, keyword: str, area: str, max_pages: int = 5) -> List[Dict[str, Any]]:
        """
        求人検索を実行し、結果を返す
        """
        all_jobs = []

        # セッション確立（トップページ訪問）
        await self._establish_session(page)

        for page_num in range(1, max_pages + 1):
            url = self.generate_search_url(keyword, area, page_num)
            logger.info(f"Fetching page {page_num}: {url}")

            success = False
            # ページ取得・抽出をリトライ（最大3回）し、取りこぼしを減らす
            for attempt in range(3):
                try:
                    # リトライ時は待機時間を増やす
                    if attempt > 0:
                        wait_time = 3000 + attempt * 2000
                        logger.info(f"[タウンワーク] リトライ前待機: {wait_time}ms")
                        await page.wait_for_timeout(wait_time)

                    response = await page.goto(
                        url,
                        wait_until="domcontentloaded",
                        timeout=30000 + attempt * 10000
                    )
                    # DOM読み込み後の追加待機
                    await page.wait_for_timeout(3000 + attempt * 1000)

                    if response and response.status == 404:
                        logger.warning(f"Page not found: {url}")
                        break

                    card_selector = self.selectors.get("job_cards", "[class*='jobCard']")

                    # カードが描画されるまで数回リトライし、描画遅延による取りこぼしを減らす
                    selector_ready = False
                    for sel_attempt in range(5):
                        try:
                            await page.wait_for_selector(card_selector, timeout=3000 + 500 * sel_attempt)
                            selector_ready = True
                            break
                        except PlaywrightTimeoutError:
                            logger.warning(
                                f"Job cards selector timeout on page {page_num} (attempt {sel_attempt + 1}/5). Retrying after short wait."
                            )
                            await page.wait_for_timeout(1000 + 300 * sel_attempt)

                    if not selector_ready:
                        logger.warning(
                            f"Job cards selector not ready on page {page_num}; attempt {attempt + 1}/3. Retrying page if attempts remain."
                        )
                        if attempt < 2:
                            continue  # もう一度このページをやり直す

                    # 求人カードを取得（おすすめ求人セクションを除外）
                    job_cards = await self._get_search_result_cards(page, card_selector)

                    # 0件の場合は短い待機のあと再取得（描画遅延対策）
                    if len(job_cards) == 0:
                        await page.wait_for_timeout(1000)
                        job_cards = await self._get_search_result_cards(page, card_selector)

                    # それでも0件なら別のリトライ機会があればやり直す
                    if len(job_cards) == 0:
                        logger.warning(f"No job cards found on page {page_num} (attempt {attempt + 1}/3).")
                        if attempt < 2:
                            await page.wait_for_timeout(2000 + attempt * 1000)
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
                    # リアルタイム件数報告
                    self._report_count(len(all_jobs))
                    break  # ページ処理成功

                except Exception as e:
                    logger.error(f"Error fetching page {page_num} (attempt {attempt + 1}/3): {e}")
                    if attempt < 2:
                        await page.wait_for_timeout(2000 + attempt * 1500)
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

            # アクセス・勤務地（複数のセレクタを試す）
            location_selectors = [
                "[class*='accessText']",
                "[class*='access']",
                "[class*='location']",
                "[class*='area']",
                "[class*='station']",
            ]
            for loc_sel in location_selectors:
                access_elem = await card.query_selector(loc_sel)
                if access_elem:
                    access_text = (await access_elem.inner_text()).strip()
                    if access_text:
                        # "交通・アクセス " プレフィックスを除去
                        data["location"] = re.sub(r"^交通・アクセス\s*", "", access_text)
                        break

            # 雇用形態
            job_type_elem = await card.query_selector("[class*='jobType']")
            if job_type_elem:
                data["employment_type"] = (await job_type_elem.inner_text()).strip()

            # 勤務地が空でもスキップしない（詳細ページで取得可能）
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
        page_num: int,
        session_established: bool = False
    ) -> List[Dict[str, Any]]:
        """
        タウンワーク: 1ページ分の求人を取得する実装
        """
        jobs = []

        # セッション確立（1ページ目のみ）
        if page_num == 1 and not session_established:
            await self._establish_session(page)

        url = self.generate_search_url(keyword, area, page_num)
        logger.info(f"[タウンワーク] ページ{page_num}取得開始: {url}")

        # ページ取得・抽出をリトライ（最大3回）
        for attempt in range(3):
            try:
                response = await page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=30000
                )
                # DOM読み込み後に追加で待機（JSレンダリング完了用）
                await page.wait_for_timeout(5000)

                if response:
                    logger.info(f"[タウンワーク] ページ{page_num} HTTPステータス: {response.status}")

                if response and response.status == 404:
                    logger.warning(f"[タウンワーク] ページが見つかりません: {url}")
                    return jobs

                if response and response.status == 403:
                    logger.error(f"[タウンワーク] アクセスブロック (403): {url}")
                    return jobs

                if response and response.status == 400:
                    logger.warning(f"[タウンワーク] 不正なリクエスト (400): {url} - リトライ {attempt + 1}/3")
                    if attempt < 2:
                        await page.wait_for_timeout(3000 + attempt * 2000)
                        continue
                    return jobs

                if response and response.status == 504:
                    logger.warning(f"[タウンワーク] ゲートウェイタイムアウト (504): {url} - リトライ {attempt + 1}/3")
                    if attempt < 2:
                        await page.wait_for_timeout(5000 + attempt * 2000)
                        continue
                    return jobs

                if response and response.status == 503:
                    logger.warning(f"[タウンワーク] サービス利用不可 (503): {url} - リトライ {attempt + 1}/3")
                    if attempt < 2:
                        await page.wait_for_timeout(5000 + attempt * 2000)
                        continue
                    return jobs

                if response and response.status >= 500:
                    logger.warning(f"[タウンワーク] サーバーエラー ({response.status}): {url} - リトライ {attempt + 1}/3")
                    if attempt < 2:
                        await page.wait_for_timeout(5000 + attempt * 2000)
                        continue
                    return jobs

                card_selector = self.selectors.get("job_cards", "[class*='jobCard']")
                logger.info(f"[タウンワーク] セレクタ: {card_selector}")

                # ★ 検索結果0件の早期検出（リトライを避けて次のエリアに進む）
                no_results_detected = await self._check_no_results(page)
                if no_results_detected:
                    logger.info(f"[タウンワーク] 検索結果0件を検出 - {area} × {keyword} (ページ{page_num})")
                    return jobs  # 空リストを返して次のエリアへ

                # カードが描画されるまで数回リトライ
                selector_ready = False
                for sel_attempt in range(4):
                    try:
                        await page.wait_for_selector(card_selector, timeout=3000 + 500 * sel_attempt)
                        selector_ready = True
                        logger.info(f"[タウンワーク] セレクタ検出成功 (試行 {sel_attempt + 1}/4)")
                        break
                    except PlaywrightTimeoutError:
                        logger.warning(
                            f"[タウンワーク] セレクタタイムアウト ページ{page_num} (試行 {sel_attempt + 1}/4)"
                        )
                        await page.wait_for_timeout(800 + 200 * sel_attempt)

                if not selector_ready:
                    # フォールバックセレクタを試す
                    fallback_selectors = [
                        "a[href*='jobid_']",
                        "[class*='JobCard']",
                        "[class*='job-card']",
                        "article[class*='job']",
                    ]
                    for fb_sel in fallback_selectors:
                        try:
                            await page.wait_for_selector(fb_sel, timeout=2000)
                            card_selector = fb_sel
                            selector_ready = True
                            logger.info(f"[タウンワーク] フォールバックセレクタ検出成功: {fb_sel}")
                            break
                        except PlaywrightTimeoutError:
                            continue

                if not selector_ready:
                    # デバッグ情報を出力
                    page_title = await page.title()
                    logger.warning(
                        f"[タウンワーク] セレクタ未検出 ページ{page_num}; 試行 {attempt + 1}/3. ページタイトル: {page_title}"
                    )
                    if attempt < 2:
                        await page.wait_for_timeout(2000)
                        continue

                # 求人カードを取得（おすすめ求人セクションを除外）
                job_cards = await self._get_search_result_cards(page, card_selector)
                logger.info(f"[タウンワーク] _get_search_result_cards結果: {len(job_cards)}件")

                # 0件の場合は短い待機のあと再取得
                if len(job_cards) == 0:
                    await page.wait_for_timeout(1500)
                    job_cards = await self._get_search_result_cards(page, card_selector)
                    logger.info(f"[タウンワーク] 再取得結果: {len(job_cards)}件")

                if len(job_cards) == 0:
                    logger.warning(f"[タウンワーク] 求人カード0件 ページ{page_num} (試行 {attempt + 1}/3)")
                    if attempt < 2:
                        await page.wait_for_timeout(2000 + attempt * 1000)
                        continue
                    else:
                        return jobs

                logger.info(f"[タウンワーク] ページ{page_num}で{len(job_cards)}件の求人を発見")

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
                logger.error(f"Error fetching page {page_num} (attempt {attempt + 1}/3): {e}")
                if attempt < 2:
                    await page.wait_for_timeout(3000 + attempt * 2000)
                    continue
                else:
                    return jobs

        return jobs
