"""
マッハバイト専用スクレイパー
https://machbaito.jp 対応
"""
import asyncio
import random
import re
from typing import Dict, Any, List, Optional
from urllib.parse import quote, urlencode
from playwright.async_api import Page, Browser, TimeoutError as PlaywrightTimeoutError
from .base_scraper import BaseScraper
from utils.stealth import StealthConfig, create_stealth_context
import logging

logger = logging.getLogger(__name__)


class MachbaitoScraper(BaseScraper):
    """マッハバイト用スクレイパー"""

    # JIS都道府県コード (1-47)
    PREFECTURE_CODES = {
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

    # 職種カテゴリID (/works エンドポイント用)
    # /works?q[ji][]=ID&q[pi][]=都道府県コード&q[sk]=1 形式で使用
    # 実際のマッハバイトAPIで検証済みのID
    JOB_CATEGORY_IDS = {
        # ===== 営業・オフィス系 (1-9) =====
        # 営業 (1)
        "営業": [1],
        "セールス": [1],
        "ルート営業": [1],
        "法人営業": [1],
        "テレアポ": [1],

        # 電話・メール (2)
        "電話": [2],
        "コールセンター": [2],
        "テレオペ": [2],

        # 事務・受付・経理 (3)
        "事務": [3],
        "一般事務": [3],
        "経理": [3],
        "受付": [3],
        "オフィス": [3, 4],
        "オフィスワーク": [3, 4],

        # その他オフィス系 (4)
        "データ入力": [4],
        "総務": [3],
        "人事": [3],

        # IT・Web・クリエイティブ系 (5-9)
        "Web": [5],
        "IT": [5, 6],
        "システム": [6],
        "SE": [6],
        "システムエンジニア": [6],
        "ゲーム": [7],
        "デザイン": [8],
        "デザイナー": [8],
        "クリエイティブ": [8, 9, 15, 16, 17],
        "プログラマー": [5, 6],
        "エンジニア": [6],

        # ===== エンタメ・レジャー系 (11-14) =====
        "カラオケ": [11],
        "漫画喫茶": [11],
        "パチンコ": [12],
        "ゲームセンター": [12],
        "アミューズメント": [13],
        "イベント": [13],
        "イベントスタッフ": [13],
        "エンタメ": [11, 12, 13, 14],
        "レジャー": [11, 12, 13, 14],

        # ===== クリエイティブ・メディア系 (15-17) =====
        "編集": [15],
        "撮影": [15],
        "制作": [15],
        "モデル": [16],
        "タレント": [16],

        # ===== 飲食系 (18-24) =====
        "居酒屋": [18],
        "バー": [18],
        "レストラン": [19],
        "ファストフード": [20],
        "ファーストフード": [20],
        "カフェ": [21],
        "喫茶": [21],
        "パン": [22],
        "ケーキ": [22],
        "お菓子": [22],
        "和食": [23],
        "中華": [23],
        "日本料理": [23],
        "飲食": [18, 19, 20, 21, 22, 23, 24],
        "フード": [18, 19, 20, 21, 22, 23, 24],
        "ホール": [18, 19, 23],
        "キッチン": [18, 19, 23],
        "調理": [18, 19, 22, 23],
        "ファミレス": [19],

        # ===== 販売系 (25-29) =====
        "コンビニ": [25],
        "スーパー": [25],
        "レジ": [25],
        "デパート": [26],
        "量販店": [26],
        "家電": [26],
        "書店": [27],
        "本屋": [27],
        "CD": [27],
        "レンタル": [27],
        "アパレル": [28],
        "雑貨": [28],
        "インテリア": [28],
        "販売": [25, 26, 27, 28, 29],

        # ===== サービス・施設系 (30-33) =====
        "ガソリンスタンド": [30],
        "ホテル": [31],
        "旅館": [31],
        "結婚式場": [31],
        "ジム": [32],
        "フィットネス": [32],
        "接客": [31, 32, 33],
        "サービス": [31, 32, 33],

        # ===== 物流・配送系 (34-37) =====
        "在庫管理": [34],
        "入出荷": [34],
        "梱包": [35],
        "仕分け": [35],
        "ピッキング": [35],
        "検品": [35],
        "宅配": [36],
        "配達": [36],
        "配送": [36, 37],
        "新聞配達": [36],
        "物流": [34, 35, 36, 37],
        "デリバリー": [36],

        # ===== 軽作業・製造系 (38-41) =====
        "工事": [38],
        "倉庫": [38],
        "引っ越し": [38],
        "警備": [39],
        "清掃": [39],
        "車両誘導": [39],
        "工場": [40],
        "製造": [40],
        "製造業": [40],
        "軽作業": [35, 38, 40, 41],
        "ドライバー": [36, 37],
        "運送": [36, 37],

        # ===== 医療・福祉系 (42-45) =====
        "看護": [42],
        "看護師": [42],
        "歯科助手": [42],
        "医療": [42, 43, 44, 45],
        "病院": [43],
        "クリニック": [43],
        "介護": [44],
        "ホームヘルパー": [44],
        "福祉": [44, 45],
        "薬剤師": [45],

        # ===== 教育系 (46-49) =====
        "塾講師": [46],
        "塾": [46],
        "家庭教師": [46],
        "語学": [47],
        "英語": [47],
        "インストラクター": [48],
        "スポーツ": [48],
        "保育": [49],
        "保育士": [49],
        "教育": [46, 47, 48, 49],
        "講師": [46, 47, 48],

        # ===== 美容系 (50-52) =====
        "エステ": [50],
        "セラピスト": [50],
        "マッサージ": [50],
        "美容師": [51],
        "理容": [51],
        "理容師": [51],
        "美容": [50, 51, 52],
        "ネイル": [52],
        "サロン": [50, 51, 52],
        "コスメ": [52],
    }

    def __init__(self):
        super().__init__(site_name="machbaito")
        self._realtime_callback = None

    def set_realtime_callback(self, callback):
        """リアルタイム件数コールバックを設定"""
        self._realtime_callback = callback

    def _report_count(self, count: int):
        """件数を報告"""
        if self._realtime_callback:
            self._realtime_callback(count)

    def _get_prefecture_code(self, area: str) -> int:
        """エリア名から都道府県コードを取得"""
        # 「県」「府」「都」などを除去して検索
        area_clean = area.rstrip("都府県")
        return self.PREFECTURE_CODES.get(area_clean, self.PREFECTURE_CODES.get(area, 13))

    def _get_job_category_ids(self, keyword: str) -> Optional[List[int]]:
        """キーワードから職種カテゴリIDリストを取得"""
        if not keyword:
            return None

        # 完全一致を優先
        if keyword in self.JOB_CATEGORY_IDS:
            return self.JOB_CATEGORY_IDS[keyword]

        # 部分一致
        for category_name, category_ids in self.JOB_CATEGORY_IDS.items():
            if keyword in category_name or category_name in keyword:
                return category_ids

        return None

    def generate_search_url(self, keyword: str, area: str, page: int = 1) -> str:
        """
        マッハバイト用の検索URL生成

        URL形式:
        https://machbaito.jp/works?q[ji][]=ID1&q[ji][]=ID2&...&q[pi][]=都道府県コード&q[sk]=1&page={ページ}

        例: https://machbaito.jp/works?q[ji][]=25&q[ji][]=26&q[ji][]=27&q[ji][]=28&q[ji][]=29&q[pi][]=15&q[sk]=1
            → 新潟県の販売カテゴリすべて、新着順

        Args:
            keyword: 検索キーワード
            area: 都道府県名
            page: ページ番号
        """
        prefecture_code = self._get_prefecture_code(area)
        job_category_ids = self._get_job_category_ids(keyword)

        # /works エンドポイントを使用
        base_url = "https://machbaito.jp/works"

        # クエリパラメータ
        params = []

        # 職種カテゴリIDを追加（複数可）
        if job_category_ids:
            for cat_id in job_category_ids:
                params.append(f"q[ji][]={cat_id}")
            logger.info(f"[マッハバイト] 職種カテゴリID: {job_category_ids} (キーワード: {keyword})")

        # 都道府県コード
        params.append(f"q[pi][]={prefecture_code}")

        # 新着順
        params.append("q[sk]=1")

        # ページ番号（2ページ目以降）
        if page > 1:
            params.append(f"page={page}")

        url = f"{base_url}?{'&'.join(params)}"
        logger.info(f"[マッハバイト] 生成URL: {url}")
        return url

    async def _check_no_results(self, page: Page) -> bool:
        """
        検索結果が0件かどうかをチェック
        早期にリターンしてセレクタタイムアウトを避ける
        """
        try:
            body_text = await page.inner_text("body")
            no_results_patterns = [
                "0件がヒット",
                "該当する求人がありません",
                "条件に合う求人が見つかりませんでした",
                "求人が見つかりませんでした",
                "検索結果がありません",
            ]
            for pattern in no_results_patterns:
                if pattern in body_text:
                    return True
            return False
        except Exception as e:
            logger.debug(f"[マッハバイト] 0件チェックエラー（続行）: {e}")
            return False

    async def search_jobs(
        self,
        page: Page,
        keyword: str,
        area: str,
        max_pages: int = 3
    ) -> Dict[str, Any]:
        """
        求人検索を実行

        Args:
            page: Playwrightのページオブジェクト
            keyword: 検索キーワード
            area: 都道府県名
            max_pages: 最大ページ数

        Returns:
            Dict with 'jobs' list and 'raw_count'
        """
        all_jobs = []
        raw_count = 0

        for page_num in range(1, max_pages + 1):
            url = self.generate_search_url(keyword, area, page_num)
            logger.info(f"[マッハバイト] ページ {page_num}/{max_pages}: {url}")

            try:
                response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)

                if response:
                    logger.info(f"[マッハバイト] HTTPステータス: {response.status}")

                if response and response.status >= 400:
                    logger.warning(f"[マッハバイト] エラーステータス: {response.status}")
                    break

                # ページ読み込み待機
                await page.wait_for_timeout(2000)

                # 検索結果0件チェック
                if await self._check_no_results(page):
                    logger.info(f"[マッハバイト] 検索結果0件を検出 - {area} × {keyword}")
                    break

                # 求人カードを取得
                jobs = await self._extract_jobs(page)

                if not jobs:
                    logger.info(f"[マッハバイト] ページ {page_num} に求人なし、終了")
                    break

                all_jobs.extend(jobs)
                raw_count += len(jobs)
                self._report_count(len(all_jobs))

                logger.info(f"[マッハバイト] ページ {page_num}: {len(jobs)}件取得 (累計: {len(all_jobs)}件)")

                # 次のページがあるかチェック
                has_next = await self._has_next_page(page, page_num)
                if not has_next:
                    logger.info(f"[マッハバイト] 最終ページに到達")
                    break

                # ページ間の待機
                await page.wait_for_timeout(random.randint(1000, 2000))

            except PlaywrightTimeoutError:
                logger.warning(f"[マッハバイト] ページ {page_num} タイムアウト")
                break
            except Exception as e:
                logger.error(f"[マッハバイト] ページ {page_num} エラー: {e}")
                break

        logger.info(f"[マッハバイト] 検索完了: {len(all_jobs)}件")
        return {
            'jobs': all_jobs,
            'raw_count': raw_count
        }

    async def _extract_jobs(self, page: Page) -> List[Dict[str, Any]]:
        """ページから求人情報を抽出"""
        jobs = []

        try:
            # 求人カードのセレクタを試す
            card_selectors = [
                "a[href*='/detail/']",
                ".job-card",
                ".job-item",
                "[class*='JobCard']",
                "[class*='jobCard']",
            ]

            job_cards = []
            used_selector = None

            for selector in card_selectors:
                job_cards = await page.query_selector_all(selector)
                if job_cards:
                    used_selector = selector
                    logger.info(f"[マッハバイト] セレクタ検出: {selector} ({len(job_cards)}件)")
                    break

            if not job_cards:
                logger.warning("[マッハバイト] 求人カードが見つかりません")
                return jobs

            for card in job_cards:
                try:
                    job_data = await self._extract_card_data(card, page)
                    if job_data and job_data.get("page_url"):
                        jobs.append(job_data)
                except Exception as e:
                    logger.debug(f"[マッハバイト] カード抽出エラー: {e}")
                    continue

        except Exception as e:
            logger.error(f"[マッハバイト] 求人抽出エラー: {e}")

        return jobs

    async def _extract_card_data(self, card, page: Page) -> Optional[Dict[str, Any]]:
        """求人カードからデータを抽出"""
        try:
            data = {"site": "マッハバイト"}

            # 雇用形態キーワード
            emp_keywords = ["アルバイト", "パート", "正社員", "派遣", "契約", "業務委託", "登録制"]

            # 親要素を取得して雇用形態を探す
            # カードがaタグの場合、親のli要素から雇用形態を取得
            try:
                # 親要素を取得（複数レベル試す）
                parent_elem = await card.evaluate_handle("el => el.parentElement")
                if parent_elem:
                    parent = parent_elem.as_element()
                    if parent:
                        # 親要素から雇用形態セレクタを探す
                        employment_selectors = [
                            "li.p-works-work-header-tag",
                            ".p-works-work-header-tag",
                            "[class*='header-tag']",
                        ]
                        for selector in employment_selectors:
                            try:
                                emp_elem = await parent.query_selector(selector)
                                if emp_elem:
                                    emp_text = await emp_elem.inner_text()
                                    if emp_text:
                                        emp_text = emp_text.strip()
                                        for kw in emp_keywords:
                                            if kw in emp_text:
                                                data["employment_type"] = emp_text
                                                logger.debug(f"[マッハバイト] 雇用形態(親): {emp_text}")
                                                break
                                        if "employment_type" in data:
                                            break
                            except:
                                continue
            except Exception as e:
                logger.debug(f"[マッハバイト] 親要素取得エラー: {e}")

            # カード内からも試す
            if "employment_type" not in data:
                employment_selectors = [
                    "li.p-works-work-header-tag",
                    ".p-works-work-header-tag",
                    "[class*='header-tag']",
                    "[class*='employment']",
                    "[class*='job-type']",
                ]
                for selector in employment_selectors:
                    try:
                        emp_type_elem = await card.query_selector(selector)
                        if emp_type_elem:
                            emp_type_text = await emp_type_elem.inner_text()
                            if emp_type_text:
                                emp_text = emp_type_text.strip()
                                for kw in emp_keywords:
                                    if kw in emp_text:
                                        data["employment_type"] = emp_text
                                        logger.debug(f"[マッハバイト] 雇用形態(カード): {emp_text}")
                                        break
                                if "employment_type" in data:
                                    break
                    except:
                        continue

            # カード内テキストからも雇用形態を探す（フォールバック）
            if "employment_type" not in data:
                card_text = await card.inner_text()
                first_lines = card_text.split('\n')[:5]
                for line in first_lines:
                    line = line.strip()
                    for kw in emp_keywords:
                        if kw in line and len(line) <= 25:
                            data["employment_type"] = line
                            logger.debug(f"[マッハバイト] 雇用形態(テキスト): {line}")
                            break
                    if "employment_type" in data:
                        break

            # リンクを取得
            href = await card.get_attribute("href")
            if not href:
                link_elem = await card.query_selector("a[href*='/detail/']")
                if link_elem:
                    href = await link_elem.get_attribute("href")

            if href:
                if href.startswith("/"):
                    href = f"https://machbaito.jp{href}"
                data["page_url"] = href

                # job_idを抽出
                match = re.search(r"/detail/(\d+)", href)
                if match:
                    data["job_id"] = match.group(1)

            # カード内のテキストを取得
            card_text = await card.inner_text()
            lines = [line.strip() for line in card_text.split('\n') if line.strip()]

            # テキストから情報を抽出
            for i, line in enumerate(lines):
                # 給与パターン
                if re.search(r'(時給|日給|月給|年収)', line):
                    data["salary"] = line
                # 勤務地パターン（駅名など）
                elif re.search(r'(駅|線)', line) and "location" not in data:
                    data["location"] = line
                # 都道府県パターン
                elif re.search(r'(都|府|県|市|区)$', line) and len(line) <= 10:
                    if "location" not in data:
                        data["location"] = line

            # タイトル除外用のパターン
            title_skip_patterns = [
                # 雇用形態
                "派遣労働者", "アルバイト・パート", "アルバイト", "パート",
                "正社員", "契約社員", "派遣社員", "派遣", "業務委託", "登録制",
                # 条件マーカー
                "短期", "長期", "単発", "日払い", "週払い",
                "フリーター", "学生歓迎", "主婦歓迎", "未経験OK",
                "未経験歓迎", "経験者歓迎", "経験者優遇", "高校生OK",
                "シニア歓迎", "Wワーク", "副業OK", "扶養内OK", "新着",
            ]

            # タイトル（最初の意味のある行）
            skip_patterns = ["NEW", "急募", "PR", "おすすめ", "人気"]
            for line in lines:
                if line in skip_patterns:
                    continue
                if len(line) < 3:
                    continue
                # 給与・駅名はスキップ
                if re.search(r'(時給|日給|月給|駅|線)', line):
                    continue
                # 雇用形態・条件マーカーのみの行はスキップ
                is_skip_pattern = False
                for pattern in title_skip_patterns:
                    if line == pattern or (pattern in line and len(line) <= 15):
                        is_skip_pattern = True
                        break
                if is_skip_pattern:
                    continue
                data["title"] = line
                break

            # 会社名を探す
            # 1. CSSセレクタで試す
            company_selectors = [
                ".p-works-work-body-name",
                "[class*='company']",
                "[class*='name']",
                "h3",
                "h2",
            ]
            for selector in company_selectors:
                try:
                    company_elem = await card.query_selector(selector)
                    if company_elem:
                        company_text = await company_elem.inner_text()
                        if company_text:
                            company_text = company_text.strip()
                            # 給与や雇用形態でないことを確認
                            if not re.search(r'(時給|日給|月給|アルバイト|パート|正社員|派遣)', company_text):
                                if len(company_text) >= 3:
                                    data["company_name"] = company_text
                                    break
                except:
                    continue

            # 2. 親要素からも試す
            if "company_name" not in data:
                try:
                    parent_elem = await card.evaluate_handle("el => el.parentElement")
                    if parent_elem:
                        parent = parent_elem.as_element()
                        if parent:
                            for selector in company_selectors:
                                try:
                                    company_elem = await parent.query_selector(selector)
                                    if company_elem:
                                        company_text = await company_elem.inner_text()
                                        if company_text:
                                            company_text = company_text.strip()
                                            if not re.search(r'(時給|日給|月給|アルバイト|パート|正社員|派遣)', company_text):
                                                if len(company_text) >= 3:
                                                    data["company_name"] = company_text
                                                    break
                                except:
                                    continue
                except:
                    pass

            # 3. テキストから会社名パターンを探す
            if "company_name" not in data:
                # 法人格パターン
                company_patterns = ["株式会社", "有限会社", "合同会社", "社団法人", "財団法人"]
                for line in lines:
                    for pattern in company_patterns:
                        if pattern in line:
                            data["company_name"] = line
                            break
                    if "company_name" in data:
                        break

            # 4. 店舗名パターン（「○○店」「○○支店」など）
            if "company_name" not in data:
                store_patterns = [r'.+店[（\(]', r'.+店/', r'.+支店', r'.+営業所', r'.+事業所']
                for line in lines:
                    for pattern in store_patterns:
                        if re.search(pattern, line):
                            # 給与や条件でないことを確認
                            if not re.search(r'(時給|日給|月給|円|駅|線|分)', line):
                                data["company_name"] = line
                                break
                    if "company_name" in data:
                        break

            return data if data.get("page_url") else None

        except Exception as e:
            logger.error(f"[マッハバイト] カードデータ抽出エラー: {e}")
            return None

    async def _has_next_page(self, page: Page, current_page: int) -> bool:
        """次のページがあるかチェック"""
        try:
            # ページネーションリンクを探す
            next_page = current_page + 1
            next_link = await page.query_selector(f"a[href*='page={next_page}']")
            if next_link:
                return True

            # 「次へ」ボタンを探す
            next_buttons = await page.query_selector_all("a[class*='next'], a[rel='next'], .pagination a:last-child")
            for btn in next_buttons:
                text = await btn.inner_text()
                if "次" in text or "›" in text or ">" in text:
                    return True

            return False
        except Exception:
            return False

    async def extract_detail_info(self, page: Page, url: str) -> Dict[str, Any]:
        """詳細ページから追加情報を取得"""
        detail_data = {}

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

            body_text = await page.inner_text("body")

            # 給与
            salary_match = re.search(r"(時給|日給|月給)[：:\s]*([0-9,]+円[^\n]*)", body_text)
            if salary_match:
                detail_data["salary"] = salary_match.group(0).strip()

            # 勤務地
            location_match = re.search(r"勤務地[：:\s]*([^\n]+)", body_text)
            if location_match:
                detail_data["location"] = location_match.group(1).strip()

            # 勤務時間
            time_match = re.search(r"勤務時間[：:\s]*([^\n]+)", body_text)
            if time_match:
                detail_data["working_hours"] = time_match.group(1).strip()

            # 仕事内容
            desc_match = re.search(r"仕事内容[：:\s]*([^\n]+)", body_text)
            if desc_match:
                detail_data["job_description"] = desc_match.group(1).strip()[:500]

            # 電話番号
            phone_match = re.search(r"(\d{2,4}-\d{2,4}-\d{3,4})", body_text)
            if phone_match:
                detail_data["phone"] = phone_match.group(1)

        except Exception as e:
            logger.error(f"[マッハバイト] 詳細取得エラー: {e}")

        return detail_data
