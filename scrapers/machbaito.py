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

    # 職種カテゴリID (q[ji][]=X)
    # マッハバイトの職種ID（https://machbaito.jp/prefectures13/jobtags から取得）
    JOB_CATEGORY_IDS = {
        # 飲食・フード (1)
        "飲食": 1,
        "フード": 1,
        "ホール": 1,
        "キッチン": 1,
        "調理": 1,
        "カフェ": 1,
        "居酒屋": 1,
        "ファミレス": 1,
        "ファストフード": 1,
        "レストラン": 1,

        # 販売 (94)
        "販売": 94,
        "レジ": 94,
        "アパレル": 94,
        "コンビニ": 94,
        "スーパー": 94,

        # 美容・理容・サロン (180)
        "美容": 180,
        "美容師": 180,
        "理容": 180,
        "理容師": 180,
        "エステ": 180,
        "ネイル": 180,
        "サロン": 180,

        # エンタメ・レジャー (198)
        "エンタメ": 198,
        "レジャー": 198,
        "カラオケ": 198,
        "ゲーム": 198,
        "パチンコ": 198,

        # 建設・建築・土木 (270)
        "建設": 270,
        "建築": 270,
        "土木": 270,

        # 物流・配達・配送・運送 (289)
        "物流": 289,
        "配達": 289,
        "配送": 289,
        "運送": 289,
        "ドライバー": 289,
        "デリバリー": 289,

        # 工場・製造業 (314)
        "工場": 314,
        "製造": 314,
        "製造業": 314,

        # 軽作業 (328)
        "軽作業": 328,
        "倉庫": 328,
        "仕分け": 328,
        "梱包": 328,
        "ピッキング": 328,
        "検品": 328,
        "シール貼り": 328,

        # オフィスワーク・デスクワーク (376)
        "事務": 376,
        "オフィス": 376,
        "オフィスワーク": 376,
        "一般事務": 376,
        "データ入力": 376,
        "経理": 376,
        "総務": 376,
        "人事": 376,
        "受付": 376,
        "コールセンター": 376,

        # 営業(セールス) (406)
        "営業": 406,
        "セールス": 406,
        "ルート営業": 406,
        "法人営業": 406,
        "テレアポ": 406,

        # IT・Web・通信 (413)
        "IT": 413,
        "Web": 413,
        "エンジニア": 413,
        "SE": 413,
        "システムエンジニア": 413,
        "プログラマー": 413,
        "通信": 413,

        # クリエイティブ・企画 (436)
        "クリエイティブ": 436,
        "企画": 436,
        "デザイン": 436,
        "デザイナー": 436,

        # 教育 (512)
        "教育": 512,
        "塾講師": 512,
        "塾": 512,
        "家庭教師": 512,
        "講師": 512,
        "保育": 512,
        "保育士": 512,

        # 医療・介護・福祉 (555)
        "医療": 555,
        "介護": 555,
        "福祉": 555,
        "看護": 555,
        "看護師": 555,
        "薬剤師": 555,

        # 専門職種・専門サービス (594)
        "専門": 594,
        "警備": 594,
        "清掃": 594,

        # 接客・サービス (633)
        "接客": 633,
        "サービス": 633,
        "イベント": 633,
        "イベントスタッフ": 633,
        "キャンペーン": 633,
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

    def _get_job_category_id(self, keyword: str) -> Optional[int]:
        """キーワードから職種カテゴリIDを取得"""
        if not keyword:
            return None

        # 完全一致を優先
        if keyword in self.JOB_CATEGORY_IDS:
            return self.JOB_CATEGORY_IDS[keyword]

        # 部分一致
        for category_name, category_id in self.JOB_CATEGORY_IDS.items():
            if keyword in category_name or category_name in keyword:
                return category_id

        return None

    def generate_search_url(self, keyword: str, area: str, page: int = 1) -> str:
        """
        マッハバイト用の検索URL生成

        URL形式:
        https://machbaito.jp/prefectures{都道府県コード}/jobtag_{職種ID}?q[sk]=1&page={ページ}

        例: https://machbaito.jp/prefectures13/jobtag_94?q[sk]=1
            → 東京都の販売カテゴリ、新着順

        Args:
            keyword: 検索キーワード
            area: 都道府県名
            page: ページ番号
        """
        prefecture_code = self._get_prefecture_code(area)
        job_category_id = self._get_job_category_id(keyword)

        # 基本URL（都道府県）
        base_url = f"https://machbaito.jp/prefectures{prefecture_code}"

        # 職種カテゴリがあればパスに追加
        if job_category_id:
            base_url = f"{base_url}/jobtag_{job_category_id}"
            logger.info(f"[マッハバイト] 職種カテゴリID: {job_category_id} (キーワード: {keyword})")

        # クエリパラメータ
        params = ["q[sk]=1"]  # 新着順

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

            # タイトル（最初の意味のある行）
            skip_patterns = ["NEW", "急募", "PR", "おすすめ", "人気"]
            for line in lines:
                if line not in skip_patterns and len(line) >= 3:
                    if not re.search(r'(時給|日給|月給|駅|線)', line):
                        data["title"] = line
                        break

            # 会社名を探す
            company_patterns = ["株式会社", "有限会社", "合同会社", "社団法人", "財団法人"]
            for line in lines:
                for pattern in company_patterns:
                    if pattern in line:
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
