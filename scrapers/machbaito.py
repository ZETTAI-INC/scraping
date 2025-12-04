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

    # JIS都道府県コード (1-47) と全域検索用cityコード
    # URL形式: /prefectures{code}/city_{city_code}/jobtag_{id}?q[sk]=1
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

    # 都道府県全域検索用のcityコード
    # 各都道府県の「すべての求人を見る」に対応するコード
    PREFECTURE_ALL_CITY_CODES = {
        1: 1,       # 北海道
        2: 98,      # 青森
        3: 117,     # 岩手
        4: 142,     # 宮城
        5: 166,     # 秋田
        6: 186,     # 山形
        7: 208,     # 福島
        8: 234,     # 茨城
        9: 274,     # 栃木
        10: 295,    # 群馬
        11: 316,    # 埼玉
        12: 366,    # 千葉
        13: 409,    # 東京
        14: 469,    # 神奈川
        15: 496,    # 新潟
        16: 527,    # 富山
        17: 540,    # 石川
        18: 557,    # 福井
        19: 574,    # 山梨
        20: 593,    # 長野
        21: 627,    # 岐阜
        22: 658,    # 静岡
        23: 691,    # 愛知
        24: 737,    # 三重
        25: 759,    # 滋賀
        26: 778,    # 京都
        27: 799,    # 大阪
        28: 838,    # 兵庫
        29: 876,    # 奈良
        30: 896,    # 和歌山
        31: 912,    # 鳥取
        32: 922,    # 島根
        33: 938,    # 岡山
        34: 964,    # 広島
        35: 984,    # 山口
        36: 1003,   # 徳島
        37: 1020,   # 香川
        38: 1034,   # 愛媛
        39: 1053,   # 高知
        40: 1071,   # 福岡
        41: 1113,   # 佐賀
        42: 1131,   # 長崎
        43: 1149,   # 熊本
        44: 1174,   # 大分
        45: 1192,   # 宮崎
        46: 1210,   # 鹿児島
        47: 1239,   # 沖縄
    }

    # 職種カテゴリID (/jobtag_ エンドポイント用)
    # /prefectures{code}/jobtag_{id}?q[sk]=1 形式で使用
    # https://machbaito.jp/jobtags から取得した正式なID
    JOB_CATEGORY_IDS = {
        # ===== 飲食系 =====
        "飲食": [1],
        "フード": [1],
        "ファミレス": [2],
        "レストラン": [4],
        "寿司": [7],
        "焼肉": [14],
        "イタリアン": [22],
        "ピザ": [23],
        "うどん": [35],
        "蕎麦": [35],
        "ラーメン": [38],
        "居酒屋": [45],
        "バー": [45],
        "カフェ": [57],
        "喫茶": [57],
        "スイーツ": [68],
        "お菓子": [68],
        "和菓子": [76],
        "デリバリー": [82],
        "キッチン": [88],
        "調理師": [90],
        "調理補助": [91],
        "調理": [90, 91],
        "ホール": [92],

        # ===== 販売系 =====
        "販売": [94],
        "家電": [98],
        "量販店": [98],
        "本屋": [110],
        "書店": [110],
        "雑貨": [126],
        "アパレル": [127],
        "服": [127],
        "携帯": [136],
        "スマホ": [136],
        "スーパー": [138],
        "レジ": [138],
        "ドラッグストア": [169],
        "薬局": [169],

        # ===== 美容系 =====
        "美容": [180],
        "理容": [180],
        "サロン": [189],
        "美容師": [189],
        "美容室": [189],
        "セラピスト": [192],
        "マッサージ": [192],
        "エステ": [192],
        "ネイル": [189],

        # ===== エンタメ・レジャー系 =====
        "エンタメ": [198],
        "レジャー": [198],
        "コンサート": [217],
        "イベント": [221],
        "イベントスタッフ": [221],
        "ゲームセンター": [230],
        "カラオケ": [232],
        "パチンコ": [233],
        "スロット": [233],
        "漫画喫茶": [248],
        "ネットカフェ": [248],
        "ジム": [259],
        "フィットネス": [259],
        "スポーツ": [259],

        # ===== 建設・物流系 =====
        "建設": [270],
        "建築": [270],
        "土木": [270],
        "フォークリフト": [283],
        "不動産": [284],
        "物流": [289],
        "配達": [289],
        "配送": [289],
        "運送": [289],
        "引っ越し": [291],
        "新聞配達": [293],
        "ドライバー": [300],
        "運転手": [300],
        "送迎": [304],

        # ===== 製造・軽作業系 =====
        "工場": [314],
        "製造": [314],
        "製造業": [314],
        "軽作業": [328],
        "シール貼り": [332],
        "品出し": [336],
        "ピッキング": [336],
        "検品": [339],
        "箱詰め": [344],
        "仕分け": [348],
        "清掃": [351],
        "掃除": [351],
        "警備": [616],
        "警備員": [616],
        "交通整理": [620],
        "駐車場": [620],
        "梱包": [773],
        "倉庫": [328],

        # ===== オフィス系 =====
        "オフィス": [376],
        "オフィスワーク": [376],
        "デスクワーク": [376],
        "受付": [378],
        "経理": [386],
        "テレアポ": [396],
        "コールセンター": [398],
        "電話": [398],
        "データ入力": [399],
        "タイピング": [399],
        "事務": [402],
        "一般事務": [404],
        "総務": [402],
        "人事": [402],
        "営業": [406],
        "セールス": [406],

        # ===== IT・クリエイティブ系 =====
        "IT": [413],
        "Web": [413],
        "通信": [413],
        "Webデザイナー": [416],
        "エンジニア": [426],
        "SE": [426],
        "システムエンジニア": [426],
        "プログラミング": [431],
        "プログラマー": [431],
        "クリエイティブ": [436],
        "企画": [436],
        "ライター": [444],
        "編集": [444],
        "ゲーム": [453],
        "ゲームテスター": [457],
        "音楽": [461],
        "カメラマン": [96],
        "写真": [96],
        "撮影": [96],
        "デザイン": [436],
        "デザイナー": [416],

        # ===== 教育系 =====
        "教育": [512],
        "保育園": [520],
        "保育士": [520],
        "保育": [520],
        "幼稚園": [520],
        "試験監督": [526],
        "個別指導": [530],
        "学校": [532],
        "学童": [537],
        "塾講師": [543],
        "塾": [543],
        "家庭教師": [544],
        "講師": [543],
        "インストラクター": [259],

        # ===== 医療・介護系 =====
        "医療": [555],
        "病院": [556],
        "クリニック": [556],
        "看護師": [567],
        "看護": [567],
        "介護": [583],
        "福祉": [583],
        "介護施設": [584],
        "老人ホーム": [584],
        "デイサービス": [588],
        "訪問介護": [591],
        "ホームヘルパー": [591],

        # ===== その他 =====
        "専門職": [594],
        "農業": [371],
        "モニター": [607],
        "接客": [633],
        "サービス": [633],
        "ガソリンスタンド": [366],
        "ホテル": [640],
        "旅館": [640],
        "結婚式場": [640],
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
        https://machbaito.jp/prefectures{code}/city_{city_code}/jobtag_{id}?q[sk]=1&page={ページ}

        例: https://machbaito.jp/prefectures13/city_409/jobtag_426?q[sk]=1
            → 東京都全域のエンジニアカテゴリ、新着順

        Args:
            keyword: 検索キーワード
            area: 都道府県名
            page: ページ番号
        """
        prefecture_code = self._get_prefecture_code(area)
        city_code = self.PREFECTURE_ALL_CITY_CODES.get(prefecture_code, prefecture_code)
        job_category_ids = self._get_job_category_ids(keyword)

        # ベースURL構築（都道府県 + 全域cityコード）
        base_url = f"https://machbaito.jp/prefectures{prefecture_code}/city_{city_code}"

        # 職種カテゴリIDがある場合はjobtag_を追加
        if job_category_ids:
            # 最初のカテゴリIDを使用
            cat_id = job_category_ids[0]
            base_url = f"{base_url}/jobtag_{cat_id}"
            logger.info(f"[マッハバイト] 職種カテゴリID: {cat_id} (キーワード: {keyword})")

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

            # 雇用形態キーワード（完全一致用）
            emp_full_patterns = [
                "派遣労働者", "アルバイト・パート", "正社員", "契約社員",
                "派遣社員", "業務委託", "登録制", "アルバイト", "パート"
            ]
            # 部分一致用キーワード
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
                    "[class*='misc']",
                    "[class*='badge']",
                    "[class*='tag']",
                    ".p-works-work-header li",
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
                first_lines = card_text.split('\n')[:10]
                for line in first_lines:
                    line = line.strip()
                    if not line:
                        continue
                    # 完全一致パターンを優先
                    for pattern in emp_full_patterns:
                        if pattern == line or line == pattern:
                            data["employment_type"] = line
                            logger.debug(f"[マッハバイト] 雇用形態(完全一致): {line}")
                            break
                    if "employment_type" in data:
                        break
                    # 部分一致（短い行のみ）
                    if len(line) <= 30:
                        for kw in emp_keywords:
                            if kw in line:
                                data["employment_type"] = line
                                logger.debug(f"[マッハバイト] 雇用形態(部分一致): {line}")
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
            # 1. CSSセレクタで試す（h3を優先）
            company_selectors = [
                "h3",  # 店舗名が通常h3にある
                ".p-works-work-body-name",
                "[class*='company']",
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
                store_patterns = [
                    r'.+店$',      # ○○店（行末）
                    r'.+店[（\(]', # ○○店（
                    r'.+店/',      # ○○店/
                    r'.+支店',     # ○○支店
                    r'.+営業所',   # ○○営業所
                    r'.+事業所',   # ○○事業所
                    r'.+本店',     # ○○本店
                    r'.+支社',     # ○○支社
                ]
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
