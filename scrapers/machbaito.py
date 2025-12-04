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

            skipped_carousel = 0
            for card in job_cards:
                try:
                    # カルーセル（おすすめ）セクションのカードはスキップ
                    # これらは検索結果ではなく推薦求人のため構造が異なる
                    parent_class = await card.evaluate(
                        "el => el.parentElement ? el.parentElement.className : ''"
                    )
                    if 'carousel' in parent_class.lower():
                        skipped_carousel += 1
                        continue

                    job_data = await self._extract_card_data(card, page)
                    if job_data and job_data.get("page_url"):
                        jobs.append(job_data)
                except Exception as e:
                    logger.debug(f"[マッハバイト] カード抽出エラー: {e}")
                    continue

            if skipped_carousel > 0:
                logger.info(f"[マッハバイト] カルーセル項目をスキップ: {skipped_carousel}件")

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

            # 親要素・祖父母要素を取得して雇用形態を探す
            # カードがaタグの場合、親/祖父母のli要素から雇用形態を取得
            employment_selectors = [
                "li.p-works-work-header-tag",
                ".p-works-work-header-tag",
                "[class*='header-tag']",
            ]
            try:
                # 親要素から検索
                parent_elem = await card.evaluate_handle("el => el.parentElement")
                if parent_elem:
                    parent = parent_elem.as_element()
                    if parent and "employment_type" not in data:
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

                # 祖父母要素からも検索
                if "employment_type" not in data:
                    grandparent_elem = await card.evaluate_handle(
                        "el => el.parentElement?.parentElement"
                    )
                    if grandparent_elem:
                        grandparent = grandparent_elem.as_element()
                        if grandparent:
                            for selector in employment_selectors:
                                try:
                                    emp_elem = await grandparent.query_selector(selector)
                                    if emp_elem:
                                        emp_text = await emp_elem.inner_text()
                                        if emp_text:
                                            emp_text = emp_text.strip()
                                            for kw in emp_keywords:
                                                if kw in emp_text:
                                                    data["employment_type"] = emp_text
                                                    logger.debug(f"[マッハバイト] 雇用形態(祖父母): {emp_text}")
                                                    break
                                            if "employment_type" in data:
                                                break
                                except:
                                    continue
            except Exception as e:
                logger.debug(f"[マッハバイト] 親/祖父母要素取得エラー: {e}")

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
            card_text = await card.inner_text()
            all_lines = [l.strip() for l in card_text.split('\n') if l.strip()]

            if "employment_type" not in data:
                for line in all_lines[:15]:
                    # 完全一致パターンを優先
                    for pattern in emp_full_patterns:
                        if pattern == line:
                            data["employment_type"] = line
                            break
                    if "employment_type" in data:
                        break

                    # 行の先頭に雇用形態がある場合（例: "正社員promesa_..."）
                    for pattern in emp_full_patterns:
                        if line.startswith(pattern):
                            data["employment_type"] = pattern
                            break
                    if "employment_type" in data:
                        break

                    # 部分一致（短い行のみ、給与以外）
                    if len(line) <= 20 and not re.search(r'(時給|日給|月給|円)', line):
                        for kw in emp_keywords:
                            if kw in line:
                                data["employment_type"] = line
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

            # テキストから情報を抽出（all_linesは既に定義済み）
            for i, line in enumerate(all_lines):
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
                # 店舗・業態タイプ
                "ドラッグストア", "コンビニ", "スーパー", "百貨店", "デパート",
                "ホームセンター", "家電量販店", "アパレル", "雑貨店",
                "居酒屋", "カフェ", "レストラン", "ファミレス", "ファストフード",
                "ホテル", "旅館", "病院", "介護施設", "保育園", "学習塾",
                # 複合カテゴリ
                "飲食・フード", "販売・サービス", "オフィス・事務", "軽作業・物流",
                "医療・介護", "教育・塾", "IT・エンジニア", "営業・販売",
                # 職種カテゴリ（短い）
                "販売", "事務", "接客", "清掃", "警備", "配送", "物流",
                "製造", "工場", "軽作業", "仕分け", "検品", "梱包",
                "調理", "キッチン", "ホール", "デリバリー",
                "営業", "受付", "データ入力", "コールセンター",
                "介護", "看護", "保育", "医療",
            ]

            # 都道府県パターン
            prefecture_pattern = re.compile(
                r'^(北海道|青森県?|岩手県?|宮城県?|秋田県?|山形県?|福島県?|'
                r'茨城県?|栃木県?|群馬県?|埼玉県?|千葉県?|東京都?|神奈川県?|'
                r'新潟県?|富山県?|石川県?|福井県?|山梨県?|長野県?|'
                r'岐阜県?|静岡県?|愛知県?|三重県?|'
                r'滋賀県?|京都府?|大阪府?|兵庫県?|奈良県?|和歌山県?|'
                r'鳥取県?|島根県?|岡山県?|広島県?|山口県?|'
                r'徳島県?|香川県?|愛媛県?|高知県?|'
                r'福岡県?|佐賀県?|長崎県?|熊本県?|大分県?|宮崎県?|鹿児島県?|沖縄県?)$'
            )

            # タイトル（最初の意味のある行）
            skip_patterns = ["NEW", "急募", "PR", "おすすめ", "人気"]
            for line in all_lines:
                if line in skip_patterns:
                    continue
                if len(line) < 3:
                    continue
                # 給与・駅名はスキップ
                if re.search(r'(時給|日給|月給|駅|線)', line):
                    continue
                # 都道府県のみの行はスキップ
                if prefecture_pattern.match(line):
                    continue
                # 雇用形態・条件マーカーのみの行はスキップ
                # 完全一致、またはパターン+少数文字のみの場合スキップ
                # 「ホール」はスキップ、「ホール係」はスキップ、「ホールスタッフ」はスキップしない
                is_skip_pattern = False
                for pattern in title_skip_patterns:
                    if line == pattern or (pattern in line and len(line) <= len(pattern) + 2):
                        is_skip_pattern = True
                        break
                if is_skip_pattern:
                    continue
                data["title"] = line
                break

            # 会社名を探す
            # ヘルパー関数: 雇用形態プレフィックスを除去
            def strip_employment_prefix(text):
                for emp in emp_full_patterns:
                    if text.startswith(emp):
                        stripped = text[len(emp):].strip()
                        # 区切り文字も除去
                        if stripped.startswith(('_', '/', '　', ' ')):
                            stripped = stripped[1:].strip()
                        return stripped if stripped else text
                return text

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
                            company_text = strip_employment_prefix(company_text.strip())
                            # 給与でないことを確認
                            if not re.search(r'(時給|日給|月給|円$)', company_text):
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
                                            company_text = strip_employment_prefix(company_text.strip())
                                            if not re.search(r'(時給|日給|月給|円$)', company_text):
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
                for line in all_lines:
                    line_clean = strip_employment_prefix(line)
                    for pattern in company_patterns:
                        if pattern in line_clean:
                            data["company_name"] = line_clean
                            break
                    if "company_name" in data:
                        break

            # 4. 店舗名パターン（「○○店」「○○支店」など）
            if "company_name" not in data:
                store_patterns = [
                    r'.+店$',      # ○○店（行末）
                    r'.+店[（\(]', # ○○店（
                    r'.+店/',      # ○○店/
                    r'.+店\s',     # ○○店 スペース続き
                    r'.+店\[',     # ○○店[ID] 形式
                    r'.+店\d',     # ○○店1, ○○店5 等
                    r'.+支店',     # ○○支店
                    r'.+営業所',   # ○○営業所
                    r'.+事業所',   # ○○事業所
                    r'.+本店',     # ○○本店
                    r'.+支社',     # ○○支社
                ]
                for line in all_lines:
                    line_clean = strip_employment_prefix(line)
                    for pattern in store_patterns:
                        if re.search(pattern, line_clean):
                            # 給与や条件でないことを確認
                            if not re.search(r'(時給|日給|月給|円|駅|線|分)', line_clean):
                                # 末尾の[ID]を除去
                                company = re.sub(r'\[\d+\]$', '', line_clean).strip()
                                data["company_name"] = company
                                break
                    if "company_name" in data:
                        break

            # 5. カッコ付き会社名（「○○(説明)」形式）
            if "company_name" not in data:
                for line in all_lines:
                    line_clean = strip_employment_prefix(line)
                    # カッコ付きの会社名パターン（例: アースサポート和光(訪問入浴オペレーター)）
                    if re.search(r'.+[（(].+[）)]$', line_clean) and len(line_clean) >= 8:
                        # 給与パターンを除外（数字付きのみ: 時給1000円、日給8000円など）
                        if not re.search(r'(時給|日給|月給)\d|円[〜～]|円$|駅\s|線\s', line_clean):
                            data["company_name"] = line_clean
                            break

            # 6. スラッシュを含む行（ID付き会社名）
            if "company_name" not in data:
                for line in all_lines:
                    line_clean = strip_employment_prefix(line)
                    # スラッシュ区切りの会社名/ID形式
                    if '/' in line_clean and len(line_clean) >= 10:
                        # 給与・条件・短い行を除外
                        if not re.search(r'(時給|日給|月給|円|駅|分|OK|歓迎)', line_clean):
                            data["company_name"] = line_clean
                            break

            # 7. 最終フォールバック: タイトルの次の行を会社名として取得
            if "company_name" not in data and data.get("title"):
                title = data["title"]
                title_idx = None
                for i, line in enumerate(all_lines):
                    if line == title:
                        title_idx = i
                        break
                if title_idx is not None and title_idx + 1 < len(all_lines):
                    next_line = all_lines[title_idx + 1]
                    # 給与・条件・職種ラベルを除外
                    if not re.search(r'(時給|日給|月給|円|駅|線|分$|職種|給与|勤務地)', next_line):
                        if len(next_line) >= 3 and len(next_line) <= 50:
                            data["company_name"] = next_line

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

        # 都道府県パターン（住所の先頭を判別）
        prefecture_pattern = re.compile(
            r'^(北海道|青森県?|岩手県?|宮城県?|秋田県?|山形県?|福島県?|'
            r'茨城県?|栃木県?|群馬県?|埼玉県?|千葉県?|東京都?|神奈川県?|'
            r'新潟県?|富山県?|石川県?|福井県?|山梨県?|長野県?|'
            r'岐阜県?|静岡県?|愛知県?|三重県?|'
            r'滋賀県?|京都府?|大阪府?|兵庫県?|奈良県?|和歌山県?|'
            r'鳥取県?|島根県?|岡山県?|広島県?|山口県?|'
            r'徳島県?|香川県?|愛媛県?|高知県?|'
            r'福岡県?|佐賀県?|長崎県?|熊本県?|大分県?|宮崎県?|鹿児島県?|沖縄県?)'
        )

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

            # dl.p-detail-table から情報を取得
            # 構造: dl > dt.p-detail-table-title + dd.p-detail-table-content
            # 「勤務地・面接地」は複数回出現するため、最初のものを優先
            dls = await page.query_selector_all("dl.p-detail-table")
            location_found = False

            for dl in dls:
                try:
                    dt = await dl.query_selector("dt.p-detail-table-title")
                    dd = await dl.query_selector("dd.p-detail-table-content")
                    if not dt or not dd:
                        continue

                    dt_text = (await dt.inner_text()).strip()
                    dd_text = (await dd.inner_text()).strip()

                    # 勤務地・面接地（住所と最寄駅を含む）
                    # 複数回出現するため、最初のもの（住所+駅のセット）を使用
                    if "勤務地" in dt_text and "面接地" in dt_text and not location_found:
                        lines = [l.strip() for l in dd_text.split('\n') if l.strip()]

                        # 住所を探す（都道府県で始まる行）
                        for line in lines:
                            if prefecture_pattern.match(line):
                                detail_data["location"] = line
                                location_found = True
                                break

                        # 最寄駅を探す（駅名を含む行）
                        stations = []
                        for line in lines:
                            if ('駅' in line or '線' in line) and not prefecture_pattern.match(line):
                                stations.append(line)
                        if stations:
                            detail_data["nearest_station"] = ", ".join(stations)

                        # 住所が見つかったら、この勤務地セクションの処理を終了
                        if location_found:
                            continue

                    # 給与
                    elif "給与" in dt_text:
                        detail_data["salary"] = dd_text

                    # 勤務時間
                    elif "勤務時間" in dt_text:
                        detail_data["working_hours"] = dd_text

                    # 時間
                    elif dt_text == "時間":
                        if "working_hours" not in detail_data:
                            detail_data["working_hours"] = dd_text

                    # 応募資格
                    elif "応募資格" in dt_text:
                        detail_data["qualifications"] = dd_text

                    # 待遇
                    elif "待遇" in dt_text:
                        detail_data["benefits"] = dd_text

                    # 仕事内容
                    elif "仕事内容" in dt_text:
                        detail_data["job_description"] = dd_text[:500]

                    # 応募情報（会社名・連絡先が含まれる場合）
                    elif "応募情報" in dt_text:
                        # 電話番号を抽出
                        phone_match = re.search(r"(\d{2,4}-\d{2,4}-\d{3,4})", dd_text)
                        if phone_match:
                            detail_data["phone"] = phone_match.group(1)

                except Exception as e:
                    logger.debug(f"[マッハバイト] dl要素処理エラー: {e}")
                    continue

            # フォールバック: dlから取得できなかった場合はbodyテキストから
            if not detail_data.get("location"):
                body_text = await page.inner_text("body")
                # 「勤務地・面接地」セクションの次の行で都道府県で始まるものを取得
                lines = body_text.split('\n')
                for i, line in enumerate(lines):
                    if '勤務地' in line and '面接地' in line:
                        # 後続の行から住所を探す
                        for j in range(i + 1, min(i + 5, len(lines))):
                            if prefecture_pattern.match(lines[j].strip()):
                                detail_data["location"] = lines[j].strip()
                                break
                        break

            if not detail_data.get("phone"):
                body_text = await page.inner_text("body") if "body_text" not in dir() else body_text
                phone_match = re.search(r"(\d{2,4}-\d{2,4}-\d{3,4})", body_text)
                if phone_match:
                    detail_data["phone"] = phone_match.group(1)

        except Exception as e:
            logger.error(f"[マッハバイト] 詳細取得エラー: {e}")

        return detail_data
