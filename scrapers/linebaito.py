"""
LINEバイト専用スクレイパー
React SPAに対応したスクレイピング
"""
import asyncio
import random
import re
from typing import Dict, Any, List, Optional
from playwright.async_api import Page, Browser, TimeoutError as PlaywrightTimeoutError
from .base_scraper import BaseScraper
from utils.stealth import StealthConfig, create_stealth_context
import logging

logger = logging.getLogger(__name__)


class LineBaitoScraper(BaseScraper):
    """LINEバイト用スクレイパー"""

    # JIS都道府県コード
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

    # 業種カテゴリID（LINEバイトのjobCategoryIds）
    # 動画から確認した小項目一覧に基づく正確なマッピング
    # 小項目は上から順番に1から番号が振られている
    JOB_CATEGORY_IDS = {
        # 飲食・フード (1-11)
        "喫茶店": 1,
        "カフェ": 1,
        "居酒屋": 2,
        "ファーストフード": 3,
        "レストラン": 4,
        "ホールスタッフ": 6,
        "ホール": 6,
        "キッチンスタッフ": 7,
        "キッチン": 7,
        "調理": 7,
        "ファミレス": 9,
        "飲食店長": 10,
        "飲食その他": 11,

        # 販売 (12-25)
        "書店": 12,
        "レンタルショップ": 12,
        "雑貨屋": 14,
        "雑貨": 14,
        "アパレル": 15,
        "家電量販店": 17,
        "商品管理": 18,
        "携帯販売": 19,
        "ドラッグストア": 21,
        "スーパー": 22,
        "コンビニ": 22,
        "レジ": 22,
        "パン屋": 23,
        "ケーキ屋": 23,
        "食品販売": 23,

        # 接客・サービス (26-33)
        "ホテル": 26,
        "ブライダル": 26,
        "旅行": 26,
        "レジャー": 28,
        "スポーツジム": 28,
        "ガソリンスタンド": 29,
        "カウンター業務": 30,
        "カウンター": 30,
        "サービスその他": 33,

        # 医療・介護 (34-40)
        "看護師": 34,
        "准看護師": 34,
        "看護": 34,
        "薬剤師": 36,
        "登録販売者": 36,
        "介護スタッフ": 37,
        "介護": 37,
        "専門職": 39,
        "作業療法士": 39,
        "医療その他": 40,
        "看護補助": 40,

        # レジャー・エンタメ (41-46)
        "映画館": 41,
        "遊園地": 41,
        "テーマパーク": 41,
        "カラオケ": 42,
        "ネットカフェ": 42,
        "マンガ喫茶": 42,
        "パチンコ": 44,
        "スロット": 44,
        "売り子": 45,

        # 引越し・配達 (47-51)
        "引越し": 47,
        "引っ越し": 47,
        "配達": 48,
        "ドライバー": 48,
        "デリバリー": 49,
        "バイク便": 49,
        "新聞配達": 50,

        # クリエイティブ (53-64)
        "デザイナー": 53,
        "イラストレーター": 53,
        "デザイン": 53,
        "エンジニア": 54,
        "SE": 54,
        "システムエンジニア": 54,
        "IT": 54,
        "プログラマー": 54,
        "プログラマ": 54,
        "Web": 54,
        "ウェブ": 54,
        "システム": 54,
        "ユーザーサポート": 57,
        "ヘルプデスク": 57,
        "サポート": 57,
        "CADオペレーター": 59,
        "CAD": 59,
        "編集者": 61,
        "ライター": 61,
        "編集": 61,
        "映像制作": 62,
        "音楽制作": 62,
        "フォトグラファー": 62,
        "カメラマン": 62,
        "芸能": 63,
        "エキストラ": 63,
        "モデル": 63,

        # オフィスワーク (65-70)
        "営業アシスタント": 65,
        "受付": 67,
        "事務": 67,
        "一般事務": 67,
        "アシスタント": 67,
        "受付事務": 67,
        "オフィス": 67,
        "データ入力": 67,
        "コールセンター": 68,
        "テレオペ": 68,
        "電話": 68,
        "テレアポ": 69,
        "テレフォンアポインター": 69,
        "オフィスその他": 70,
        "経理": 67,
        "総務": 67,
        "人事": 67,

        # 営業 (71-73)
        "営業": 71,
        "法人営業": 71,
        "ルートセールス": 73,
        "ルート営業": 73,

        # 教育 (77-82)
        "家庭教師": 77,
        "塾講師": 78,
        "塾": 78,
        "保育士": 79,
        "保育補助": 79,
        "保育": 79,
        "試験監督": 80,
        "教育その他": 82,

        # 軽作業 (83-88)
        "工場": 83,
        "製造": 83,
        "ものづくり": 83,
        "仕分け": 84,
        "梱包": 84,
        "軽作業": 84,
        "搬入": 85,
        "設営": 85,
        "警備員": 86,
        "監視員": 86,
        "清掃員": 86,
        "清掃": 86,
        "警備": 86,

        # 建築 (90-92)
        "建築": 90,
        "土木": 90,
        "フォークリフト": 91,

        # イベント・キャンペーン (94-97)
        "サンプリング": 94,
        "ティッシュ配り": 94,
        "イベント企画": 95,
        "イベント運営": 95,
        "イベント": 95,
        "イベントスタッフ": 95,
        "アンケート": 96,
        "モニター": 96,

        # ビューティー (98-100)
        "美容師": 98,
        "理容師": 98,
        "美容": 98,
        "エステ": 100,
        "マッサージ": 100,
    }

    def __init__(self):
        super().__init__(site_name="linebaito")
        self._realtime_callback = None

    def set_realtime_callback(self, callback):
        """リアルタイム件数コールバックを設定"""
        self._realtime_callback = callback

    def _report_count(self, count: int):
        """件数を報告"""
        if self._realtime_callback:
            self._realtime_callback(count)

    def _get_prefecture_id(self, area: str) -> int:
        """エリア名から都道府県IDを取得"""
        # 「県」「府」「都」などを除去して検索
        area_clean = area.rstrip("都府県")
        return self.PREFECTURE_IDS.get(area_clean, self.PREFECTURE_IDS.get(area, 13))

    def _get_job_category_id(self, keyword: str) -> Optional[int]:
        """キーワードから業種カテゴリIDを取得

        完全一致または部分一致でカテゴリIDを検索
        見つからない場合はNoneを返す（キーワード検索にフォールバック）
        """
        if not keyword:
            return None

        # 完全一致を優先
        if keyword in self.JOB_CATEGORY_IDS:
            return self.JOB_CATEGORY_IDS[keyword]

        # 部分一致（キーワードがカテゴリ名に含まれる場合）
        for category_name, category_id in self.JOB_CATEGORY_IDS.items():
            if keyword in category_name or category_name in keyword:
                return category_id

        return None

    def generate_search_url(self, keyword: str, area: str, page: int = 1, job_category_id: Optional[int] = None) -> str:
        """
        LINEバイト用の検索URL生成

        URL形式:
        - 業種指定あり: https://baito.line.me/jobs?jobCategoryIds={category_id}&prefectureId={id}&sort=new_arrival&page={page}
        - 業種指定なし: https://baito.line.me/jobs?prefectureId={id}&keyword={keyword}&sort=new_arrival&page={page}

        Args:
            keyword: 検索キーワード
            area: 都道府県名
            page: ページ番号
            job_category_id: 業種カテゴリID（指定がなければキーワードから自動検出）
        """
        from urllib.parse import quote

        prefecture_id = self._get_prefecture_id(area)

        # 基本URL
        base_url = "https://baito.line.me/jobs"
        params = []

        # 業種カテゴリIDを決定
        # 1. 明示的に指定されている場合はそれを使用
        # 2. 指定がなければキーワードから自動検出
        category_id = job_category_id
        use_keyword_search = True

        if category_id is None and keyword:
            category_id = self._get_job_category_id(keyword)

        if category_id is not None:
            # 業種IDでフィルタリング
            params.append(f"jobCategoryIds={category_id}")
            use_keyword_search = False
            logger.info(f"[LINEバイト] 業種カテゴリID: {category_id} (キーワード: {keyword})")

        # 都道府県
        params.append(f"prefectureId={prefecture_id}")

        # キーワード検索（業種IDが見つからなかった場合のフォールバック）
        if use_keyword_search and keyword:
            params.append(f"keyword={quote(keyword)}")

        # 新着順
        params.append("sort=new_arrival")

        # ページ番号（2ページ目以降）
        if page > 1:
            params.append(f"page={page}")

        url = f"{base_url}?{'&'.join(params)}"
        logger.info(f"[LINEバイト] 生成URL: {url}")
        return url

    # 1ページあたりの件数（無限スクロール）
    ITEMS_PER_PAGE = 20

    async def search_jobs(
        self,
        page: Page,
        keyword: str,
        area: str,
        max_pages: int = 3,
        seen_job_ids: set = None
    ) -> Dict[str, Any]:
        """
        求人検索を実行（無限スクロール対応）

        Args:
            page: Playwrightのページオブジェクト
            keyword: 検索キーワード
            area: 都道府県名
            max_pages: 最大ページ数（1ページ=20件として換算）
            seen_job_ids: 既に取得済みのjob_idセット

        Returns:
            Dict with 'jobs' list and 'raw_count'
        """
        all_jobs = []
        raw_count = 0
        max_items = max_pages * self.ITEMS_PER_PAGE

        if seen_job_ids is None:
            seen_job_ids = set()

        # 検索ページにアクセス（ページ番号なし）
        url = self.generate_search_url(keyword, area, page=1)
        logger.info(f"[LINEバイト] 検索開始: {url}")
        logger.info(f"[LINEバイト] 最大取得件数: {max_items}件 ({max_pages}ページ × {self.ITEMS_PER_PAGE}件)")

        try:
            response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            if response:
                logger.info(f"[LINEバイト] HTTPステータス: {response.status}")

            if response and response.status >= 400:
                logger.warning(f"[LINEバイト] エラーステータス: {response.status}")
                return {'jobs': [], 'raw_count': 0}

            # React SPAなのでレンダリングを待つ
            await page.wait_for_timeout(3000)

            # 求人カードのセレクタを特定
            used_selector = await self._find_job_card_selector(page)
            if not used_selector:
                logger.warning(f"[LINEバイト] 求人カードが見つかりません")
                return {'jobs': [], 'raw_count': 0}

            # 無限スクロールで求人を読み込む
            scroll_count = 0
            max_scroll_attempts = max_pages + 5  # 余裕を持たせる
            previous_count = 0
            no_new_items_count = 0
            reached_recommend_section = False

            # 検索結果終端を示すセクション名（検索結果の下に表示されるもの）
            END_SECTION_TEXTS = [
                "地域のおすすめ",
                "おすすめの求人",
                "こちらもおすすめ",
            ]
            # ページ上部に表示される可能性があるセクション（スクロール後のみチェック）
            TOP_SECTION_TEXTS = [
                "最近チェックした求人",
                "閲覧履歴",
            ]

            while len(all_jobs) < max_items and scroll_count < max_scroll_attempts:
                # 検索結果終端セクションに到達したかチェック
                # ただし、最低1回はスクロールしてから、かつ求人を取得した後のみチェック
                if scroll_count > 0 and len(all_jobs) > 0:
                    end_section = None
                    end_section_name = None

                    # 検索結果の下に表示される終端セクションをチェック
                    for section_text in END_SECTION_TEXTS:
                        end_section = await page.query_selector(f"text={section_text}")
                        if end_section:
                            end_section_name = section_text
                            break

                    # 上部セクションは、スクロール後かつ一定数取得後のみチェック
                    if not end_section and scroll_count >= 2 and len(all_jobs) >= 10:
                        for section_text in TOP_SECTION_TEXTS:
                            section_elem = await page.query_selector(f"text={section_text}")
                            if section_elem:
                                # このセクションがビューポートの下半分にあるかチェック
                                is_below = await section_elem.evaluate("""
                                    (el) => {
                                        const rect = el.getBoundingClientRect();
                                        const viewportHeight = window.innerHeight;
                                        // ビューポートの下半分より下にある場合のみtrue
                                        return rect.top > viewportHeight * 0.5;
                                    }
                                """)
                                if is_below:
                                    end_section = section_elem
                                    end_section_name = section_text
                                    break

                    if end_section:
                        # セクションが画面内に表示されているかチェック
                        is_visible = await end_section.is_visible()
                        if is_visible:
                            logger.info(f"[LINEバイト] 「{end_section_name}」セクションに到達。検索結果の終端です。")
                            reached_recommend_section = True
                            break

                # 現在表示されている求人カードを取得
                job_cards = await page.query_selector_all(used_selector)
                current_count = len(job_cards)

                logger.info(f"[LINEバイト] スクロール{scroll_count}: {current_count}件の求人カード検出")

                # 新しいカードから情報を抽出
                for i in range(previous_count, current_count):
                    if len(all_jobs) >= max_items:
                        break

                    try:
                        card = job_cards[i]

                        # カードが終端セクション（地域のおすすめ等）の後にあるかチェック
                        # 閲覧履歴はページ上部にあるため除外
                        is_in_recommend = await card.evaluate("""
                            (el) => {
                                const endSections = [
                                    '地域のおすすめ',
                                    'おすすめの求人',
                                    'こちらもおすすめ'
                                ];
                                const rect = el.getBoundingClientRect();

                                // ページ内の終端セクションを探す
                                const allElements = document.querySelectorAll('*');
                                for (const sec of allElements) {
                                    const secText = sec.innerText || '';
                                    for (const sectionText of endSections) {
                                        // セクションヘッダーを探す（短いテキストで完全一致に近いもの）
                                        if (secText.trim() === sectionText ||
                                            (secText.includes(sectionText) && secText.length < 50)) {
                                            const secRect = sec.getBoundingClientRect();
                                            // このカードが終端セクションより下にある場合
                                            if (rect.top > secRect.bottom) {
                                                return true;
                                            }
                                        }
                                    }
                                }
                                return false;
                            }
                        """)

                        if is_in_recommend:
                            logger.info(f"[LINEバイト] おすすめセクション内のカードをスキップ")
                            reached_recommend_section = True
                            break

                        job_data = await self._extract_card_data(card, page)
                        if job_data and job_data.get("page_url"):
                            job_id = job_data.get("job_id")
                            if job_id and job_id not in seen_job_ids:
                                seen_job_ids.add(job_id)
                                all_jobs.append(job_data)
                                raw_count += 1
                            elif not job_id:
                                all_jobs.append(job_data)
                                raw_count += 1
                    except Exception as e:
                        logger.debug(f"[LINEバイト] カード抽出エラー: {e}")
                        continue

                if reached_recommend_section:
                    break

                self._report_count(len(all_jobs))

                # 最大件数に達した場合は終了
                if len(all_jobs) >= max_items:
                    logger.info(f"[LINEバイト] 最大件数 {max_items}件に到達")
                    break

                # 新しいアイテムが読み込まれなかった場合
                if current_count == previous_count:
                    no_new_items_count += 1
                    if no_new_items_count >= 3:
                        logger.info(f"[LINEバイト] 新しい求人が読み込まれません。全件取得完了")
                        break
                else:
                    no_new_items_count = 0

                previous_count = current_count

                # ページ下部までスクロール
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(random.randint(1500, 2500))

                scroll_count += 1

            if reached_recommend_section:
                logger.info(f"[LINEバイト] 検索結果終了（おすすめセクション到達）: {len(all_jobs)}件")
            else:
                logger.info(f"[LINEバイト] 取得完了: {len(all_jobs)}件")

        except Exception as e:
            logger.error(f"[LINEバイト] 検索エラー: {e}")

        return {
            'jobs': all_jobs,
            'raw_count': raw_count
        }

    async def _find_job_card_selector(self, page: Page) -> Optional[str]:
        """求人カードのセレクタを特定"""
        card_selectors = [
            "[data-testid='job-card']",
            "[class*='JobCard']",
            "[class*='jobCard']",
            "article[class*='job']",
            "a[href*='/jobs/']",
        ]

        for selector in card_selectors:
            try:
                await page.wait_for_selector(selector, timeout=5000)
                logger.info(f"[LINEバイト] セレクタ検出成功: {selector}")
                return selector
            except PlaywrightTimeoutError:
                continue

        # フォールバック: ページ内のリンクから求人を探す
        logger.warning(f"[LINEバイト] カードセレクタ未検出、リンクから探索")
        job_links = await page.query_selector_all("a[href*='/jobs/']")
        if job_links:
            return "a[href*='/jobs/']"

        return None

    async def _extract_card_data(self, card, page: Page) -> Optional[Dict[str, Any]]:
        """求人カードからデータを抽出"""
        try:
            data = {"site": "LINEバイト"}

            # リンクを取得
            href = await card.get_attribute("href")
            if not href:
                link_elem = await card.query_selector("a[href*='/jobs/']")
                if link_elem:
                    href = await link_elem.get_attribute("href")

            if href:
                if href.startswith("/"):
                    href = f"https://baito.line.me{href}"
                data["page_url"] = href

                # job_idを抽出
                match = re.search(r"/jobs/([^/?]+)", href)
                if match:
                    data["job_id"] = match.group(1)

            # スキップすべきバッジ/ラベルテキスト
            SKIP_TEXTS = {"NEW", "新着", "急募", "PR", "おすすめ", "人気", "注目", "ア", "派", "契", "正"}

            # 職種名を取得
            # LINEバイトのカード構造: NEW → 雇用形態 → 職種名 → 説明文 の順
            job_type = None
            description = None

            # カード内の全テキスト要素を取得して順番に確認
            all_text_elements = await card.query_selector_all("span, div, p, h2, h3, h4")
            text_candidates = []

            for elem in all_text_elements:
                try:
                    text = await elem.inner_text()
                    text = text.strip()
                    if text and len(text) >= 2:
                        # スキップテキストを除外
                        if text in SKIP_TEXTS:
                            continue
                        # 改行を含む場合は分割
                        lines = [line.strip() for line in text.split('\n') if line.strip()]
                        for line in lines:
                            if line not in SKIP_TEXTS and len(line) >= 2:
                                text_candidates.append(line)
                except:
                    continue

            # 重複を除去しつつ順序を保持
            seen = set()
            unique_candidates = []
            for t in text_candidates:
                if t not in seen and t not in SKIP_TEXTS:
                    seen.add(t)
                    unique_candidates.append(t)

            # 職種名と説明文を判別
            for text in unique_candidates:
                # スキップすべきテキスト
                if text in SKIP_TEXTS:
                    continue
                # 給与っぽいもの
                if re.search(r'(時給|日給|月給|年収|万円|\d{3,}円)', text):
                    continue
                # 住所・駅っぽいもの
                if re.search(r'(駅|線|分|区$|市$|町$|村$|都$|府$|県$)', text):
                    continue

                # 短いテキスト（3-25文字）で記号が少ない → 職種名の可能性
                if 3 <= len(text) <= 25 and not any(c in text for c in "♪◆★●！？"):
                    if not job_type:
                        job_type = text
                # 長いテキストまたは記号が多い → 説明文
                elif len(text) > 25 or any(c in text for c in "♪◆★●"):
                    if not description:
                        description = text

                # 両方見つかったら終了
                if job_type and description:
                    break

            # 結果を設定
            if job_type:
                data["title"] = job_type
                if description:
                    data["job_description"] = description
            elif description:
                data["title"] = description

            # 会社名
            company_selectors = [
                "[class*='company']",
                "[class*='Company']",
                "[class*='employer']",
            ]
            for sel in company_selectors:
                company_elem = await card.query_selector(sel)
                if company_elem:
                    company = await company_elem.inner_text()
                    if company:
                        data["company_name"] = company.strip()
                        break

            # 給与と勤務地を取得
            # LINEバイトのカード構造: 給与（青字）→ 勤務地（その直下）
            salary_selectors = [
                "[class*='salary']",
                "[class*='Salary']",
                "[class*='wage']",
                "[class*='pay']",
            ]
            salary_elem = None
            for sel in salary_selectors:
                salary_elem = await card.query_selector(sel)
                if salary_elem:
                    salary = await salary_elem.inner_text()
                    if salary:
                        data["salary"] = salary.strip()
                        break

            # 勤務地: 給与要素の次の兄弟要素から取得
            if salary_elem:
                try:
                    # 給与要素の次の兄弟要素を取得
                    next_sibling = await salary_elem.evaluate_handle("""
                        (el) => {
                            // 次の兄弟要素を探す
                            let next = el.nextElementSibling;
                            if (next) return next;
                            // なければ親の次の兄弟
                            let parent = el.parentElement;
                            while (parent) {
                                next = parent.nextElementSibling;
                                if (next) return next;
                                parent = parent.parentElement;
                            }
                            return null;
                        }
                    """)
                    if next_sibling:
                        next_elem = next_sibling.as_element()
                        if next_elem:
                            location_text = await next_elem.inner_text()
                            if location_text:
                                location_text = location_text.strip()
                                # 給与っぽくないものだけ勤務地として採用
                                if not re.search(r'(時給|日給|月給|円)', location_text):
                                    data["location"] = location_text
                except Exception as e:
                    logger.debug(f"[LINEバイト] 勤務地取得エラー（兄弟要素）: {e}")

            # フォールバック: セレクタで勤務地を探す
            if not data.get("location"):
                location_selectors = [
                    "[class*='location']",
                    "[class*='Location']",
                    "[class*='area']",
                    "[class*='address']",
                    "[class*='place']",
                    "[class*='Place']",
                ]
                for sel in location_selectors:
                    loc_elem = await card.query_selector(sel)
                    if loc_elem:
                        location = await loc_elem.inner_text()
                        if location:
                            data["location"] = location.strip()
                            break

            # さらにフォールバック: テキスト候補から駅名・地名を探す
            if not data.get("location") and unique_candidates:
                for text in unique_candidates:
                    # 駅名・地名パターン
                    if re.search(r'(駅|線|区|市|町|村)', text) and len(text) <= 30:
                        # 給与ではない
                        if not re.search(r'(時給|日給|月給|円)', text):
                            data["location"] = text
                            break

            return data if data.get("page_url") else None

        except Exception as e:
            logger.error(f"[LINEバイト] カードデータ抽出エラー: {e}")
            return None

    async def extract_detail_info(self, page: Page, url: str) -> Dict[str, Any]:
        """詳細ページから追加情報を取得"""
        detail_data = {}

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)

            # ページ全体のテキストを取得
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
            desc_elem = await page.query_selector("[class*='description'], [class*='Description']")
            if desc_elem:
                desc = await desc_elem.inner_text()
                if desc:
                    detail_data["job_description"] = desc.strip()[:500]

        except Exception as e:
            logger.error(f"[LINEバイト] 詳細取得エラー: {e}")

        return detail_data
