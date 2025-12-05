"""
カイゴジョブエージェント専用スクレイパー
https://www.kaigoagent.com/

React SPA (Next.js) に対応したスクレイピング
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


class KaigojobScraper(BaseScraper):
    """カイゴジョブエージェント用スクレイパー"""

    # 都道府県ID (kaigoagent.com独自)
    PREFECTURE_IDS = {
        "北海道": 12000001,
        "青森": 12000002, "岩手": 12000003, "宮城": 12000004, "秋田": 12000005, "山形": 12000006, "福島": 12000007,
        "茨城": 12000008, "栃木": 12000009, "群馬": 12000010, "埼玉": 12000011, "千葉": 12000012, "東京": 12000026, "神奈川": 12000014,
        "新潟": 12000015, "富山": 12000016, "石川": 12000017, "福井": 12000018, "山梨": 12000019, "長野": 12000020,
        "岐阜": 12000021, "静岡": 12000022, "愛知": 12000023, "三重": 12000024,
        "滋賀": 12000025, "京都": 12000013, "大阪": 12000027, "兵庫": 12000028, "奈良": 12000029, "和歌山": 12000030,
        "鳥取": 12000031, "島根": 12000032, "岡山": 12000033, "広島": 12000034, "山口": 12000035,
        "徳島": 12000036, "香川": 12000037, "愛媛": 12000038, "高知": 12000039,
        "福岡": 12000040, "佐賀": 12000041, "長崎": 12000042, "熊本": 12000043, "大分": 12000044, "宮崎": 12000045, "鹿児島": 12000046, "沖縄": 12000047,
    }

    # 職種カテゴリID (occupation_XXXXXXXX)
    # 介護事務・医療事務 → 事務
    # 送迎運転手・ケアドライバー → ドライバー
    # その他 → 介護
    OCCUPATION_IDS = {
        # 事務系
        "事務": "10100013",
        "介護事務": "10100013",
        "医療事務": "10100013",

        # ドライバー系
        "ドライバー": "10100008",
        "送迎運転手": "10100008",
        "ケアドライバー": "10100008",
        "運転手": "10100008",

        # 介護系（デフォルト - 職種指定なしで全介護）
        "介護": None,  # 職種指定なし = 全介護職
        "介護職": None,
        "ヘルパー": None,
        "介護福祉士": None,
        "ケアマネ": None,
        "看護": None,
        "看護師": None,
    }

    # カテゴリ分類（キーワード → カテゴリ名）
    KEYWORD_TO_CATEGORY = {
        "事務": "事務",
        "介護事務": "事務",
        "医療事務": "事務",
        "ドライバー": "ドライバー",
        "送迎運転手": "ドライバー",
        "ケアドライバー": "ドライバー",
        "運転手": "ドライバー",
        # その他は全て「介護」カテゴリ
    }

    def __init__(self):
        super().__init__(site_name="kaigojob")
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

    def _get_occupation_id(self, keyword: str) -> Optional[str]:
        """キーワードから職種IDを取得"""
        if not keyword:
            return None
        return self.OCCUPATION_IDS.get(keyword)

    def _get_category(self, keyword: str) -> str:
        """キーワードからカテゴリ名を取得"""
        return self.KEYWORD_TO_CATEGORY.get(keyword, "介護")

    def generate_search_url(self, keyword: str, area: str, page: int = 1) -> str:
        """
        カイゴジョブエージェント用の検索URL生成

        URL形式:
        - 都道府県のみ: https://www.kaigoagent.com/search/prefecture_{prefecture_id}
        - 職種指定あり: https://www.kaigoagent.com/search/prefecture_{prefecture_id}/occupation_{occupation_id}
        - ページ指定: ?page={page}
        """
        prefecture_id = self._get_prefecture_id(area)
        if not prefecture_id:
            logger.warning(f"[カイゴジョブ] 未知の都道府県: {area}")
            prefecture_id = 12000026  # デフォルト: 東京

        base_url = f"https://www.kaigoagent.com/search/prefecture_{prefecture_id}"

        # 職種IDを取得
        occupation_id = self._get_occupation_id(keyword)
        if occupation_id:
            base_url += f"/occupation_{occupation_id}"
            logger.info(f"[カイゴジョブ] 職種ID: {occupation_id} (キーワード: {keyword})")

        # ページ指定
        if page > 1:
            base_url += f"?page={page}"

        logger.info(f"[カイゴジョブ] 生成URL: {base_url}")
        return base_url

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
        if seen_job_ids is None:
            seen_job_ids = set()

        category = self._get_category(keyword)
        logger.info(f"[カイゴジョブ] 検索開始: {area} × {keyword} (カテゴリ: {category})")

        for page_num in range(1, max_pages + 1):
            url = self.generate_search_url(keyword, area, page_num)
            logger.info(f"[カイゴジョブ] ページ{page_num}: {url}")

            try:
                response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)

                if response and response.status >= 400:
                    logger.warning(f"[カイゴジョブ] エラーステータス: {response.status}")
                    break

                # React SPAなのでレンダリングを待つ
                await page.wait_for_timeout(3000)

                # 検索結果件数を取得
                result_count = await self._get_search_result_count(page)
                if result_count is not None:
                    logger.info(f"[カイゴジョブ] 検索結果: {result_count}件")
                    if result_count == 0:
                        logger.info(f"[カイゴジョブ] 検索結果0件 - 終了")
                        break

                # 求人カードを取得
                jobs = await self._extract_jobs_from_page(page, keyword, area, category, seen_job_ids)

                if not jobs:
                    logger.info(f"[カイゴジョブ] ページ{page_num}で求人が見つかりません - 終了")
                    break

                all_jobs.extend(jobs)
                self._report_count(len(all_jobs))
                logger.info(f"[カイゴジョブ] ページ{page_num}: {len(jobs)}件取得（累計: {len(all_jobs)}件）")

                # 次のページがあるか確認
                has_next = await self._has_next_page(page)
                if not has_next:
                    logger.info(f"[カイゴジョブ] 最終ページに到達")
                    break

                # 待機（ボット検出対策）
                await page.wait_for_timeout(random.randint(1500, 2500))

            except Exception as e:
                logger.error(f"[カイゴジョブ] ページ{page_num}でエラー: {e}")
                break

        logger.info(f"[カイゴジョブ] 検索完了: {len(all_jobs)}件")
        return all_jobs

    async def _get_search_result_count(self, page: Page) -> Optional[int]:
        """検索結果件数を取得"""
        try:
            # ページ内のテキストから件数を抽出
            body_text = await page.inner_text("body")

            # パターン: "XX件の求人" or "検索結果 XX件"
            patterns = [
                r'(\d+)\s*件の求人',
                r'検索結果\s*(\d+)\s*件',
                r'(\d+)\s*件見つかりました',
                r'"numberOfItems"\s*:\s*(\d+)',
            ]

            for pattern in patterns:
                match = re.search(pattern, body_text)
                if match:
                    return int(match.group(1))

            return None
        except Exception as e:
            logger.debug(f"[カイゴジョブ] 件数取得エラー: {e}")
            return None

    async def _extract_jobs_from_page(
        self,
        page: Page,
        keyword: str,
        area: str,
        category: str,
        seen_job_ids: set
    ) -> List[Dict[str, Any]]:
        """ページから求人データを抽出"""
        jobs = []

        try:
            # Next.jsのJSONデータから求人情報を抽出
            json_jobs = await self._extract_from_nextjs_data(page)

            if json_jobs:
                for job_data in json_jobs:
                    job_id = str(job_data.get('id', ''))
                    if job_id and job_id in seen_job_ids:
                        continue
                    if job_id:
                        seen_job_ids.add(job_id)

                    job = {
                        'site': 'カイゴジョブ',
                        'job_id': job_id,
                        'job_number': job_id,
                        'title': job_data.get('name', ''),
                        'company_name': job_data.get('corporation_name', ''),
                        'location': job_data.get('full_address', job_data.get('city', '')),
                        'salary': self._format_salary(job_data.get('min_salary'), job_data.get('max_salary')),
                        'employment_type': self._get_employment_types(job_data.get('employment_types', [])),
                        'job_description': job_data.get('job_description', ''),
                        'page_url': f"https://www.kaigoagent.com/job/{job_id}",
                        'keyword': keyword,
                        'area': area,
                        'category': category,
                        'qualifications': self._get_qualifications(job_data.get('qualifications', [])),
                        'facilities': self._get_facilities(job_data.get('facilities', [])),
                    }
                    jobs.append(job)
            else:
                # フォールバック: HTMLから抽出
                jobs = await self._extract_from_html(page, keyword, area, category, seen_job_ids)

        except Exception as e:
            logger.error(f"[カイゴジョブ] 求人抽出エラー: {e}")

        return jobs

    async def _extract_from_nextjs_data(self, page: Page) -> List[Dict[str, Any]]:
        """Next.jsのJSONデータから求人情報を抽出"""
        try:
            # ページ内のスクリプトからJSONデータを抽出
            script_content = await page.evaluate("""
                () => {
                    const scripts = document.querySelectorAll('script');
                    for (const script of scripts) {
                        const text = script.textContent || '';
                        if (text.includes('__NEXT_DATA__') || text.includes('self.__next_f.push')) {
                            return text;
                        }
                    }
                    // ページ全体のHTMLからデータを探す
                    const html = document.documentElement.innerHTML;
                    const match = html.match(/"jobs":\s*(\[[\s\S]*?\])/);
                    if (match) {
                        return match[1];
                    }
                    return null;
                }
            """)

            if not script_content:
                return []

            # JSONデータを抽出
            jobs_data = []

            # パターン1: "jobs": [...] 形式
            jobs_match = re.search(r'"jobs"\s*:\s*(\[[\s\S]*?\])', script_content)
            if jobs_match:
                try:
                    jobs_data = json.loads(jobs_match.group(1))
                except json.JSONDecodeError:
                    pass

            # パターン2: ItemListElement形式（構造化データ）
            if not jobs_data:
                item_list_match = re.search(r'"itemListElement"\s*:\s*(\[[\s\S]*?\])', script_content)
                if item_list_match:
                    try:
                        items = json.loads(item_list_match.group(1))
                        for item in items:
                            if 'item' in item:
                                jobs_data.append(item['item'])
                    except json.JSONDecodeError:
                        pass

            # パターン3: 個別の求人オブジェクトを抽出
            if not jobs_data:
                job_pattern = r'\{"id"\s*:\s*(\d+)[^}]*"name"\s*:\s*"([^"]*)"[^}]*\}'
                for match in re.finditer(job_pattern, script_content):
                    jobs_data.append({
                        'id': int(match.group(1)),
                        'name': match.group(2)
                    })

            logger.info(f"[カイゴジョブ] Next.jsデータから {len(jobs_data)}件の求人を抽出")
            return jobs_data

        except Exception as e:
            logger.debug(f"[カイゴジョブ] Next.jsデータ抽出エラー: {e}")
            return []

    async def _extract_from_html(
        self,
        page: Page,
        keyword: str,
        area: str,
        category: str,
        seen_job_ids: set
    ) -> List[Dict[str, Any]]:
        """HTMLから求人データを抽出（フォールバック）"""
        jobs = []

        try:
            # 求人カードのセレクタを試行
            card_selectors = [
                "a[href*='/job/']",
                "[class*='JobCard']",
                "[class*='job-card']",
                "[class*='searchResult']",
                "article",
            ]

            for selector in card_selectors:
                cards = await page.query_selector_all(selector)
                if cards:
                    logger.info(f"[カイゴジョブ] セレクタ {selector} で {len(cards)}件のカードを検出")

                    for card in cards:
                        try:
                            job_data = await self._extract_card_data(card, page)
                            if job_data and job_data.get('page_url'):
                                job_id = job_data.get('job_id', '')
                                if job_id and job_id in seen_job_ids:
                                    continue
                                if job_id:
                                    seen_job_ids.add(job_id)

                                job_data['keyword'] = keyword
                                job_data['area'] = area
                                job_data['category'] = category
                                jobs.append(job_data)
                        except Exception as e:
                            logger.debug(f"[カイゴジョブ] カード抽出エラー: {e}")
                            continue
                    break

        except Exception as e:
            logger.error(f"[カイゴジョブ] HTML抽出エラー: {e}")

        return jobs

    async def _extract_card_data(self, card, page: Page) -> Optional[Dict[str, Any]]:
        """求人カードからデータを抽出"""
        try:
            data = {'site': 'カイゴジョブ'}

            # リンクを取得
            href = await card.get_attribute('href')
            if href:
                if href.startswith('/'):
                    href = f"https://www.kaigoagent.com{href}"
                data['page_url'] = href

                # job_idを抽出
                match = re.search(r'/job/(\d+)', href)
                if match:
                    data['job_id'] = match.group(1)
                    data['job_number'] = match.group(1)

            # テキスト情報を取得
            text = await card.inner_text()
            lines = [l.strip() for l in text.split('\n') if l.strip()]

            # 会社名、職種名などを推定
            for i, line in enumerate(lines):
                if not data.get('title') and len(line) > 5 and len(line) < 100:
                    data['title'] = line
                elif '円' in line or '万' in line:
                    data['salary'] = line
                elif any(word in line for word in ['市', '区', '町', '県', '都', '府']):
                    if not data.get('location'):
                        data['location'] = line

            return data if data.get('page_url') else None

        except Exception as e:
            logger.debug(f"[カイゴジョブ] カードデータ抽出エラー: {e}")
            return None

    async def _has_next_page(self, page: Page) -> bool:
        """次のページがあるか確認"""
        try:
            # ページネーションの「次へ」ボタンを探す
            next_selectors = [
                "a[aria-label='次へ']",
                "a:has-text('次へ')",
                "[class*='pagination'] a:last-child",
                "button:has-text('次')",
            ]

            for selector in next_selectors:
                next_btn = await page.query_selector(selector)
                if next_btn:
                    is_disabled = await next_btn.get_attribute('disabled')
                    if not is_disabled:
                        return True

            return False
        except Exception:
            return False

    def _format_salary(self, min_salary: Optional[int], max_salary: Optional[int]) -> str:
        """給与をフォーマット"""
        if min_salary and max_salary:
            return f"{min_salary:,}円～{max_salary:,}円"
        elif min_salary:
            return f"{min_salary:,}円～"
        elif max_salary:
            return f"～{max_salary:,}円"
        return ""

    def _get_employment_types(self, types: List) -> str:
        """雇用形態を取得"""
        if isinstance(types, list):
            return "、".join(str(t) for t in types if t)
        return str(types) if types else ""

    def _get_qualifications(self, qualifications: List) -> str:
        """資格要件を取得"""
        if isinstance(qualifications, list):
            return "、".join(str(q) for q in qualifications if q)
        return str(qualifications) if qualifications else ""

    def _get_facilities(self, facilities: List) -> str:
        """施設形態を取得"""
        if isinstance(facilities, list):
            return "、".join(str(f) for f in facilities if f)
        return str(facilities) if facilities else ""

    async def extract_detail_info(self, page: Page, url: str) -> Dict[str, Any]:
        """詳細ページから追加情報を取得"""
        detail_data = {}

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

            # JSON-LDから情報取得を試みる
            json_ld_data = await self._extract_json_ld(page)
            if json_ld_data:
                detail_data.update(json_ld_data)

            # HTMLから追加情報を取得
            body_text = await page.inner_text("body")

            # 電話番号
            phone_match = re.search(r'(0\d{1,4}-?\d{1,4}-?\d{3,4})', body_text)
            if phone_match:
                detail_data['phone'] = phone_match.group(1)

            # 郵便番号
            postal_match = re.search(r'〒?\s*(\d{3}-?\d{4})', body_text)
            if postal_match:
                detail_data['postal_code'] = postal_match.group(1)

            # 施設形態
            facility_patterns = [
                r'施設形態[：:]\s*([^\n]+)',
                r'サービス種別[：:]\s*([^\n]+)',
            ]
            for pattern in facility_patterns:
                match = re.search(pattern, body_text)
                if match:
                    detail_data['facility_type'] = match.group(1).strip()
                    break

            # 事業内容
            business_patterns = [
                r'事業内容[：:]\s*([^\n]+)',
                r'会社概要[：:]\s*([^\n]+)',
            ]
            for pattern in business_patterns:
                match = re.search(pattern, body_text)
                if match:
                    detail_data['business_content'] = match.group(1).strip()
                    break

            # 仕事内容
            job_desc_patterns = [
                r'仕事内容[：:]\s*([^\n]+(?:\n[^\n]+){0,5})',
                r'業務内容[：:]\s*([^\n]+(?:\n[^\n]+){0,5})',
            ]
            for pattern in job_desc_patterns:
                match = re.search(pattern, body_text)
                if match:
                    detail_data['job_description'] = match.group(1).strip()[:500]
                    break

            # 勤務時間
            hours_patterns = [
                r'勤務時間[：:]\s*([^\n]+)',
                r'就業時間[：:]\s*([^\n]+)',
            ]
            for pattern in hours_patterns:
                match = re.search(pattern, body_text)
                if match:
                    detail_data['working_hours'] = match.group(1).strip()
                    break

            # 休日
            holiday_patterns = [
                r'休日[：:]\s*([^\n]+)',
                r'休暇[：:]\s*([^\n]+)',
            ]
            for pattern in holiday_patterns:
                match = re.search(pattern, body_text)
                if match:
                    detail_data['holidays'] = match.group(1).strip()
                    break

        except Exception as e:
            logger.error(f"[カイゴジョブ] 詳細取得エラー: {e}")

        return detail_data

    async def _extract_json_ld(self, page: Page) -> Dict[str, Any]:
        """JSON-LDから求人情報を抽出"""
        try:
            json_ld_scripts = await page.query_selector_all('script[type="application/ld+json"]')

            for script in json_ld_scripts:
                try:
                    content = await script.inner_text()
                    data = json.loads(content)

                    if isinstance(data, dict):
                        if data.get('@type') == 'JobPosting':
                            return self._parse_job_posting_ld(data)
                    elif isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict) and item.get('@type') == 'JobPosting':
                                return self._parse_job_posting_ld(item)
                except json.JSONDecodeError:
                    continue

        except Exception as e:
            logger.debug(f"[カイゴジョブ] JSON-LD抽出エラー: {e}")

        return {}

    def _parse_job_posting_ld(self, data: Dict) -> Dict[str, Any]:
        """JobPosting JSON-LDをパース"""
        result = {}

        if 'hiringOrganization' in data:
            org = data['hiringOrganization']
            if isinstance(org, dict):
                result['company_name'] = org.get('name', '')
                if 'address' in org:
                    addr = org['address']
                    if isinstance(addr, dict):
                        result['company_address'] = addr.get('streetAddress', '')
                        result['postal_code'] = addr.get('postalCode', '')

        if 'jobLocation' in data:
            loc = data['jobLocation']
            if isinstance(loc, dict):
                if 'address' in loc:
                    addr = loc['address']
                    if isinstance(addr, dict):
                        result['location'] = addr.get('streetAddress', '') or addr.get('addressLocality', '')

        if 'baseSalary' in data:
            salary = data['baseSalary']
            if isinstance(salary, dict) and 'value' in salary:
                val = salary['value']
                if isinstance(val, dict):
                    min_val = val.get('minValue')
                    max_val = val.get('maxValue')
                    result['salary'] = self._format_salary(min_val, max_val)

        if 'description' in data:
            result['job_description'] = data['description'][:500]

        return result
