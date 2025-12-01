"""
バイトル専用スクレイパー
2024年12月版 - 新しいサイト構造に対応
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


class BaitoruScraper(BaseScraper):
    """バイトル用スクレイパー"""

    def __init__(self):
        super().__init__(site_name="baitoru")

    def get_categories_for_keyword(self, keyword: str) -> List[str]:
        """
        キーワードに対応するカテゴリのリストを取得

        Args:
            keyword: 検索キーワード

        Returns:
            カテゴリコードのリスト（見つからない場合は空リスト）
        """
        job_categories = self.site_config.get("job_categories", {})
        category = job_categories.get(keyword, None)

        if category is None:
            return []
        elif isinstance(category, list):
            return category
        else:
            return [category]

    def generate_search_url(self, keyword: str, area: str, page: int = 1, category: str = None) -> str:
        """
        バイトル用の検索URL生成

        バイトルのURL構造:
        - カテゴリなし: https://www.baitoru.com/{region}/jlist/{prefecture}/{area}/
        - カテゴリあり（新着順）: https://www.baitoru.com/{region}/jlist/{prefecture}/{area}/{category}/srt2/
        - キーワード検索（新着順）: https://www.baitoru.com/{region}/jlist/{prefecture}/{area}/wrd{keyword}/srt2/
        - ページ指定: 上記 + page{page}/

        Args:
            keyword: 職種カテゴリ名（販売, 飲食, 事務 など）またはキーワード
            area: エリア名（東京, 大阪 など）
            page: ページ番号
            category: カテゴリコード（直接指定する場合）
        """
        area_codes = self.site_config.get("area_codes", {})
        area_config = area_codes.get(area, area_codes.get("東京"))

        if isinstance(area_config, dict):
            region = area_config.get("region", "kanto")
            prefecture = area_config.get("prefecture", "tokyo")
            area_path = area_config.get("area", "23ku")
        else:
            # フォールバック: 東京23区
            region = "kanto"
            prefecture = "tokyo"
            area_path = "23ku"

        # カテゴリが直接指定されていない場合は検索
        if category is None:
            categories = self.get_categories_for_keyword(keyword)
            category = categories[0] if categories else ""

        # area_pathが空の場合は県全域検索
        area_segment = f"{area_path}/" if area_path else ""

        if category:
            # カテゴリ指定あり（新着順 srt2）
            if page == 1:
                url = f"https://www.baitoru.com/{region}/jlist/{prefecture}/{area_segment}{category}/srt2/"
            else:
                url = f"https://www.baitoru.com/{region}/jlist/{prefecture}/{area_segment}{category}/srt2/page{page}/"
        elif keyword:
            # カテゴリが見つからない場合はキーワード検索にフォールバック
            # バイトルのキーワード検索URL: https://www.baitoru.com/{region}/jlist/{prefecture}/{area}/wrd{keyword}/srt2/
            # 例: https://www.baitoru.com/kanto/jlist/tokyo/23ku/wrdweb/srt2/
            logger.info(f"Category not found for '{keyword}', using keyword search with area")
            from urllib.parse import quote
            encoded_keyword = quote(keyword, safe='')
            if page == 1:
                url = f"https://www.baitoru.com/{region}/jlist/{prefecture}/{area_segment}wrd{encoded_keyword}/srt2/"
            else:
                url = f"https://www.baitoru.com/{region}/jlist/{prefecture}/{area_segment}wrd{encoded_keyword}/srt2/page{page}/"
        else:
            # キーワードなし
            if page == 1:
                url = f"https://www.baitoru.com/{region}/jlist/{prefecture}/{area_segment}"
            else:
                url = f"https://www.baitoru.com/{region}/jlist/{prefecture}/{area_segment}page{page}/"

        logger.info(f"Generated URL: {url}")
        return url

    async def _is_pr_card(self, card, is_first: bool, is_last: bool) -> bool:
        """
        PRカード（広告）かどうかを判定

        PRカードの法則:
        1. 最初のカード（list-listingの直後）
        2. DIV（list-infoArea）の直後のカード
        3. 中間のlist-listingの直後のカード
        4. 最後のカード

        判定方法: 前の兄弟要素がlist-jobListDetailでなければPR
        """
        # 最初と最後はPR
        if is_first or is_last:
            return True

        # 前の兄弟要素をチェック
        try:
            prev_info = await card.evaluate("""(el) => {
                const prev = el.previousElementSibling;
                if (!prev) return { hasPrev: false };
                return {
                    hasPrev: true,
                    isJobDetail: prev.classList.contains('list-jobListDetail'),
                    tagName: prev.tagName,
                    className: prev.className
                };
            }""")

            if not prev_info.get('hasPrev'):
                return True  # 前の要素がない場合はPR扱い

            # 前の要素がlist-jobListDetailでなければPR
            if not prev_info.get('isJobDetail'):
                return True

        except Exception as e:
            logger.warning(f"Error checking PR status: {e}")

        return False

    async def _extract_card_data(self, card) -> Optional[Dict[str, Any]]:
        """
        求人カードからデータを抽出
        """
        try:
            data = {}

            # 詳細ページへのリンク
            link_elem = await card.query_selector(".pt02b .ul01 .li01 h3 a")
            if link_elem:
                href = await link_elem.get_attribute("href")
                if href:
                    if href.startswith("/"):
                        href = f"https://www.baitoru.com{href}"
                    data["page_url"] = href

                    # 求人IDを抽出 (例: job151221507)
                    match = re.search(r"job(\d+)", href)
                    if match:
                        data["job_number"] = match.group(1)

            # タイトル
            title_elem = await card.query_selector(".pt02b .ul01 .li01 h3 a span")
            if title_elem:
                title_text = await title_elem.inner_text()
                data["title"] = title_text.strip() if title_text else ""

            # 会社名
            company_elem = await card.query_selector(".pt02b > p")
            if company_elem:
                company_text = await company_elem.inner_text()
                data["company_name"] = company_text.strip() if company_text else ""

            # 給与
            salary_elem = await card.query_selector(".pt03 dl:nth-child(2) dd li em")
            if salary_elem:
                salary_text = await salary_elem.inner_text()
                data["salary"] = salary_text.strip() if salary_text else ""

            # 勤務地
            location_elem = await card.query_selector(".pt02b .ul02 li")
            if location_elem:
                location_text = await location_elem.inner_text()
                # "[勤務地・面接地] " プレフィックスを除去
                location_text = re.sub(r"^\[勤務地.*?\]\s*", "", location_text.strip())
                data["location"] = location_text

            # 雇用形態
            employment_elem = await card.query_selector(".pt01a .ul01 li:first-child")
            if employment_elem:
                employment_text = await employment_elem.inner_text()
                data["employment_type"] = employment_text.strip() if employment_text else ""

            # 職種
            job_type_elem = await card.query_selector(".pt03 dl:first-child dd li")
            if job_type_elem:
                job_type_text = await job_type_elem.inner_text()
                # [ア・パ] などのプレフィックスを除去
                job_type_text = re.sub(r"^\[.*?\]\s*", "", job_type_text.strip())
                data["job_type"] = job_type_text

            # 勤務時間
            hours_elem = await card.query_selector(".pt03 dl:nth-child(3) dd li")
            if hours_elem:
                hours_text = await hours_elem.inner_text()
                # [ア・パ] などのプレフィックスを除去
                hours_text = re.sub(r"^\[.*?\]\s*", "", hours_text.strip())
                data["working_hours"] = hours_text

            # 特徴タグ
            tags = []
            tag_elems = await card.query_selector_all(".pt04 ul li em")
            for tag_elem in tag_elems:
                tag_text = await tag_elem.inner_text()
                if tag_text:
                    tags.append(tag_text.strip())
            if tags:
                data["tags"] = ", ".join(tags)

            # 仕事番号
            job_no_elem = await card.query_selector(".pt09 .p06")
            if job_no_elem:
                job_no_text = await job_no_elem.inner_text()
                data["job_number_display"] = job_no_text.strip() if job_no_text else ""

            return data if data.get("page_url") else None

        except Exception as e:
            logger.error(f"Error extracting card data: {e}")
            return None

    async def search_jobs(self, page: Page, keyword: str, area: str, max_pages: int = 5) -> List[Dict[str, Any]]:
        """
        求人検索を実行し、結果を返す
        複数カテゴリがある場合は、各カテゴリで検索を実行
        """
        all_jobs = []

        # キーワードに対応するカテゴリのリストを取得
        categories = self.get_categories_for_keyword(keyword)

        # カテゴリがない場合はキーワード検索（カテゴリとしてNoneを指定）
        if not categories:
            categories = [None]
            logger.info(f"No categories found for '{keyword}', will use keyword search")
        else:
            logger.info(f"Found {len(categories)} categories for '{keyword}': {categories}")

        for category in categories:
            category_jobs = await self._search_category(page, keyword, area, category, max_pages)
            all_jobs.extend(category_jobs)

            # 複数カテゴリの場合は待機
            if len(categories) > 1 and category != categories[-1]:
                import random
                await page.wait_for_timeout(random.randint(2000, 4000))

        return all_jobs

    async def _search_category(self, page: Page, keyword: str, area: str, category: str, max_pages: int) -> List[Dict[str, Any]]:
        """
        単一カテゴリの検索を実行
        """
        all_jobs = []
        has_next_page = True

        for page_num in range(1, max_pages + 1):
            if not has_next_page:
                logger.info(f"No more pages available, stopping at page {page_num - 1}")
                break

            url = self.generate_search_url(keyword, area, page_num, category=category)
            logger.info(f"Fetching page {page_num}: {url}")

            success = False
            for attempt in range(2):
                try:
                    response = await page.goto(
                        url,
                        wait_until="domcontentloaded",
                        timeout=30000 if attempt == 0 else 40000
                    )

                    if response and response.status == 404:
                        logger.warning(f"Page not found: {url}")
                        has_next_page = False
                        break

                    # ページ読み込み待機
                    await page.wait_for_timeout(2000)

                    # 「該当する求人がありません」をチェック
                    no_results = await page.evaluate("""() => {
                        const body = document.body.innerText;
                        return body.includes('該当する求人がありません') ||
                               body.includes('条件に合う求人がありませんでした') ||
                               body.includes('お探しの求人は見つかりませんでした') ||
                               body.includes('検索結果はありません');
                    }""")

                    if no_results:
                        logger.info(f"No results page detected on page {page_num}")
                        has_next_page = False
                        success = True
                        break

                    card_selector = self.selectors.get("job_cards", "article.list-jobListDetail")

                    # カードが描画されるまで待機
                    selector_ready = False
                    max_attempts = 4 if page_num == 1 else 2  # 1ページ目は多めにリトライ
                    for sel_attempt in range(max_attempts):
                        try:
                            await page.wait_for_selector(card_selector, timeout=5000)
                            selector_ready = True
                            break
                        except PlaywrightTimeoutError:
                            logger.warning(
                                f"Job cards selector timeout on page {page_num} (attempt {sel_attempt + 1}/{max_attempts})"
                            )
                            await page.wait_for_timeout(1000)

                    if not selector_ready:
                        if page_num == 1:
                            # 1ページ目でセレクタが見つからない場合はリトライ
                            logger.warning(f"Job cards not found on page 1, will retry")
                            if attempt == 0:
                                continue
                        # 2ページ目以降はページが存在しない可能性が高い
                        logger.info(f"Job cards not found on page {page_num}, assuming no more pages")
                        has_next_page = False
                        success = True
                        break

                    # 求人カードを取得（ページネーションより上のもののみ）
                    # ページネーション要素より前のカードだけを取得するJavaScript
                    job_cards_above_pagination = await page.evaluate("""() => {
                        const cards = document.querySelectorAll('article.list-jobListDetail');
                        const pagination = document.querySelector('.list-pager, .pagination, nav.pager');

                        if (!pagination) {
                            // ページネーションがない場合は全カードを返す
                            return Array.from(cards).map((_, i) => i);
                        }

                        const paginationTop = pagination.getBoundingClientRect().top;
                        const validIndices = [];

                        cards.forEach((card, index) => {
                            const cardTop = card.getBoundingClientRect().top;
                            if (cardTop < paginationTop) {
                                validIndices.push(index);
                            }
                        });

                        return validIndices;
                    }""")

                    all_cards = await page.query_selector_all(card_selector)

                    # ページネーションより上のカードのみをフィルタリング
                    if job_cards_above_pagination:
                        job_cards = [all_cards[i] for i in job_cards_above_pagination if i < len(all_cards)]
                    else:
                        job_cards = all_cards

                    if len(job_cards) == 0:
                        await page.wait_for_timeout(1000)
                        all_cards = await page.query_selector_all(card_selector)
                        job_cards = all_cards

                    if len(job_cards) == 0:
                        logger.info(f"No job cards found on page {page_num}, assuming end of results")
                        has_next_page = False
                        success = True
                        break

                    logger.info(f"Found {len(job_cards)} jobs on page {page_num} (filtered above pagination)")

                    # PRカードをスキップ
                    pr_count = 0
                    total_cards = len(job_cards)
                    for idx, card in enumerate(job_cards):
                        is_first = (idx == 0)
                        is_last = (idx == total_cards - 1)

                        try:
                            if await self._is_pr_card(card, is_first, is_last):
                                pr_count += 1
                                continue

                            job_data = await self._extract_card_data(card)
                            if job_data:
                                all_jobs.append(job_data)
                        except Exception as e:
                            logger.error(f"Error extracting job card: {e}")
                            continue

                    if pr_count > 0:
                        logger.info(f"Skipped {pr_count} PR card(s)")

                    # 次のページが存在するかチェック
                    has_next_page = await page.evaluate("""(currentPageNum) => {
                        // 「次へ」テキストを含むリンクがあるか確認
                        const allLinks = document.querySelectorAll('a');
                        for (const link of allLinks) {
                            if (link.textContent.trim() === '次へ' || link.textContent.includes('次のページ')) {
                                return true;
                            }
                        }

                        // ページ番号リンクで現在より大きいページがあるか確認
                        // バイトルのページネーション: div.pager内のaタグ
                        const pageLinks = document.querySelectorAll('.pager a, .list-pager a, nav a, [class*="pager"] a, [class*="pagination"] a');
                        for (const link of pageLinks) {
                            const text = link.textContent.trim();
                            const pageNum = parseInt(text, 10);
                            if (!isNaN(pageNum) && pageNum > currentPageNum) {
                                return true;
                            }
                        }

                        // 最後のチェック: ページ番号テキストを含む要素全体から検索
                        const bodyText = document.body.innerText;
                        const match = bodyText.match(/(\d+)件中/);
                        if (match) {
                            // 結果件数が20件以上ある場合は次ページがある可能性
                            const totalCount = parseInt(match[1], 10);
                            if (totalCount > currentPageNum * 20) {
                                return true;
                            }
                        }

                        return false;
                    }""", page_num)

                    if not has_next_page:
                        logger.info(f"No next page link found, page {page_num} is the last page")

                    success = True
                    break

                except Exception as e:
                    logger.error(f"Error fetching page {page_num} (attempt {attempt + 1}/2): {e}")
                    if attempt == 0:
                        await page.wait_for_timeout(1500)
                        continue
                    else:
                        break

            if not success:
                break

            # ページ間の待機
            await page.wait_for_timeout(500)

        return all_jobs

    async def extract_detail_info(self, page: Page, url: str) -> Dict[str, Any]:
        """
        詳細ページから追加情報を取得

        取得項目:
        - 郵便番号
        - 住所詳細
        - 電話番号
        - 会社名
        - 事業内容
        - 仕事内容
        - 勤務時間
        - 休日休暇
        - 応募資格
        - 掲載日
        - 雇用形態
        """
        detail_data = {}

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

            # セレクタから各種情報を取得
            try:
                # 雇用形態
                emp_elem = await page.query_selector(".job-type, .employment-type, [class*='employment'], [class*='jobtype']")
                if emp_elem:
                    emp_text = await emp_elem.inner_text()
                    if emp_text:
                        detail_data["employment_type"] = emp_text.strip()
            except Exception:
                pass

            # 住所をセレクタから取得
            try:
                addr_selectors = [
                    "td:has-text('住所') + td",
                    "th:has-text('住所') + td",
                    "[class*='address']",
                    "[class*='location']",
                    "dd:has(dt:has-text('勤務地'))",
                ]
                for sel in addr_selectors:
                    try:
                        addr_elem = await page.query_selector(sel)
                        if addr_elem:
                            addr_text = await addr_elem.inner_text()
                            if addr_text and len(addr_text) > 3:
                                detail_data["address"] = addr_text.strip()[:200]
                                break
                    except Exception:
                        continue
            except Exception:
                pass

            # 「電話番号を表示する」ボタンをクリックして電話番号を取得
            try:
                # ボタンを探してクリック
                phone_button_selectors = [
                    "button:has-text('電話番号を表示')",
                    "a:has-text('電話番号を表示')",
                    "[class*='phone'] button",
                    "[class*='tel'] button",
                    "button:has-text('電話番号')",
                    ".showTel",
                    "[data-action*='phone']",
                    "[onclick*='phone']",
                ]

                phone_button_clicked = False
                for btn_sel in phone_button_selectors:
                    try:
                        phone_btn = await page.query_selector(btn_sel)
                        if phone_btn:
                            await phone_btn.click()
                            phone_button_clicked = True
                            logger.debug(f"Clicked phone button with selector: {btn_sel}")
                            # ボタンクリック後、電話番号が表示されるまで待機
                            await page.wait_for_timeout(1000)
                            break
                    except Exception as e:
                        logger.debug(f"Failed to click phone button {btn_sel}: {e}")
                        continue

                # 電話番号をセレクタから取得
                tel_selectors = [
                    "a[href^='tel:']",
                    "td:has-text('TEL') + td",
                    "th:has-text('電話') + td",
                    "[class*='tel']",
                    "[class*='phone']",
                    ".telNumber",
                    "[class*='telNum']",
                ]
                for sel in tel_selectors:
                    try:
                        tel_elem = await page.query_selector(sel)
                        if tel_elem:
                            tel_text = await tel_elem.inner_text()
                            # tel:リンクの場合はhrefから取得
                            if not tel_text:
                                tel_text = await tel_elem.get_attribute("href")
                                if tel_text:
                                    tel_text = tel_text.replace("tel:", "")
                            if tel_text:
                                # 数字のみ抽出
                                phone_raw = re.sub(r"[^\d]", "", tel_text)
                                # 有効な電話番号かチェック（0120除外、マスク番号除外）
                                if len(phone_raw) >= 9 and not phone_raw.startswith("0500000") and not phone_raw.startswith("0120"):
                                    detail_data["phone"] = phone_raw
                                    detail_data["phone_number_normalized"] = phone_raw
                                    logger.debug(f"Found phone number: {phone_raw}")
                                    break
                    except Exception:
                        continue
            except Exception as e:
                logger.debug(f"Error getting phone number: {e}")

            body_text = await page.inner_text("body")

            # 雇用形態をテキストから抽出（セレクタで取得できなかった場合）
            if not detail_data.get("employment_type"):
                emp_match = re.search(r"(雇用形態|勤務形態)[：:\s]*[\n\r]*(.+?)(?=\n|$)", body_text)
                if emp_match:
                    detail_data["employment_type"] = emp_match.group(2).strip()[:50]

            # 住所の抽出（複数パターン対応）
            # パターン1: 郵便番号付き
            postal_match = re.search(r"〒?(\d{3})-?(\d{4})\s*(東京都|大阪府|北海道|京都府|.{2,3}県)(.+?)(?=\n|交通|地図|※|アクセス|TEL|電話)", body_text)
            if postal_match:
                detail_data["postal_code"] = postal_match.group(1) + "-" + postal_match.group(2)
                detail_data["address"] = postal_match.group(3) + postal_match.group(4).strip()
            else:
                # パターン2: 「住所」ラベルの後
                addr_match = re.search(r"住所[：:\s]*[\n\r]*(東京都|大阪府|北海道|京都府|.{2,3}県)(.{0,100}?)(?=\n|交通|地図|※|アクセス|最寄|$)", body_text)
                if addr_match:
                    detail_data["address"] = addr_match.group(1) + addr_match.group(2).strip()
                else:
                    # パターン3: 「勤務地」の後に都道府県
                    addr_match2 = re.search(r"勤務地[：:\s]*[\n\r]*(東京都|大阪府|北海道|京都府|.{2,3}県)(.{0,100}?)(?=\n|交通|地図|※|アクセス|最寄|$)", body_text)
                    if addr_match2:
                        detail_data["address"] = addr_match2.group(1) + addr_match2.group(2).strip()
                    else:
                        # パターン4: 所在地
                        addr_match3 = re.search(r"所在地[：:\s]*[\n\r]*(東京都|大阪府|北海道|京都府|.{2,3}県)(.{0,100}?)(?=\n|$)", body_text)
                        if addr_match3:
                            detail_data["address"] = addr_match3.group(1) + addr_match3.group(2).strip()

            # 電話番号の抽出（複数パターン対応）
            phone_patterns = [
                r"TEL[：:\s]*(\d{2,4}[-ー]?\d{2,4}[-ー]?\d{3,4})",  # TEL:形式
                r"電話番号[：:\s]*(\d{2,4}[-ー]?\d{2,4}[-ー]?\d{3,4})",  # 電話番号:形式
                r"連絡先[：:\s]*(\d{2,4}[-ー]?\d{2,4}[-ー]?\d{3,4})",  # 連絡先:形式
                r"tel[：:\s]*(\d{2,4}[-ー]?\d{2,4}[-ー]?\d{3,4})",  # 小文字tel
                r"(?:問[い合わせ]*|お問合せ)[：:\s]*(\d{2,4}[-ー]?\d{2,4}[-ー]?\d{3,4})",  # お問い合わせ
            ]

            for pattern in phone_patterns:
                phone_match = re.search(pattern, body_text, re.IGNORECASE)
                if phone_match:
                    phone_raw = phone_match.group(1).replace("-", "").replace("ー", "")
                    # 0120やマスクされた番号（050-0000-0000）は除外
                    if not phone_raw.startswith("0500000") and len(phone_raw) >= 9:
                        detail_data["phone"] = phone_raw
                        # 正規化された電話番号も保存
                        detail_data["phone_number_normalized"] = phone_raw
                        break

            # 会社名
            company_match = re.search(r"会社名\s*[\n\r]*(.+?)(?=\n|$)", body_text)
            if company_match:
                detail_data["company_name"] = company_match.group(1).strip()
            else:
                # 別のパターン：企業名、店舗名
                company_match2 = re.search(r"(企業名|店舗名|事業所名)\s*[\n\r]*(.+?)(?=\n|$)", body_text)
                if company_match2:
                    detail_data["company_name"] = company_match2.group(2).strip()

            # 事業内容
            business_match = re.search(r"事業内容\s*[\n\r]*(.+?)(?=\n所在|従業員|設立|$)", body_text)
            if business_match:
                detail_data["business_content"] = business_match.group(1).strip()[:300]

            # 仕事内容
            desc_match = re.search(r"仕事内容\s*[\n\r]*(.+?)(?=\n勤務地|\n給与|\n待遇|$)", body_text, re.DOTALL)
            if desc_match:
                detail_data["job_description"] = desc_match.group(1).strip()[:500]

            # 勤務時間
            time_match = re.search(r"勤務時間\s*[\n\r]*(.+?)(?=\n休日|\n給与|\n待遇|$)", body_text)
            if time_match:
                detail_data["working_hours"] = time_match.group(1).strip()[:200]

            # 休日休暇
            holiday_match = re.search(r"休日[・休暇]*\s*[\n\r]*(.+?)(?=\n待遇|\n福利|\n応募|$)", body_text)
            if holiday_match:
                detail_data["holidays"] = holiday_match.group(1).strip()[:200]

            # 応募資格
            qualification_match = re.search(r"(応募資格|対象となる方|歓迎)\s*[\n\r]*(.+?)(?=\n待遇|\n勤務|\n給与|$)", body_text, re.DOTALL)
            if qualification_match:
                detail_data["qualifications"] = qualification_match.group(2).strip()[:300]

            # 掲載日（複数パターン対応）
            published_patterns = [
                # 掲載開始日：2025-12-01 形式
                r"掲載開始日[：:\s]*_?(\d{4}[-/]\d{1,2}[-/]\d{1,2})_?",
                # 更新日時　2025/12/1（月）16:00 形式
                r"更新日時[：:\s]*(\d{4}[/\-]\d{1,2}[/\-]\d{1,2})",
                # 掲載期間：2025/12/01～ 形式
                r"掲載期間[：:\s]*(\d{4}[/\-年]\d{1,2}[/\-月]\d{1,2})",
                # 更新日：2025年12月1日 形式
                r"更新日[：:\s]*(\d{4}年\d{1,2}月\d{1,2}日?)",
                # 登録日：2025/12/01 形式
                r"登録日[：:\s]*(\d{4}[/\-]\d{1,2}[/\-]\d{1,2})",
                # 掲載開始：2025年12月1日 形式
                r"掲載開始[：:\s]*(\d{4}年\d{1,2}月\d{1,2}日?)",
            ]

            for pattern in published_patterns:
                published_match = re.search(pattern, body_text)
                if published_match:
                    date_str = published_match.group(1)
                    # 日付形式を統一（YYYY/MM/DD）
                    date_str = date_str.replace("年", "/").replace("月", "/").replace("日", "").replace("-", "/")
                    detail_data["published_date"] = date_str
                    break

            # 求人番号
            job_id_match = re.search(r"(求人番号|お仕事No|原稿ID)[：:\s]*([A-Za-z0-9\-]+)", body_text)
            if job_id_match:
                detail_data["job_number"] = job_id_match.group(2)

        except Exception as e:
            logger.error(f"Error extracting detail info from {url}: {e}")

        return detail_data

    async def scrape_with_details(self, page: Page, keyword: str, area: str,
                                   max_pages: int = 5, fetch_details: bool = True) -> List[Dict[str, Any]]:
        """
        求人検索と詳細情報取得を実行
        """
        jobs = await self.search_jobs(page, keyword, area, max_pages)

        if not fetch_details:
            return jobs

        for i, job in enumerate(jobs):
            if job.get("page_url"):
                logger.info(f"Fetching detail {i+1}/{len(jobs)}: {job['page_url']}")
                try:
                    detail_data = await self.extract_detail_info(page, job["page_url"])
                    job.update(detail_data)
                    await page.wait_for_timeout(1000)
                except Exception as e:
                    logger.error(f"Error fetching detail for job {i+1}: {e}")

        return jobs

    async def scrape_single_page(
        self,
        browser: Browser,
        keyword: str,
        area: str,
        page_num: int,
        task_idx: int = 0
    ) -> List[Dict[str, Any]]:
        """
        バイトル用: 1ページを並列用にスクレイピング
        """
        stagger_delay = task_idx * 1.5 + random.uniform(0.5, 1.5)
        logger.info(f"[タスク{task_idx+1}] {stagger_delay:.1f}秒後に開始...")
        await asyncio.sleep(stagger_delay)

        user_agent = ua_rotator.get_random()

        proxy_config = None
        if proxy_rotator.is_enabled():
            proxy = proxy_rotator.get_random()
            if proxy:
                proxy_config = proxy.to_playwright_format()

        context = await create_stealth_context(
            browser,
            user_agent=user_agent,
            proxy=proxy_config
        )

        jobs = []
        try:
            page = await context.new_page()
            await StealthConfig.apply_stealth_scripts(page)

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
        バイトル: 1ページ分の求人を取得する実装
        """
        jobs = []
        url = self.generate_search_url(keyword, area, page_num)
        logger.info(f"Fetching page {page_num}: {url}")

        for attempt in range(2):
            try:
                response = await page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=60000
                )
                await page.wait_for_timeout(2000)

                if response and response.status == 404:
                    logger.warning(f"Page not found: {url}")
                    return jobs

                if response and response.status == 403:
                    logger.error(f"Access blocked (403): {url}")
                    return jobs

                card_selector = self.selectors.get("job_cards", "article.list-jobListDetail")

                selector_ready = False
                for sel_attempt in range(4):
                    try:
                        await page.wait_for_selector(card_selector, timeout=3000 + 500 * sel_attempt)
                        selector_ready = True
                        break
                    except PlaywrightTimeoutError:
                        logger.warning(
                            f"Job cards selector timeout on page {page_num} (attempt {sel_attempt + 1}/4)"
                        )
                        await page.wait_for_timeout(600 + 200 * sel_attempt)

                if not selector_ready:
                    logger.warning(f"Job cards selector not ready on page {page_num}")
                    if attempt == 0:
                        continue

                job_cards = await page.query_selector_all(card_selector)

                if len(job_cards) == 0:
                    await page.wait_for_timeout(1000)
                    job_cards = await page.query_selector_all(card_selector)

                if len(job_cards) == 0:
                    logger.warning(f"No job cards found on page {page_num}")
                    if attempt == 0:
                        await page.wait_for_timeout(1200)
                        continue
                    else:
                        return jobs

                logger.info(f"Found {len(job_cards)} jobs on page {page_num}")

                # キーワード検索（/kw/）の場合はPRスキップなし
                # カテゴリ検索の場合はPRカードをスキップ
                is_keyword_search = "/kw/" in url
                if is_keyword_search:
                    logger.info("Keyword search - not skipping PR cards")
                    for card in job_cards:
                        try:
                            job_data = await self._extract_card_data(card)
                            if job_data:
                                jobs.append(job_data)
                        except Exception as e:
                            logger.error(f"Error extracting job card: {e}")
                            continue
                else:
                    # カテゴリ検索: PRカードをスキップ
                    pr_count = 0
                    total_cards = len(job_cards)
                    for idx, card in enumerate(job_cards):
                        is_first = (idx == 0)
                        is_last = (idx == total_cards - 1)

                        try:
                            if await self._is_pr_card(card, is_first, is_last):
                                pr_count += 1
                                continue

                            job_data = await self._extract_card_data(card)
                            if job_data:
                                jobs.append(job_data)
                        except Exception as e:
                            logger.error(f"Error extracting job card: {e}")
                            continue

                    if pr_count > 0:
                        logger.info(f"Category search - skipped {pr_count} PR card(s)")

                return jobs

            except Exception as e:
                logger.error(f"Error fetching page {page_num} (attempt {attempt + 1}/2): {e}")
                if attempt == 0:
                    await page.wait_for_timeout(1500)
                    continue
                else:
                    return jobs

        return jobs
