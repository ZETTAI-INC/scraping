"""
ページ操作ユーティリティ
実践的な待機ロジックとセレクタ検証
"""
import asyncio
import logging
from typing import Optional, List, Dict, Any
from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

logger = logging.getLogger(__name__)


class PageUtils:
    """ページ操作ユーティリティクラス"""

    @staticmethod
    async def wait_for_page_load(
        page: Page,
        timeout: int = 15000,
        wait_for_network_idle: bool = True
    ) -> bool:
        """
        ページが完全に読み込まれるまで待機

        Args:
            page: Playwrightページ
            timeout: タイムアウト（ミリ秒）
            wait_for_network_idle: ネットワークアイドル待機

        Returns:
            成功したかどうか
        """
        try:
            # DOMContentLoadedを待つ
            await page.wait_for_load_state("domcontentloaded", timeout=timeout)

            # ネットワークアイドルを短時間待つ（2秒で打ち切り）
            if wait_for_network_idle:
                try:
                    await page.wait_for_load_state("networkidle", timeout=2000)
                except PlaywrightTimeoutError:
                    pass  # タイムアウトは無視して続行

            # 追加の待機（0.5秒）
            await asyncio.sleep(0.5)

            logger.debug("Page fully loaded")
            return True

        except PlaywrightTimeoutError:
            logger.error(f"Page load timeout after {timeout}ms")
            return False

    @staticmethod
    async def safe_goto(
        page: Page,
        url: str,
        timeout: int = 30000,
        wait_until: str = "domcontentloaded"
    ) -> bool:
        """
        安全なページ遷移（エラーハンドリング付き）

        Args:
            page: Playwrightページ
            url: 遷移先URL
            timeout: タイムアウト（ミリ秒）
            wait_until: 待機条件

        Returns:
            成功したかどうか
        """
        try:
            logger.info(f"Navigating to: {url}")
            response = await page.goto(url, wait_until=wait_until, timeout=timeout)

            if response and response.status >= 400:
                logger.warning(f"HTTP error: {response.status} for {url}")
                return False

            await PageUtils.wait_for_page_load(page, timeout)
            return True

        except PlaywrightTimeoutError:
            logger.error(f"Navigation timeout for {url}")
            return False
        except Exception as e:
            logger.error(f"Navigation error for {url}: {e}")
            return False

    @staticmethod
    async def verify_selector(
        page: Page,
        selector: str,
        timeout: int = 3000
    ) -> bool:
        """
        セレクタが存在するか検証

        Args:
            page: Playwrightページ
            selector: CSSセレクタ
            timeout: タイムアウト（ミリ秒）- 短縮済み

        Returns:
            セレクタが見つかったかどうか
        """
        try:
            # state="attached"に変更（visibleより高速）
            await page.wait_for_selector(selector, timeout=timeout, state="attached")
            logger.debug(f"Selector found: {selector}")
            return True
        except PlaywrightTimeoutError:
            logger.warning(f"Selector not found: {selector}")
            return False

    @staticmethod
    async def get_elements_count(page: Page, selector: str) -> int:
        """
        セレクタに一致する要素数を取得

        Args:
            page: Playwrightページ
            selector: CSSセレクタ

        Returns:
            要素数
        """
        try:
            elements = await page.query_selector_all(selector)
            count = len(elements)
            logger.debug(f"Found {count} elements for selector: {selector}")
            return count
        except Exception as e:
            logger.error(f"Error counting elements for {selector}: {e}")
            return 0

    @staticmethod
    async def take_screenshot(
        page: Page,
        path: str,
        full_page: bool = False
    ) -> bool:
        """
        スクリーンショットを撮影

        Args:
            page: Playwrightページ
            path: 保存先パス
            full_page: フルページスクリーンショット

        Returns:
            成功したかどうか
        """
        try:
            await page.screenshot(path=path, full_page=full_page)
            logger.info(f"Screenshot saved: {path}")
            return True
        except Exception as e:
            logger.error(f"Screenshot error: {e}")
            return False

    @staticmethod
    async def extract_text_safe(page: Page, selector: str) -> Optional[str]:
        """
        安全にテキストを抽出（エラーハンドリング付き）

        Args:
            page: Playwrightページ
            selector: CSSセレクタ

        Returns:
            抽出されたテキスト（失敗時はNone）
        """
        try:
            element = await page.query_selector(selector)
            if element:
                text = await element.inner_text()
                return text.strip() if text else None
            return None
        except Exception as e:
            logger.debug(f"Failed to extract text from {selector}: {e}")
            return None

    @staticmethod
    async def scroll_to_bottom(page: Page, delay: float = 0.5, max_scrolls: int = 10):
        """
        ページを最下部までスクロール（遅延読み込み対応）

        Args:
            page: Playwrightページ
            delay: スクロール間の遅延（秒）
            max_scrolls: 最大スクロール回数
        """
        for i in range(max_scrolls):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(delay)

            # 新しいコンテンツが読み込まれたか確認
            current_height = await page.evaluate("document.body.scrollHeight")
            await asyncio.sleep(delay)
            new_height = await page.evaluate("document.body.scrollHeight")

            if current_height == new_height:
                logger.debug(f"Reached bottom after {i+1} scrolls")
                break

    @staticmethod
    async def check_for_captcha(page: Page) -> bool:
        """
        CAPTCHAが表示されているかチェック（高速版）

        Returns:
            CAPTCHAが検出された場合True
        """
        # 並列でチェック（高速化）
        captcha_indicators = [
            "iframe[src*='recaptcha']",
            ".g-recaptcha",
            "#captcha",
        ]

        try:
            # 一度にまとめてチェック
            for indicator in captcha_indicators:
                element = await page.query_selector(indicator)
                if element:
                    logger.warning(f"CAPTCHA detected: {indicator}")
                    return True
        except Exception:
            pass

        return False

    @staticmethod
    async def check_for_block(page: Page) -> Dict[str, Any]:
        """
        ブロックされているかチェック

        Returns:
            ブロック情報の辞書
        """
        result = {
            "is_blocked": False,
            "reason": None,
            "indicators": []
        }

        try:
            # ブロック指標をチェック
            block_indicators = [
                ("Access Denied", "text"),
                ("403", "text"),
                ("404", "text"),
                ("blocked", "text"),
                ("Robot Check", "text"),
                ("Checking your browser", "text"),
            ]

            page_text = await page.inner_text("body")

            for indicator, check_type in block_indicators:
                if indicator.lower() in page_text.lower():
                    result["is_blocked"] = True
                    result["indicators"].append(indicator)
                    logger.warning(f"Block indicator detected: {indicator}")

            # CAPTCHAチェック
            if await PageUtils.check_for_captcha(page):
                result["is_blocked"] = True
                result["reason"] = "CAPTCHA"
                result["indicators"].append("CAPTCHA")

        except Exception as e:
            # ページがナビゲーション中などでエラーが出た場合は無視
            logger.debug(f"Error checking for block (likely page navigating): {e}")

        return result
