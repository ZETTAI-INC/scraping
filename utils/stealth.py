"""
ヘッドレスブラウザ検出回避（Stealth設定）
"""
from typing import Dict, Any
from playwright.async_api import BrowserContext, Page
import logging
import random

logger = logging.getLogger(__name__)

# リアルなUser-Agentのリスト
REAL_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]


class StealthConfig:
    """Stealth設定マネージャー"""

    @staticmethod
    async def apply_stealth_scripts(page: Page):
        """
        ページにステルススクリプトを適用
        ヘッドレスモード検出を回避
        """
        # WebDriverプロパティを隠蔽
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        # Chrome特有のプロパティを追加
        await page.add_init_script("""
            window.chrome = {
                runtime: {}
            };
        """)

        # Permissions APIの上書き
        await page.add_init_script("""
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
        """)

        # プラグインの追加
        await page.add_init_script("""
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
        """)

        # 言語設定
        await page.add_init_script("""
            Object.defineProperty(navigator, 'languages', {
                get: () => ['ja-JP', 'ja', 'en-US', 'en']
            });
        """)

        logger.debug("Stealth scripts applied")

    @staticmethod
    def get_browser_context_args() -> Dict[str, Any]:
        """
        ブラウザコンテキストの設定を取得
        より自然なブラウザに見せる
        """
        return {
            "viewport": {"width": 1920, "height": 1080},
            "locale": "ja-JP",
            "timezone_id": "Asia/Tokyo",
            "permissions": ["geolocation"],
            "geolocation": {"latitude": 35.6762, "longitude": 139.6503},  # 東京
            "color_scheme": "light",
            "reduced_motion": "no-preference",
            "forced_colors": "none",
        }

    @staticmethod
    def get_launch_args() -> Dict[str, Any]:
        """
        ブラウザ起動時の引数を取得
        検出回避のための設定
        """
        return {
            "headless": True,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-web-security",
                "--disable-features=IsolateOrigins,site-per-process",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-gpu",
                "--window-size=1920,1080",
                "--start-maximized",
                "--disable-infobars",
                "--disable-notifications",
                "--disable-popup-blocking",
            ]
        }


async def create_stealth_context(browser, user_agent: str = None, proxy: Dict = None, block_resources: bool = True) -> BrowserContext:
    """
    Stealth設定を適用したブラウザコンテキストを作成

    Args:
        browser: Playwrightブラウザインスタンス
        user_agent: カスタムUser-Agent（オプション）
        proxy: プロキシ設定（オプション）
        block_resources: 画像・動画等のリソースをブロックするか（デフォルト: True）

    Returns:
        設定済みのBrowserContext
    """
    context_args = StealthConfig.get_browser_context_args()

    # User-Agentが指定されていない場合はリアルなものをランダムに選択
    if user_agent:
        context_args["user_agent"] = user_agent
    else:
        context_args["user_agent"] = random.choice(REAL_USER_AGENTS)
        logger.info(f"Using User-Agent: {context_args['user_agent'][:50]}...")

    if proxy:
        context_args["proxy"] = proxy

    # 追加のHTTPヘッダーを設定（ボット検出回避）
    context_args["extra_http_headers"] = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }

    context = await browser.new_context(**context_args)

    # 全ページにステルススクリプトを適用
    context.on("page", lambda page: StealthConfig.apply_stealth_scripts(page))

    # 画像・動画・フォント等のリソースをブロック（軽量化）
    if block_resources:
        async def setup_route_blocking(page: Page):
            """不要なリソースをブロックするルートを設定"""
            # stylesheetはブロックしない（CSSクラス名のセレクタに必要）
            blocked_types = {'image', 'media', 'font'}
            blocked_extensions = {'.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg', '.ico',
                                  '.mp4', '.webm', '.avi', '.mov', '.mp3', '.wav',
                                  '.woff', '.woff2', '.ttf', '.otf', '.eot'}

            async def block_resources_handler(route):
                request = route.request
                resource_type = request.resource_type
                url = request.url.lower()

                # リソースタイプでブロック
                if resource_type in blocked_types:
                    await route.abort()
                    return

                # 拡張子でブロック
                for ext in blocked_extensions:
                    if url.endswith(ext) or f'{ext}?' in url:
                        await route.abort()
                        return

                # 広告・トラッキングURLをブロック
                blocked_domains = ['google-analytics.com', 'googletagmanager.com',
                                   'doubleclick.net', 'facebook.net', 'twitter.com/i/']
                for domain in blocked_domains:
                    if domain in url:
                        await route.abort()
                        return

                await route.continue_()

            await page.route('**/*', block_resources_handler)

        # 新しいページが作成されたときにルートを設定
        async def on_page_created(page: Page):
            await setup_route_blocking(page)

        context.on("page", lambda page: page.context.browser.contexts)  # ダミー（非同期対応のため）

        # コンテキストにフラグを設定（後でページ作成時に使用）
        context._block_resources = True
        context._setup_route_blocking = setup_route_blocking

        logger.info("Resource blocking enabled (images, media, fonts)")

    logger.info("Stealth context created")
    return context
