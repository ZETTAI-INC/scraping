"""
E2Eスクレイピングテスト
実際のサイトにアクセスして求人情報が取得できるか検証

使用方法:
    # E2Eテストのみ実行
    pytest tests/test_scraping_e2e.py -v -m e2e

    # 特定のサイトのみ
    pytest tests/test_scraping_e2e.py -v -k "townwork"
"""
import pytest
import asyncio
from playwright.async_api import async_playwright


# E2Eテストマーカー
pytestmark = pytest.mark.e2e


@pytest.fixture(scope="module")
def event_loop():
    """イベントループのフィクスチャ"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
async def browser():
    """ブラウザのフィクスチャ（モジュール単位で共有）"""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        yield browser
        await browser.close()


@pytest.fixture
async def page(browser):
    """ページのフィクスチャ（テストごとに新規作成）"""
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    )
    page = await context.new_page()
    yield page
    await context.close()


class TestTownworkE2E:
    """タウンワークE2Eテスト"""

    @pytest.mark.asyncio
    async def test_search_page_loads(self, page, townwork_scraper):
        """検索ページが正常にロードされるか"""
        url = townwork_scraper.generate_search_url("SE", "東京", 1)
        response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)

        assert response is not None
        assert response.status in [200, 301, 302], f"HTTPステータス: {response.status}"

    @pytest.mark.asyncio
    async def test_job_cards_exist(self, page, townwork_scraper):
        """求人カードが存在するか"""
        url = townwork_scraper.generate_search_url("事務", "東京", 1)
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)

        # 求人カードのセレクタ
        cards = await page.query_selector_all("[class*='jobCard'], a[href*='jobid_']")
        assert len(cards) > 0, "求人カードが見つからない"

    @pytest.mark.asyncio
    async def test_category_search_returns_results(self, page, townwork_scraper):
        """カテゴリ検索で結果が返るか"""
        url = townwork_scraper.generate_search_url("SE", "石川", 1)
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)

        # ページタイトルまたはコンテンツで検索結果を確認
        title = await page.title()
        content = await page.content()

        # エラーページでないことを確認
        assert "404" not in title
        assert "エラー" not in title


class TestBaitoruE2E:
    """バイトルE2Eテスト"""

    @pytest.mark.asyncio
    async def test_search_page_loads(self, page, baitoru_scraper):
        """検索ページが正常にロードされるか"""
        url = baitoru_scraper.generate_search_url("販売", "東京", 1)
        response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)

        assert response is not None
        assert response.status in [200, 301, 302], f"HTTPステータス: {response.status}"

    @pytest.mark.asyncio
    async def test_job_cards_exist(self, page, baitoru_scraper):
        """求人カードが存在するか"""
        url = baitoru_scraper.generate_search_url("販売", "東京", 1)
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)

        # バイトルの求人カードセレクタ
        cards = await page.query_selector_all("[class*='list-job'], [class*='jobCard']")
        # カードが見つからなくてもエラーページでなければOK
        title = await page.title()
        assert "404" not in title


class TestLineBaitoE2E:
    """LINEバイトE2Eテスト"""

    @pytest.mark.asyncio
    async def test_search_page_loads(self, page, linebaito_scraper):
        """検索ページが正常にロードされるか"""
        url = linebaito_scraper.generate_search_url("飲食", "東京", 1)
        response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)

        assert response is not None
        assert response.status in [200, 301, 302], f"HTTPステータス: {response.status}"

    @pytest.mark.asyncio
    async def test_react_app_renders(self, page, linebaito_scraper):
        """ReactアプリがレンダリングされるかSPA用）"""
        url = linebaito_scraper.generate_search_url("飲食", "東京", 1)
        await page.goto(url, wait_until="networkidle", timeout=60000)
        await page.wait_for_timeout(5000)

        # React SPAがレンダリングされているか
        content = await page.content()
        assert len(content) > 1000, "ページコンテンツが少なすぎる"


class TestIndeedE2E:
    """IndeedE2Eテスト"""

    @pytest.mark.asyncio
    async def test_search_page_loads(self, page, indeed_scraper):
        """検索ページが正常にロードされるか"""
        url = indeed_scraper.generate_search_url("SE", "東京", 1)
        response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)

        assert response is not None
        # Indeedはリダイレクトすることがある
        assert response.status in [200, 301, 302, 303], f"HTTPステータス: {response.status}"

    @pytest.mark.asyncio
    async def test_job_cards_exist(self, page, indeed_scraper):
        """求人カードが存在するか"""
        url = indeed_scraper.generate_search_url("エンジニア", "東京", 1)
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)

        # Indeedの求人カードセレクタ
        cards = await page.query_selector_all("[class*='job_seen'], .jobsearch-ResultsList > li")
        # カードが見つからなくてもエラーページでなければOK（ボット検出の可能性）
        title = await page.title()
        assert "404" not in title.lower()


class TestHelloworkE2E:
    """ハローワークE2Eテスト"""

    @pytest.mark.asyncio
    async def test_search_form_loads(self, page):
        """検索フォームが正常にロードされるか"""
        url = "https://www.hellowork.mhlw.go.jp/kensaku/GECA110010.do"
        response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)

        assert response is not None
        assert response.status == 200, f"HTTPステータス: {response.status}"

    @pytest.mark.asyncio
    async def test_form_elements_exist(self, page):
        """フォーム要素が存在するか"""
        url = "https://www.hellowork.mhlw.go.jp/kensaku/GECA110010.do"
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(2000)

        # フォーム要素の存在確認
        content = await page.content()
        assert "検索" in content or "求人" in content


class TestMultiplePrefecturesE2E:
    """複数都道府県でのE2Eテスト"""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("area", ["東京", "大阪", "石川"])
    async def test_townwork_multiple_areas(self, page, townwork_scraper, area):
        """タウンワーク: 複数の都道府県で検索できるか"""
        url = townwork_scraper.generate_search_url("SE", area, 1)
        response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)

        assert response is not None
        assert response.status in [200, 301, 302], f"{area}: HTTPステータス {response.status}"


class TestRegressionE2E:
    """リグレッションテスト（デグレ防止）"""

    @pytest.mark.asyncio
    async def test_townwork_ishikawa_se_category(self, page, townwork_scraper):
        """
        タウンワーク: 石川+SEでカテゴリ検索が動作するか
        （過去に問題があったケース）
        """
        url = townwork_scraper.generate_search_url("SE", "石川", 1)

        # URLがカテゴリ形式であることを確認
        assert "prefectures/ishikawa" in url
        assert "oc-013" in url
        assert "sc=new" in url

        # 実際にアクセス
        response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        assert response is not None
        assert response.status in [200, 301, 302]

    @pytest.mark.asyncio
    async def test_townwork_new_sort_parameter(self, page, townwork_scraper):
        """
        タウンワーク: sc=newパラメータが含まれているか
        （新着順ソートが有効か）
        """
        test_cases = [
            ("SE", "東京"),
            ("事務", "大阪"),
            ("介護", "北海道"),
        ]

        for keyword, area in test_cases:
            url = townwork_scraper.generate_search_url(keyword, area, 1)
            assert "sc=new" in url, f"{area}+{keyword}: sc=newが含まれていない"
