"""
タウンワークのページ構造を調査するスクリプト
おすすめ求人と検索結果の違いを特定する
"""
import asyncio
from playwright.async_api import async_playwright


async def debug_townwork():
    """タウンワークのHTML構造を調査"""

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        # 徳島 + コールセンター で検索
        url = "https://townwork.net/prefectures/tokushima/job_search/?keyword=%E3%82%B3%E3%83%BC%E3%83%AB%E3%82%BB%E3%83%B3%E3%82%BF%E3%83%BC&sort=1"
        print(f"アクセス中: {url}")

        await page.goto(url, wait_until="networkidle", timeout=60000)
        await asyncio.sleep(3)

        # ページ全体のHTMLを取得して構造を確認
        print("\n=== ページ構造の調査 ===\n")

        # 1. おすすめ求人セクションを探す
        recommend_sections = await page.query_selector_all("[class*='recommend'], [class*='Recommend'], [class*='pickup'], [class*='Pickup']")
        print(f"おすすめ/ピックアップセクション: {len(recommend_sections)}件")

        # 2. 検索結果セクションを探す
        search_result_sections = await page.query_selector_all("[class*='searchResult'], [class*='SearchResult'], [class*='jobList'], [class*='JobList']")
        print(f"検索結果セクション: {len(search_result_sections)}件")

        # 3. 全てのjobCardを取得
        all_job_cards = await page.query_selector_all("[class*='jobCard']")
        print(f"\n全jobCard: {len(all_job_cards)}件")

        # 4. 各カードの親要素のクラスを確認
        print("\n=== 各カードの親要素を確認 ===")
        for i, card in enumerate(all_job_cards[:10]):
            # カード自身のクラス
            card_class = await card.get_attribute("class")

            # 親要素のクラス
            parent = await card.evaluate("el => el.parentElement ? el.parentElement.className : 'no-parent'")
            grandparent = await card.evaluate("el => el.parentElement?.parentElement ? el.parentElement.parentElement.className : 'no-grandparent'")

            # リンク先URL
            href = await card.get_attribute("href")
            if not href:
                link = await card.query_selector("a[href*='jobid']")
                if link:
                    href = await link.get_attribute("href")

            print(f"\nカード {i+1}:")
            print(f"  クラス: {card_class[:80] if card_class else 'N/A'}...")
            print(f"  親: {parent[:80] if parent else 'N/A'}...")
            print(f"  祖父母: {grandparent[:80] if grandparent else 'N/A'}...")
            print(f"  URL: {href[:80] if href else 'N/A'}...")

        # 5. sectionやdivの構造を確認
        print("\n=== セクション構造の確認 ===")
        sections = await page.query_selector_all("section, [class*='Section']")
        for i, section in enumerate(sections[:10]):
            section_class = await section.get_attribute("class")
            cards_in_section = await section.query_selector_all("[class*='jobCard']")
            print(f"セクション {i+1}: クラス={section_class[:60] if section_class else 'N/A'}... カード数={len(cards_in_section)}")

        # 6. aria-labelやdata属性を確認
        print("\n=== aria-label/data属性の確認 ===")
        labeled_elements = await page.query_selector_all("[aria-label], [data-testid], [data-cy]")
        for elem in labeled_elements[:15]:
            aria = await elem.get_attribute("aria-label")
            testid = await elem.get_attribute("data-testid")
            elem_class = await elem.get_attribute("class")
            if aria or testid:
                print(f"  aria-label={aria}, data-testid={testid}, class={elem_class[:40] if elem_class else 'N/A'}...")

        # スクリーンショット保存
        await page.screenshot(path="debug_townwork_screenshot.png", full_page=True)
        print("\n📸 スクリーンショット保存: debug_townwork_screenshot.png")

        # 10秒待機して確認
        print("\n10秒後にブラウザを閉じます...")
        await asyncio.sleep(10)

        await browser.close()


if __name__ == "__main__":
    asyncio.run(debug_townwork())
