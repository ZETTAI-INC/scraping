"""
タウンワーク スクレイパー実行スクリプト
Streamlitを使わずに直接実行

使用方法:
  py -3.11 run_scraper.py [キーワード] [地域] [最大ページ数]

例:
  py -3.11 run_scraper.py コールセンター 徳島 3
"""
import asyncio
import sys
from playwright.async_api import async_playwright
from scrapers.townwork import TownworkScraper
import pandas as pd
from datetime import datetime
from pathlib import Path


async def main():
    print("=" * 60)
    print("タウンワーク スクレイパー")
    print("=" * 60)

    # コマンドライン引数から検索条件を取得
    keyword = sys.argv[1] if len(sys.argv) > 1 else "コールセンター"
    area = sys.argv[2] if len(sys.argv) > 2 else "徳島"
    max_pages = int(sys.argv[3]) if len(sys.argv) > 3 else 3

    print(f"\n検索条件: キーワード={keyword}, 地域={area}, 最大ページ数={max_pages}")
    print("\nスクレイピングを開始します...\n")

    # スクレイパー初期化
    scraper = TownworkScraper()

    async with async_playwright() as p:
        # ブラウザ起動（表示モード）
        browser = await p.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled"]
        )

        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        page = await context.new_page()

        try:
            # 検索実行
            jobs = await scraper.search_jobs(page, keyword, area, max_pages)

            print(f"\n取得件数: {len(jobs)} 件")

            if jobs:
                # 結果を表示
                print("\n" + "=" * 60)
                print("取得した求人一覧")
                print("=" * 60)

                for i, job in enumerate(jobs[:10], 1):
                    print(f"\n{i}. {job.get('title', 'N/A')}")
                    print(f"   会社: {job.get('company_name', 'N/A')}")
                    print(f"   給与: {job.get('salary', 'N/A')}")
                    print(f"   URL: {job.get('page_url', 'N/A')}")

                if len(jobs) > 10:
                    print(f"\n... 他 {len(jobs) - 10} 件")

                # Excelに保存
                output_dir = Path("data/output")
                output_dir.mkdir(parents=True, exist_ok=True)

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = output_dir / f"townwork_{keyword}_{area}_{timestamp}.xlsx"

                df = pd.DataFrame(jobs)
                df.to_excel(filename, index=False, engine='openpyxl')

                print(f"\n保存完了: {filename}")
            else:
                print("\n求人が見つかりませんでした")

        except Exception as e:
            print(f"\nエラーが発生しました: {e}")
            import traceback
            traceback.print_exc()

        finally:
            print("\n5秒後にブラウザを閉じます...")
            await asyncio.sleep(5)
            await browser.close()

    print("\n完了!")


if __name__ == "__main__":
    asyncio.run(main())
