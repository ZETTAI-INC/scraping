#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
バイトルスクレイピング実行スクリプト
"""
import asyncio
import sys
import os
import csv
from datetime import datetime
from pathlib import Path

# Windows UTF-8対応
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from playwright.async_api import async_playwright
from scrapers.baitoru import BaitoruScraper
from utils.stealth import StealthConfig, create_stealth_context


async def run_baitoru_scraper(
    category: str = "",
    area: str = "東京",
    max_pages: int = 2,
    headless: bool = False
):
    """
    バイトルスクレイパーを実行

    Args:
        category: 職種カテゴリ (販売, 飲食, 事務, etc.)
        area: 検索エリア (東京, 大阪, etc.)
        max_pages: 最大ページ数
        headless: ヘッドレスモード
    """
    print("="*60)
    print("バイトル スクレイピング開始")
    print(f"カテゴリ: {category if category else '全て'}")
    print(f"エリア: {area}")
    print(f"最大ページ数: {max_pages}")
    print("="*60)

    scraper = BaitoruScraper()

    # 利用可能なカテゴリを表示
    if category and category not in scraper.site_config.get("job_categories", {}):
        print(f"\n警告: カテゴリ '{category}' は設定されていません。")
        print("利用可能なカテゴリ:")
        for cat in scraper.site_config.get("job_categories", {}).keys():
            print(f"  - {cat}")
        print()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
            ]
        )

        context = await create_stealth_context(browser)
        page = await context.new_page()
        await StealthConfig.apply_stealth_scripts(page)

        try:
            # 検索実行（カテゴリ指定）
            jobs = await scraper.search_jobs(page, category, area, max_pages)

            print(f"\n取得件数: {len(jobs)}件")

            if jobs:
                # CSVファイルに保存
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_dir = project_root / "data" / "output"
                output_dir.mkdir(parents=True, exist_ok=True)
                output_file = output_dir / f"baitoru_{timestamp}.csv"

                # CSVのフィールド定義
                fieldnames = [
                    "job_number",
                    "company_name",
                    "title",
                    "employment_type",
                    "salary",
                    "location",
                    "job_type",
                    "working_hours",
                    "tags",
                    "page_url",
                    "job_number_display",
                ]

                with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                    writer.writeheader()
                    for job in jobs:
                        writer.writerow(job)

                print(f"出力ファイル: {output_file}")

                # 最初の5件を表示
                print("\n--- 取得した求人（最初の5件）---")
                for i, job in enumerate(jobs[:5], 1):
                    print(f"\n[{i}] {job.get('title', 'タイトルなし')[:50]}...")
                    print(f"    会社: {job.get('company_name', '')}")
                    print(f"    給与: {job.get('salary', '')}")
                    print(f"    場所: {job.get('location', '')[:40]}...")
                    print(f"    雇用形態: {job.get('employment_type', '')}")

        except Exception as e:
            print(f"エラー: {e}")
            import traceback
            traceback.print_exc()

        finally:
            await context.close()
            await browser.close()

    print("\n" + "="*60)
    print("スクレイピング完了")
    print("="*60)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="バイトルスクレイパー")
    parser.add_argument("--category", "-c", default="",
                        help="職種カテゴリ (販売, 飲食, 事務, オフィス, 工場, etc.)")
    parser.add_argument("--area", "-a", default="東京",
                        help="検索エリア (東京, 大阪, etc.)")
    parser.add_argument("--pages", "-p", type=int, default=2,
                        help="最大ページ数")
    parser.add_argument("--headless", action="store_true",
                        help="ヘッドレスモード")
    parser.add_argument("--list-categories", action="store_true",
                        help="利用可能なカテゴリ一覧を表示")

    args = parser.parse_args()

    if args.list_categories:
        import json
        with open("config/selectors.json", encoding="utf-8") as f:
            config = json.load(f)
        print("利用可能なカテゴリ:")
        for cat in config.get("baitoru", {}).get("job_categories", {}).keys():
            print(f"  - {cat}")
    else:
        asyncio.run(run_baitoru_scraper(
            category=args.category,
            area=args.area,
            max_pages=args.pages,
            headless=args.headless
        ))
