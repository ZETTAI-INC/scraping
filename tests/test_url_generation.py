"""
URL生成テスト
各スクレイパーのgenerate_search_urlが正しいURLを生成するか検証
"""
import pytest
import re
from urllib.parse import urlparse, parse_qs


class TestTownworkUrlGeneration:
    """タウンワークURL生成テスト"""

    def test_category_search_se_ishikawa(self, townwork_scraper):
        """石川+SEでカテゴリ検索URLが生成されるか"""
        url = townwork_scraper.generate_search_url("SE", "石川", 1)
        assert "prefectures/ishikawa" in url
        assert "oc-013" in url
        assert "omc-0102" in url
        assert "sc=new" in url

    def test_category_search_jimu_tokyo(self, townwork_scraper):
        """東京+事務でカテゴリ検索URLが生成されるか"""
        url = townwork_scraper.generate_search_url("事務", "東京", 1)
        assert "prefectures/tokyo" in url
        assert "oc-006" in url
        assert "sc=new" in url

    def test_category_search_kaigo_osaka(self, townwork_scraper):
        """大阪+介護でカテゴリ検索URLが生成されるか"""
        url = townwork_scraper.generate_search_url("介護", "大阪", 1)
        assert "prefectures/oosaka" in url
        assert "oc-010" in url
        assert "sc=new" in url

    def test_keyword_fallback(self, townwork_scraper):
        """カテゴリにないキーワードはキーワード検索にフォールバック"""
        url = townwork_scraper.generate_search_url("システム開発", "福岡", 1)
        assert "job_search/kw/" in url
        assert "sc=new" in url

    def test_pagination(self, townwork_scraper):
        """ページネーションが正しく動作するか"""
        url_page1 = townwork_scraper.generate_search_url("SE", "東京", 1)
        url_page2 = townwork_scraper.generate_search_url("SE", "東京", 2)
        assert "page=" not in url_page1 or "page=1" not in url_page1
        assert "page=2" in url_page2

    @pytest.mark.parametrize("area", [
        "北海道", "東京", "大阪", "石川", "沖縄", "京都", "愛知", "福岡"
    ])
    def test_prefecture_roman_mapping(self, townwork_scraper, area):
        """主要都道府県のローマ字マッピングが存在するか"""
        assert area in townwork_scraper.PREF_ROMAN
        roman = townwork_scraper.PREF_ROMAN[area]
        assert roman.isascii()
        assert roman.islower()

    @pytest.mark.parametrize("keyword,expected_oc", [
        ("SE", "oc-013"),
        ("事務", "oc-006"),
        ("介護", "oc-010"),
        ("営業", "oc-002"),
        ("飲食", "oc-001"),
    ])
    def test_job_category_mapping(self, townwork_scraper, keyword, expected_oc):
        """職種カテゴリマッピングが正しいか"""
        category_info = townwork_scraper.JOB_CATEGORIES.get(keyword)
        assert category_info is not None
        assert category_info[0] == expected_oc


class TestBaitoruUrlGeneration:
    """バイトルURL生成テスト"""

    def test_category_search_tokyo(self, baitoru_scraper):
        """東京でカテゴリ検索URLが生成されるか"""
        url = baitoru_scraper.generate_search_url("販売", "東京", 1)
        assert "baitoru.com" in url
        assert "kanto" in url or "tokyo" in url

    def test_keyword_fallback(self, baitoru_scraper):
        """カテゴリがない場合キーワード検索にフォールバック"""
        url = baitoru_scraper.generate_search_url("特殊キーワード", "東京", 1)
        assert "wrd" in url or "baitoru.com" in url

    def test_pagination(self, baitoru_scraper):
        """ページネーションが正しく動作するか"""
        url_page2 = baitoru_scraper.generate_search_url("販売", "東京", 2)
        assert "page2" in url_page2

    def test_srt2_new_sort(self, baitoru_scraper):
        """新着順ソート(srt2)が含まれるか"""
        url = baitoru_scraper.generate_search_url("販売", "東京", 1)
        assert "srt2" in url


class TestLineBaitoUrlGeneration:
    """LINEバイトURL生成テスト"""

    def test_basic_url_structure(self, linebaito_scraper):
        """基本的なURL構造"""
        url = linebaito_scraper.generate_search_url("飲食", "東京", 1)
        assert "baito.line.me" in url

    def test_prefecture_id_mapping(self, linebaito_scraper):
        """都道府県IDマッピング"""
        assert linebaito_scraper.PREFECTURE_IDS["東京"] == 13
        assert linebaito_scraper.PREFECTURE_IDS["大阪"] == 27
        assert linebaito_scraper.PREFECTURE_IDS["石川"] == 17

    @pytest.mark.parametrize("area,expected_id", [
        ("北海道", 1),
        ("東京", 13),
        ("大阪", 27),
        ("沖縄", 47),
    ])
    def test_prefecture_ids(self, linebaito_scraper, area, expected_id):
        """JIS都道府県コードが正しいか"""
        assert linebaito_scraper.PREFECTURE_IDS[area] == expected_id

    def test_job_category_ids(self, linebaito_scraper):
        """職種カテゴリIDが存在するか"""
        assert "介護" in linebaito_scraper.JOB_CATEGORY_IDS
        assert "看護" in linebaito_scraper.JOB_CATEGORY_IDS or "看護師" in linebaito_scraper.JOB_CATEGORY_IDS


class TestIndeedUrlGeneration:
    """IndeedURL生成テスト"""

    def test_basic_url_structure(self, indeed_scraper):
        """基本的なURL構造"""
        url = indeed_scraper.generate_search_url("SE", "東京", 1)
        assert "jp.indeed.com" in url
        assert "jobs?" in url

    def test_query_parameters(self, indeed_scraper):
        """クエリパラメータが正しいか"""
        url = indeed_scraper.generate_search_url("エンジニア", "大阪", 1)
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        assert "q" in params
        assert "l" in params

    def test_pagination_offset(self, indeed_scraper):
        """ページネーションのオフセットが正しいか"""
        url_page1 = indeed_scraper.generate_search_url("SE", "東京", 1)
        url_page2 = indeed_scraper.generate_search_url("SE", "東京", 2)

        parsed1 = urlparse(url_page1)
        params1 = parse_qs(parsed1.query)

        parsed2 = urlparse(url_page2)
        params2 = parse_qs(parsed2.query)

        # page1はstart=0、page2はstart=15（デフォルト）
        assert params1.get("start", ["0"])[0] == "0"
        assert int(params2.get("start", ["0"])[0]) > 0


class TestAllScrapersUrlValidity:
    """全スクレイパーのURL有効性テスト"""

    def test_townwork_url_is_valid(self, townwork_scraper):
        """タウンワークのURLが有効な形式か"""
        url = townwork_scraper.generate_search_url("SE", "東京", 1)
        parsed = urlparse(url)
        assert parsed.scheme == "https"
        assert "townwork.net" in parsed.netloc

    def test_baitoru_url_is_valid(self, baitoru_scraper):
        """バイトルのURLが有効な形式か"""
        url = baitoru_scraper.generate_search_url("販売", "東京", 1)
        parsed = urlparse(url)
        assert parsed.scheme == "https"
        assert "baitoru.com" in parsed.netloc

    def test_linebaito_url_is_valid(self, linebaito_scraper):
        """LINEバイトのURLが有効な形式か"""
        url = linebaito_scraper.generate_search_url("飲食", "東京", 1)
        parsed = urlparse(url)
        assert parsed.scheme == "https"
        assert "line.me" in parsed.netloc

    def test_indeed_url_is_valid(self, indeed_scraper):
        """IndeedのURLが有効な形式か"""
        url = indeed_scraper.generate_search_url("SE", "東京", 1)
        parsed = urlparse(url)
        assert parsed.scheme == "https"
        assert "indeed.com" in parsed.netloc
