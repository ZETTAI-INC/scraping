"""
pytest共通フィクスチャ
"""
import sys
import os
import pytest

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def townwork_scraper():
    """タウンワークスクレイパーのインスタンス"""
    from scrapers.townwork import TownworkScraper
    return TownworkScraper()


@pytest.fixture
def baitoru_scraper():
    """バイトルスクレイパーのインスタンス"""
    from scrapers.baitoru import BaitoruScraper
    return BaitoruScraper()


@pytest.fixture
def linebaito_scraper():
    """LINEバイトスクレイパーのインスタンス"""
    from scrapers.linebaito import LineBaitoScraper
    return LineBaitoScraper()


@pytest.fixture
def indeed_scraper():
    """Indeedスクレイパーのインスタンス"""
    from scrapers.indeed import IndeedScraper
    return IndeedScraper()


@pytest.fixture
def hellowork_scraper():
    """ハローワークスクレイパーのインスタンス"""
    from scrapers.hellowork import HelloWorkScraper
    return HelloWorkScraper()


@pytest.fixture
def machbaito_scraper():
    """マッハバイトスクレイパーのインスタンス"""
    from scrapers.machbaito import MachbaitoScraper
    return MachbaitoScraper()


# 全47都道府県リスト
ALL_PREFECTURES = [
    "北海道",
    "青森", "岩手", "宮城", "秋田", "山形", "福島",
    "茨城", "栃木", "群馬", "埼玉", "千葉", "東京", "神奈川",
    "新潟", "富山", "石川", "福井", "山梨", "長野",
    "岐阜", "静岡", "愛知", "三重",
    "滋賀", "京都", "大阪", "兵庫", "奈良", "和歌山",
    "鳥取", "島根", "岡山", "広島", "山口",
    "徳島", "香川", "愛媛", "高知",
    "福岡", "佐賀", "長崎", "熊本", "大分", "宮崎", "鹿児島", "沖縄",
]


@pytest.fixture
def all_prefectures():
    """全47都道府県のリスト"""
    return ALL_PREFECTURES


# 主要なテスト用キーワード
TEST_KEYWORDS = [
    "SE", "事務", "営業", "介護", "看護", "飲食",
    "販売", "IT", "エンジニア", "ドライバー",
]


@pytest.fixture
def test_keywords():
    """テスト用キーワードリスト"""
    return TEST_KEYWORDS
