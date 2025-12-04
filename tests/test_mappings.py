"""
マッピングテスト
都道府県、職種カテゴリなどのマッピングが正しいか検証
"""
import pytest


class TestTownworkMappings:
    """タウンワークのマッピングテスト"""

    def test_all_prefectures_have_roman(self, townwork_scraper, all_prefectures):
        """全47都道府県にローマ字マッピングがあるか"""
        missing = []
        for pref in all_prefectures:
            if pref not in townwork_scraper.PREF_ROMAN:
                missing.append(pref)
        assert len(missing) == 0, f"ローマ字マッピングがない都道府県: {missing}"

    def test_all_prefectures_have_area_name(self, townwork_scraper, all_prefectures):
        """全47都道府県にエリア名マッピングがあるか"""
        missing = []
        for pref in all_prefectures:
            if pref not in townwork_scraper.AREA_NAMES:
                missing.append(pref)
        assert len(missing) == 0, f"エリア名マッピングがない都道府県: {missing}"

    def test_pref_roman_values_are_lowercase_ascii(self, townwork_scraper):
        """ローマ字が小文字ASCIIのみか"""
        for pref, roman in townwork_scraper.PREF_ROMAN.items():
            assert roman.isascii(), f"{pref}のローマ字 '{roman}' がASCIIではない"
            assert roman.islower(), f"{pref}のローマ字 '{roman}' が小文字ではない"

    def test_area_names_end_with_suffix(self, townwork_scraper):
        """エリア名が正しい接尾辞（都府県道）で終わるか"""
        for short_name, full_name in townwork_scraper.AREA_NAMES.items():
            if short_name == "北海道":
                assert full_name == "北海道"
            elif short_name in ["東京"]:
                assert full_name.endswith("都"), f"{short_name} -> {full_name}"
            elif short_name in ["大阪", "京都"]:
                assert full_name.endswith("府"), f"{short_name} -> {full_name}"
            else:
                assert full_name.endswith("県"), f"{short_name} -> {full_name}"

    def test_job_categories_have_valid_codes(self, townwork_scraper):
        """職種カテゴリコードが有効な形式か"""
        for keyword, (oc_code, omc_code) in townwork_scraper.JOB_CATEGORIES.items():
            assert oc_code.startswith("oc-"), f"{keyword}の大カテゴリ '{oc_code}' が無効"
            if omc_code is not None:
                assert omc_code.startswith("omc-"), f"{keyword}の小カテゴリ '{omc_code}' が無効"

    @pytest.mark.parametrize("keyword", [
        "SE", "事務", "営業", "介護", "飲食", "販売", "IT", "エンジニア"
    ])
    def test_common_keywords_mapped(self, townwork_scraper, keyword):
        """主要なキーワードがマッピングされているか"""
        assert keyword in townwork_scraper.JOB_CATEGORIES, f"'{keyword}'がマッピングされていない"


class TestLineBaitoMappings:
    """LINEバイトのマッピングテスト"""

    def test_all_prefectures_have_id(self, linebaito_scraper, all_prefectures):
        """全47都道府県にIDマッピングがあるか"""
        missing = []
        for pref in all_prefectures:
            if pref not in linebaito_scraper.PREFECTURE_IDS:
                missing.append(pref)
        assert len(missing) == 0, f"IDマッピングがない都道府県: {missing}"

    def test_prefecture_ids_are_valid(self, linebaito_scraper):
        """都道府県IDが1-47の範囲内か"""
        for pref, id_val in linebaito_scraper.PREFECTURE_IDS.items():
            assert 1 <= id_val <= 47, f"{pref}のID {id_val} が範囲外"

    def test_prefecture_ids_are_unique(self, linebaito_scraper):
        """都道府県IDが重複していないか"""
        ids = list(linebaito_scraper.PREFECTURE_IDS.values())
        assert len(ids) == len(set(ids)), "都道府県IDに重複がある"

    def test_job_category_ids_are_positive(self, linebaito_scraper):
        """職種カテゴリIDが正の整数か"""
        for keyword, id_val in linebaito_scraper.JOB_CATEGORY_IDS.items():
            assert isinstance(id_val, int), f"{keyword}のID {id_val} が整数ではない"
            assert id_val > 0, f"{keyword}のID {id_val} が正の整数ではない"


class TestHelloworkMappings:
    """ハローワークのマッピングテスト"""

    def test_prefecture_codes_count(self):
        """都道府県コードが47個あるか"""
        from scrapers.hellowork import PREFECTURE_CODES
        assert len(PREFECTURE_CODES) == 47

    def test_prefecture_codes_are_valid(self):
        """都道府県コードが01-47の形式か"""
        from scrapers.hellowork import PREFECTURE_CODES
        for pref, code in PREFECTURE_CODES.items():
            assert len(code) == 2, f"{pref}のコード '{code}' が2桁ではない"
            assert code.isdigit(), f"{pref}のコード '{code}' が数字ではない"
            assert 1 <= int(code) <= 47, f"{pref}のコード '{code}' が範囲外"

    def test_job_category_codes_format(self):
        """職業分類コードが正しい形式か"""
        from scrapers.hellowork import KEYWORD_TO_CATEGORY
        for keyword, code in KEYWORD_TO_CATEGORY.items():
            # 中分類コードは3桁
            assert len(code) == 3 or len(code) == 2, f"{keyword}のコード '{code}' が不正"
            assert code.isdigit(), f"{keyword}のコード '{code}' が数字ではない"


class TestBaitoruMappings:
    """バイトルのマッピングテスト"""

    def test_area_codes_have_required_fields(self, baitoru_scraper):
        """エリアコードに必要なフィールドがあるか"""
        area_codes = baitoru_scraper.site_config.get("area_codes", {})
        required_fields = ["region", "prefecture"]

        for area, config in area_codes.items():
            if isinstance(config, dict):
                for field in required_fields:
                    assert field in config, f"{area}に'{field}'フィールドがない"


class TestMachbaitoMappings:
    """マッハバイトのマッピングテスト"""

    def test_all_prefectures_have_code(self, machbaito_scraper, all_prefectures):
        """全47都道府県にコードマッピングがあるか"""
        missing = []
        for pref in all_prefectures:
            if pref not in machbaito_scraper.PREFECTURE_CODES:
                missing.append(pref)
        assert len(missing) == 0, f"コードマッピングがない都道府県: {missing}"

    def test_prefecture_codes_are_valid(self, machbaito_scraper):
        """都道府県コードが1-47の範囲内か（JIS準拠）"""
        for pref, code in machbaito_scraper.PREFECTURE_CODES.items():
            assert 1 <= code <= 47, f"{pref}のコード {code} が範囲外"

    def test_prefecture_codes_are_unique(self, machbaito_scraper):
        """都道府県コードが重複していないか"""
        codes = list(machbaito_scraper.PREFECTURE_CODES.values())
        assert len(codes) == len(set(codes)), "都道府県コードに重複がある"

    def test_all_prefectures_have_city_code(self, machbaito_scraper):
        """全都道府県に対応するcityコードがあるか"""
        for pref, pref_code in machbaito_scraper.PREFECTURE_CODES.items():
            assert pref_code in machbaito_scraper.PREFECTURE_ALL_CITY_CODES, \
                f"{pref}(code={pref_code})に対応するcityコードがない"

    def test_city_codes_are_positive(self, machbaito_scraper):
        """cityコードが正の整数か"""
        for pref_code, city_code in machbaito_scraper.PREFECTURE_ALL_CITY_CODES.items():
            assert isinstance(city_code, int), f"pref_code={pref_code}のcityコードが整数ではない"
            assert city_code > 0, f"pref_code={pref_code}のcityコード {city_code} が正の整数ではない"

    def test_job_category_ids_are_lists(self, machbaito_scraper):
        """職種カテゴリIDがリスト形式か"""
        for keyword, ids in machbaito_scraper.JOB_CATEGORY_IDS.items():
            assert isinstance(ids, list), f"{keyword}のIDがリストではない"
            assert len(ids) > 0, f"{keyword}のIDリストが空"

    def test_job_category_ids_are_positive(self, machbaito_scraper):
        """職種カテゴリIDが正の整数か"""
        for keyword, ids in machbaito_scraper.JOB_CATEGORY_IDS.items():
            for id_val in ids:
                assert isinstance(id_val, int), f"{keyword}のID {id_val} が整数ではない"
                assert id_val > 0, f"{keyword}のID {id_val} が正の整数ではない"

    @pytest.mark.parametrize("keyword", [
        "SE", "事務", "営業", "介護", "飲食", "販売", "IT", "エンジニア", "ドライバー"
    ])
    def test_common_keywords_mapped(self, machbaito_scraper, keyword):
        """主要なキーワードがマッピングされているか"""
        found = keyword in machbaito_scraper.JOB_CATEGORY_IDS or \
                any(keyword in k for k in machbaito_scraper.JOB_CATEGORY_IDS.keys())
        assert found, f"'{keyword}'がマッピングされていない"

    @pytest.mark.parametrize("pref,expected_code", [
        ("北海道", 1),
        ("東京", 13),
        ("大阪", 27),
        ("沖縄", 47),
    ])
    def test_jis_prefecture_codes(self, machbaito_scraper, pref, expected_code):
        """JIS都道府県コードが正しいか"""
        assert machbaito_scraper.PREFECTURE_CODES[pref] == expected_code


class TestCrossScraperConsistency:
    """スクレイパー間の一貫性テスト"""

    def test_prefecture_count_consistency(self, townwork_scraper, linebaito_scraper):
        """都道府県数の一貫性"""
        townwork_count = len(townwork_scraper.PREF_ROMAN)
        linebaito_count = len(linebaito_scraper.PREFECTURE_IDS)
        assert townwork_count == linebaito_count == 47

    def test_common_keywords_across_scrapers(self, townwork_scraper, linebaito_scraper):
        """主要キーワードが複数スクレイパーでサポートされているか"""
        common_keywords = ["介護", "看護", "飲食"]

        for keyword in common_keywords:
            # タウンワーク
            assert keyword in townwork_scraper.JOB_CATEGORIES or \
                   any(keyword in k for k in townwork_scraper.JOB_CATEGORIES.keys()), \
                   f"タウンワークで '{keyword}' がサポートされていない"

            # LINEバイト（部分一致でもOK）
            linebaito_has = keyword in linebaito_scraper.JOB_CATEGORY_IDS or \
                           any(keyword in k for k in linebaito_scraper.JOB_CATEGORY_IDS.keys())
            # LINEバイトはカテゴリがない場合もあるのでwarningのみ
            if not linebaito_has:
                import warnings
                warnings.warn(f"LINEバイトで '{keyword}' が直接マッピングされていない")
