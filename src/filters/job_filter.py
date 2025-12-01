"""
求人フィルタリング機能
要件定義 7章 CSV出力時の除外・フィルタリングルールに準拠
"""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
import re
import logging

logger = logging.getLogger(__name__)


@dataclass
class FilterResult:
    """フィルタリング結果"""
    total_count: int = 0  # 取得総件数
    filtered_jobs: List[Dict[str, Any]] = field(default_factory=list)  # フィルタ後の求人
    excluded_count: int = 0  # 除外された総件数

    # 除外内訳
    duplicate_phone_count: int = 0  # 電話番号重複
    large_company_count: int = 0  # 従業員数1001人以上
    dispatch_keyword_count: int = 0  # 派遣・紹介キーワード
    industry_count: int = 0  # 業界（広告・メディア等）
    location_count: int = 0  # 勤務地（沖縄）
    phone_prefix_count: int = 0  # 電話番号プレフィックス

    def get_summary(self) -> str:
        """フィルタ結果のサマリーを取得"""
        return f"""
フィルタ適用結果
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

取得総件数:                    {self.total_count:,} 件

除外内訳:
  - 電話番号重複:              {self.duplicate_phone_count:,} 件
  - 従業員数1,001人以上:       {self.large_company_count:,} 件
  - 派遣・紹介キーワード:       {self.dispatch_keyword_count:,} 件
  - 業界（広告・メディア等）:    {self.industry_count:,} 件
  - 勤務地（沖縄県）:          {self.location_count:,} 件
  - 電話番号プレフィックス:     {self.phone_prefix_count:,} 件
  ─────────────────────────
  除外合計:                    {self.excluded_count:,} 件

最終出力件数:                  {len(self.filtered_jobs):,} 件
"""


class JobFilter:
    """求人フィルタリングクラス"""

    # 除外キーワード（会社名・事業内容）
    EXCLUDE_KEYWORDS = [
        "人材派遣",
        "人材紹介",
        "職業紹介",
        "有料職業紹介",
        "アウトソーシング",
        "紹介予定派遣",
        "スタッフサービス",
        "テンプスタッフ",
        "パソナ",
        "リクルートスタッフィング",
        "マンパワー",
        "アデコ",
        "ランスタッド",
    ]

    # 除外業界
    EXCLUDE_INDUSTRIES = [
        "広告",
        "新聞",
        "メディア",
        "出版",
        "放送",
        "広告代理店",
        "PR",
    ]

    # 除外電話番号プレフィックス
    EXCLUDE_PHONE_PREFIXES = [
        "0120",  # フリーダイヤル
        "0988",  # 沖縄
        "0980",  # 沖縄
        "0989",  # 沖縄
        "050",   # IP電話
        "50",    # IP電話（先頭0なし）
        "0880",  # 高知県西部
    ]

    # 除外地域
    EXCLUDE_LOCATIONS = [
        "沖縄県",
        "沖縄",
    ]

    # 大企業の従業員数しきい値
    LARGE_COMPANY_THRESHOLD = 1001

    # 媒体優先順位（重複除去時）
    SOURCE_PRIORITY = {
        "indeed": 1,
        "hellowork": 2,
        "townwork": 3,
        "baitoru": 4,
        "mahhabaito": 5,
        "linebaito": 6,
        "rikunavi": 7,
        "mynavi": 8,
        "entenshoku": 9,
        "kaigojob": 10,
        "jobmedley": 11,
    }

    def __init__(
        self,
        exclude_keywords: Optional[List[str]] = None,
        exclude_industries: Optional[List[str]] = None,
        exclude_phone_prefixes: Optional[List[str]] = None,
        exclude_locations: Optional[List[str]] = None,
        large_company_threshold: Optional[int] = None
    ):
        """
        フィルタの初期化

        Args:
            exclude_keywords: 除外キーワード（追加）
            exclude_industries: 除外業界（追加）
            exclude_phone_prefixes: 除外電話番号プレフィックス（追加）
            exclude_locations: 除外地域（追加）
            large_company_threshold: 大企業判定しきい値
        """
        self.exclude_keywords = self.EXCLUDE_KEYWORDS + (exclude_keywords or [])
        self.exclude_industries = self.EXCLUDE_INDUSTRIES + (exclude_industries or [])
        self.exclude_phone_prefixes = self.EXCLUDE_PHONE_PREFIXES + (exclude_phone_prefixes or [])
        self.exclude_locations = self.EXCLUDE_LOCATIONS + (exclude_locations or [])
        self.large_company_threshold = large_company_threshold or self.LARGE_COMPANY_THRESHOLD

    def filter_jobs(self, jobs: List[Dict[str, Any]]) -> FilterResult:
        """
        求人リストにフィルタを適用

        Args:
            jobs: 求人データのリスト

        Returns:
            FilterResult: フィルタリング結果
        """
        result = FilterResult(total_count=len(jobs))

        # Step 1: 電話番号重複削除
        jobs, dup_count = self._remove_phone_duplicates(jobs)
        result.duplicate_phone_count = dup_count

        filtered_jobs = []
        for job in jobs:
            exclude_reason = self._check_exclusion(job)
            if exclude_reason:
                # 除外理由をカウント
                if "従業員数" in exclude_reason:
                    result.large_company_count += 1
                elif "派遣" in exclude_reason or "紹介" in exclude_reason:
                    result.dispatch_keyword_count += 1
                elif "業界" in exclude_reason:
                    result.industry_count += 1
                elif "沖縄" in exclude_reason or "勤務地" in exclude_reason:
                    result.location_count += 1
                elif "電話番号" in exclude_reason:
                    result.phone_prefix_count += 1

                # フィルタ済みフラグを設定
                job['is_filtered'] = True
                job['filter_reason'] = exclude_reason
            else:
                filtered_jobs.append(job)

        result.filtered_jobs = filtered_jobs
        result.excluded_count = result.total_count - len(filtered_jobs)

        logger.info(f"Filtering completed: {result.total_count} -> {len(filtered_jobs)} jobs")
        return result

    def _remove_phone_duplicates(self, jobs: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
        """
        電話番号による重複削除

        優先順位:
        1. 掲載開始日が新しい
        2. 取得日時が新しい
        3. 媒体優先順位
        """
        phone_map: Dict[str, Dict[str, Any]] = {}
        no_phone_jobs = []

        for job in jobs:
            phone = job.get('phone_number_normalized', '')
            if not phone:
                no_phone_jobs.append(job)
                continue

            if phone not in phone_map:
                phone_map[phone] = job
            else:
                existing = phone_map[phone]
                if self._should_replace(existing, job):
                    phone_map[phone] = job

        unique_jobs = list(phone_map.values()) + no_phone_jobs
        duplicate_count = len(jobs) - len(unique_jobs)

        return unique_jobs, duplicate_count

    def _should_replace(self, existing: Dict[str, Any], new: Dict[str, Any]) -> bool:
        """新しい求人が既存を置き換えるべきか判定"""
        # 掲載開始日比較
        existing_posted = existing.get('posted_date')
        new_posted = new.get('posted_date')
        if existing_posted and new_posted:
            if new_posted > existing_posted:
                return True
            elif new_posted < existing_posted:
                return False

        # 取得日時比較
        existing_crawled = existing.get('crawled_at')
        new_crawled = new.get('crawled_at')
        if existing_crawled and new_crawled:
            if new_crawled > existing_crawled:
                return True
            elif new_crawled < existing_crawled:
                return False

        # 媒体優先順位比較
        existing_source = existing.get('source_name', existing.get('site', ''))
        new_source = new.get('source_name', new.get('site', ''))
        existing_priority = self.SOURCE_PRIORITY.get(existing_source.lower(), 99)
        new_priority = self.SOURCE_PRIORITY.get(new_source.lower(), 99)

        return new_priority < existing_priority

    def _check_exclusion(self, job: Dict[str, Any]) -> Optional[str]:
        """
        求人が除外対象かチェック

        Returns:
            除外理由（該当しない場合はNone）
        """
        # Step 2: 従業員数フィルタ
        employee_count = job.get('employee_count')
        if employee_count and employee_count >= self.large_company_threshold:
            return f"従業員数{employee_count}人（{self.large_company_threshold}人以上）"

        # Step 3: 企業名・事業内容キーワードフィルタ
        company_name = job.get('company_name', job.get('company', ''))
        business_desc = job.get('business_description', job.get('business_content', ''))
        combined_text = f"{company_name} {business_desc}"

        for keyword in self.exclude_keywords:
            if keyword in combined_text:
                return f"除外キーワード（{keyword}）"

        # Step 3.5: 雇用形態・タイトル・職種・その他フィールドに「派遣」が含まれる場合も除外
        # 複数のフィールド名をチェック（サイトによって異なる可能性があるため）
        employment_type = job.get('employment_type', '') or job.get('雇用形態', '') or ''
        title = job.get('title', '') or job.get('job_title', '') or ''
        job_type = job.get('job_type', '') or job.get('職種', '') or ''
        working_style = job.get('working_style', '') or job.get('勤務形態', '') or ''
        job_description = job.get('job_description', '') or job.get('仕事内容', '') or ''

        # 派遣関連のキーワード
        dispatch_keywords = ['派遣', '派遣社員', '無期雇用派遣', '登録型派遣']

        fields_to_check = [
            ('雇用形態', employment_type),
            ('タイトル', title),
            ('職種', job_type),
            ('勤務形態', working_style),
        ]

        for field_name, field_value in fields_to_check:
            if field_value:
                for dispatch_kw in dispatch_keywords:
                    if dispatch_kw in field_value:
                        logger.debug(f"Dispatch filter matched: {field_name}={field_value}")
                        return f"{field_name}に派遣（{field_value}）"

        # 仕事内容の冒頭に「派遣」が含まれる場合も除外（本文中の「派遣」は除外しない）
        if job_description:
            desc_start = job_description[:50]  # 冒頭50文字をチェック
            for dispatch_kw in dispatch_keywords:
                if dispatch_kw in desc_start:
                    logger.debug(f"Dispatch filter matched in job_description start: {desc_start}")
                    return f"仕事内容冒頭に派遣（{desc_start[:30]}...）"

        # Step 4: 業界フィルタ
        for industry in self.exclude_industries:
            if industry in combined_text:
                return f"除外業界（{industry}）"

        # Step 5: 勤務地フィルタ
        address_pref = job.get('address_pref', '')
        work_location = job.get('work_location', job.get('location', ''))
        location_text = f"{address_pref} {work_location}"

        for location in self.exclude_locations:
            if location in location_text:
                return f"除外勤務地（{location}）"

        # Step 6: 電話番号プレフィックスフィルタ
        phone = job.get('phone_number_normalized', '')
        if phone:
            for prefix in self.exclude_phone_prefixes:
                if phone.startswith(prefix):
                    return f"除外電話番号（{prefix}）"

        return None

    def get_filter_settings(self) -> Dict[str, Any]:
        """現在のフィルタ設定を取得"""
        return {
            'exclude_keywords': self.exclude_keywords,
            'exclude_industries': self.exclude_industries,
            'exclude_phone_prefixes': self.exclude_phone_prefixes,
            'exclude_locations': self.exclude_locations,
            'large_company_threshold': self.large_company_threshold,
        }
