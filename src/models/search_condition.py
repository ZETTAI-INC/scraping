"""
検索条件モデル
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional
import json


@dataclass
class SearchCondition:
    """検索条件データクラス"""

    id: Optional[int] = None
    name: str = ""  # 条件名（保存用）
    keywords: List[str] = field(default_factory=list)  # 検索キーワード
    exclude_keywords: List[str] = field(default_factory=list)  # 除外キーワード
    areas: List[str] = field(default_factory=list)  # 地域
    prefectures: List[str] = field(default_factory=list)  # 都道府県
    cities: List[str] = field(default_factory=list)  # 市区町村
    job_categories: List[str] = field(default_factory=list)  # 職種カテゴリ
    employment_types: List[str] = field(default_factory=list)  # 雇用形態
    salary_min: Optional[int] = None  # 給与下限
    salary_max: Optional[int] = None  # 給与上限
    salary_type: str = "monthly"  # hourly/monthly/yearly
    features: List[str] = field(default_factory=list)  # 特徴タグ
    sources: List[str] = field(default_factory=list)  # 対象媒体
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_json(self) -> str:
        """JSON文字列に変換"""
        return json.dumps({
            'name': self.name,
            'keywords': self.keywords,
            'exclude_keywords': self.exclude_keywords,
            'areas': self.areas,
            'prefectures': self.prefectures,
            'cities': self.cities,
            'job_categories': self.job_categories,
            'employment_types': self.employment_types,
            'salary_min': self.salary_min,
            'salary_max': self.salary_max,
            'salary_type': self.salary_type,
            'features': self.features,
            'sources': self.sources,
        }, ensure_ascii=False)

    @classmethod
    def from_json(cls, json_str: str, **kwargs) -> 'SearchCondition':
        """JSON文字列から生成"""
        data = json.loads(json_str)
        data.update(kwargs)
        return cls(**data)

    def get_summary(self) -> str:
        """条件のサマリー文字列を生成"""
        parts = []
        if self.keywords:
            parts.append(f"キーワード: {', '.join(self.keywords[:3])}")
        if self.prefectures:
            parts.append(f"地域: {', '.join(self.prefectures[:3])}")
        if self.job_categories:
            parts.append(f"職種: {', '.join(self.job_categories[:3])}")
        return ' | '.join(parts) if parts else '条件なし'
