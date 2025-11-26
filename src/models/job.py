"""
求人情報モデル
要件定義の4.1 データ項目一覧に準拠
"""
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional
import re


class JobStatus(Enum):
    """求人ステータス"""
    ACTIVE = "active"
    EXPIRED = "expired"
    FILTERED = "filtered"


@dataclass
class Job:
    """求人情報データクラス"""

    # 必須項目
    source_site: str  # 媒体名
    job_id: str  # 求人番号（媒体固有ID）
    company_name: str  # 会社名
    job_title: str  # 職種
    employment_type: str  # 雇用形態
    page_url: str  # ページURL
    crawled_at: datetime  # 取得日時
    updated_at: datetime  # 更新日時

    # オプション項目
    id: Optional[int] = None  # DB内部ID
    company_name_kana: Optional[str] = None  # 会社名カナ
    postal_code: Optional[str] = None  # 郵便番号
    address_pref: Optional[str] = None  # 住所1（都道府県）
    address_city: Optional[str] = None  # 住所2（市区町村）
    address_detail: Optional[str] = None  # 住所3（番地以降）
    phone_number: Optional[str] = None  # 電話番号
    phone_number_normalized: Optional[str] = None  # 電話番号（正規化済み）
    fax_number: Optional[str] = None  # FAX番号
    salary: Optional[str] = None  # 給与
    salary_min: Optional[int] = None  # 給与下限（円）
    salary_max: Optional[int] = None  # 給与上限（円）
    working_hours: Optional[str] = None  # 勤務時間
    holidays: Optional[str] = None  # 休日・休暇
    work_location: Optional[str] = None  # 就業場所
    business_description: Optional[str] = None  # 事業内容
    job_description: Optional[str] = None  # 仕事内容
    requirements: Optional[str] = None  # 応募資格
    hiring_count: Optional[int] = None  # 採用人数
    contact_person: Optional[str] = None  # 担当者名
    contact_email: Optional[str] = None  # 担当者メールアドレス
    employee_count: Optional[int] = None  # 従業員数
    established_year: Optional[int] = None  # 設立年
    capital: Optional[int] = None  # 資本金（円）
    posted_date: Optional[datetime] = None  # 掲載開始日
    expire_date: Optional[datetime] = None  # 掲載終了日
    is_new: bool = True  # 新着フラグ
    is_filtered: bool = False  # フィルタ済みフラグ
    filter_reason: Optional[str] = None  # フィルタ理由
    status: JobStatus = JobStatus.ACTIVE

    def __post_init__(self):
        """初期化後の処理"""
        # 電話番号の正規化
        if self.phone_number and not self.phone_number_normalized:
            self.phone_number_normalized = self.normalize_phone_number(self.phone_number)

        # 郵便番号の正規化
        if self.postal_code:
            self.postal_code = self.normalize_postal_code(self.postal_code)

    @staticmethod
    def normalize_phone_number(phone: str) -> str:
        """
        電話番号を正規化（ハイフンなしの数字のみ）

        例:
        03(1234)5678 → 0312345678
        03-1234-5678 → 0312345678
        ０３−１２３４−５６７８ → 0312345678
        """
        if not phone:
            return ""

        # 全角→半角変換
        trans_table = str.maketrans(
            '０１２３４５６７８９−（）　',
            '0123456789-()  '
        )
        phone = phone.translate(trans_table)

        # 数字以外を除去
        return re.sub(r'[^\d]', '', phone)

    @staticmethod
    def normalize_postal_code(postal: str) -> str:
        """
        郵便番号を正規化（XXX-XXXX形式）

        例:
        1000001 → 100-0001
        〒100-0001 → 100-0001
        """
        if not postal:
            return ""

        # 数字のみ抽出
        digits = re.sub(r'[^\d]', '', postal)

        if len(digits) == 7:
            return f"{digits[:3]}-{digits[3:]}"
        return postal

    @staticmethod
    def format_phone_number(normalized: str) -> str:
        """
        正規化された電話番号をハイフン付きで整形

        例:
        0312345678 → 03-1234-5678
        09012345678 → 090-1234-5678
        0120123456 → 0120-12-3456
        """
        if not normalized:
            return ""

        # フリーダイヤル
        if normalized.startswith('0120'):
            if len(normalized) == 10:
                return f"{normalized[:4]}-{normalized[4:6]}-{normalized[6:]}"
        # 携帯電話
        elif normalized.startswith('0') and len(normalized) == 11:
            return f"{normalized[:3]}-{normalized[3:7]}-{normalized[7:]}"
        # 固定電話（市外局番2桁）
        elif normalized.startswith('03') or normalized.startswith('06'):
            if len(normalized) == 10:
                return f"{normalized[:2]}-{normalized[2:6]}-{normalized[6:]}"
        # 固定電話（市外局番3桁）
        elif len(normalized) == 10:
            return f"{normalized[:3]}-{normalized[3:6]}-{normalized[6:]}"

        return normalized

    def to_dict(self) -> dict:
        """辞書に変換"""
        return {
            'id': self.id,
            'source_site': self.source_site,
            'job_id': self.job_id,
            'company_name': self.company_name,
            'company_name_kana': self.company_name_kana,
            'postal_code': self.postal_code,
            'address_pref': self.address_pref,
            'address_city': self.address_city,
            'address_detail': self.address_detail,
            'phone_number': self.phone_number,
            'phone_number_normalized': self.phone_number_normalized,
            'fax_number': self.fax_number,
            'job_title': self.job_title,
            'employment_type': self.employment_type,
            'salary': self.salary,
            'salary_min': self.salary_min,
            'salary_max': self.salary_max,
            'working_hours': self.working_hours,
            'holidays': self.holidays,
            'work_location': self.work_location,
            'business_description': self.business_description,
            'job_description': self.job_description,
            'requirements': self.requirements,
            'hiring_count': self.hiring_count,
            'contact_person': self.contact_person,
            'contact_email': self.contact_email,
            'page_url': self.page_url,
            'employee_count': self.employee_count,
            'established_year': self.established_year,
            'capital': self.capital,
            'posted_date': self.posted_date.isoformat() if self.posted_date else None,
            'expire_date': self.expire_date.isoformat() if self.expire_date else None,
            'crawled_at': self.crawled_at.isoformat() if self.crawled_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'is_new': self.is_new,
            'is_filtered': self.is_filtered,
            'filter_reason': self.filter_reason,
            'status': self.status.value,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Job':
        """辞書から生成"""
        # 日付フィールドの変換
        for date_field in ['posted_date', 'expire_date', 'crawled_at', 'updated_at']:
            if data.get(date_field) and isinstance(data[date_field], str):
                data[date_field] = datetime.fromisoformat(data[date_field])

        # ステータスの変換
        if 'status' in data and isinstance(data['status'], str):
            data['status'] = JobStatus(data['status'])

        return cls(**data)

    def to_csv_row(self) -> dict:
        """CSV出力用の辞書（日本語キー）"""
        return {
            '媒体名': self.source_site,
            '求人番号': self.job_id,
            '会社名': self.company_name,
            '会社名カナ': self.company_name_kana or '',
            '郵便番号': self.postal_code or '',
            '住所1': self.address_pref or '',
            '住所2': self.address_city or '',
            '住所3': self.address_detail or '',
            '電話番号': self.format_phone_number(self.phone_number_normalized) if self.phone_number_normalized else '',
            'FAX番号': self.fax_number or '',
            '職種': self.job_title,
            '雇用形態': self.employment_type,
            '給与': self.salary or '',
            '勤務時間': self.working_hours or '',
            '休日': self.holidays or '',
            '就業場所': self.work_location or '',
            '事業内容': self.business_description or '',
            '仕事内容': self.job_description or '',
            '応募資格': self.requirements or '',
            '採用人数': self.hiring_count if self.hiring_count else '',
            '担当者名': self.contact_person or '',
            '担当者メールアドレス': self.contact_email or '',
            'ページURL': self.page_url,
            '従業員数': self.employee_count if self.employee_count else '',
            '取得日時': self.crawled_at.strftime('%Y-%m-%d %H:%M:%S') if self.crawled_at else '',
        }
