"""
CSV出力機能
要件定義 5.3 CSV出力形式に準拠
"""
import csv
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
import re
import logging

logger = logging.getLogger(__name__)


class CSVExporter:
    """CSV出力クラス"""

    # CSV出力列順序（要件定義 5.3.3）
    CSV_COLUMNS = [
        ('source_display_name', '媒体名'),
        ('job_id', '求人番号'),
        ('company_name', '会社名'),
        ('company_name_kana', '会社名カナ'),
        ('postal_code', '郵便番号'),
        ('address_pref', '住所1'),
        ('address_city', '住所2'),
        ('address_detail', '住所3'),
        ('phone_number_formatted', '電話番号'),
        ('fax_number', 'FAX番号'),
        ('job_title', '職種'),
        ('employment_type', '雇用形態'),
        ('salary', '給与'),
        ('working_hours', '勤務時間'),
        ('holidays', '休日'),
        ('work_location', '就業場所'),
        ('business_description', '事業内容'),
        ('job_description', '仕事内容'),
        ('requirements', '応募資格'),
        ('hiring_count', '採用人数'),
        ('contact_person', '担当者名'),
        ('contact_email', '担当者メールアドレス'),
        ('page_url', 'ページURL'),
        ('employee_count', '従業員数'),
        ('published_date', '掲載日'),
        ('crawled_at', '取得日時'),
    ]

    def __init__(self, output_dir: str = "data/output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export(
        self,
        jobs: List[Dict[str, Any]],
        keyword: Optional[str] = None,
        area: Optional[str] = None,
        filename: Optional[str] = None
    ) -> Path:
        """
        求人データをCSVファイルにエクスポート

        Args:
            jobs: 求人データのリスト
            keyword: 検索キーワード（ファイル名用）
            area: 地域（ファイル名用）
            filename: カスタムファイル名

        Returns:
            出力ファイルパス
        """
        # ファイル名生成
        if filename:
            output_path = self.output_dir / filename
        else:
            output_path = self.output_dir / self._generate_filename(keyword, area)

        # データの前処理
        processed_jobs = [self._process_job(job) for job in jobs]

        # CSV出力（UTF-8 BOM付き）
        with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)

            # ヘッダー行
            headers = [col[1] for col in self.CSV_COLUMNS]
            writer.writerow(headers)

            # データ行
            for job in processed_jobs:
                row = [self._get_value(job, col[0]) for col in self.CSV_COLUMNS]
                writer.writerow(row)

        logger.info(f"CSV exported: {output_path} ({len(jobs)} records)")
        return output_path

    def _generate_filename(self, keyword: Optional[str], area: Optional[str]) -> str:
        """ファイル名を生成"""
        parts = ["求人データ"]

        if keyword:
            # ファイル名に使えない文字を除去
            safe_keyword = re.sub(r'[\\/:*?"<>|]', '', keyword)[:20]
            parts.append(safe_keyword)

        if area:
            safe_area = re.sub(r'[\\/:*?"<>|]', '', area)[:20]
            parts.append(safe_area)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        parts.append(timestamp)

        return "_".join(parts) + ".csv"

    def _process_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """求人データを出力用に加工"""
        processed = job.copy()

        # 電話番号のフォーマット（phone または phone_number から取得）
        phone = job.get('phone_number_normalized', job.get('phone_number', job.get('phone', '')))
        processed['phone_number_formatted'] = self._format_phone(phone)

        # 住所の処理（address フィールドがある場合、address_pref に設定）
        if job.get('address') and not job.get('address_pref'):
            processed['address_pref'] = job.get('address', '')

        # 事業内容のマッピング
        if job.get('business_content') and not job.get('business_description'):
            processed['business_description'] = job.get('business_content', '')

        # 日時のフォーマット
        crawled_at = job.get('crawled_at')
        if crawled_at:
            if isinstance(crawled_at, str):
                processed['crawled_at'] = crawled_at
            else:
                processed['crawled_at'] = crawled_at.strftime('%Y-%m-%d %H:%M:%S')

        # 媒体名の表示名
        if not job.get('source_display_name'):
            processed['source_display_name'] = job.get('site', job.get('source_name', ''))

        return processed

    def _get_value(self, job: Dict[str, Any], key: str) -> str:
        """辞書から値を取得（None対応）"""
        value = job.get(key)
        if value is None:
            return ''
        if isinstance(value, (int, float)):
            return str(value)
        return str(value)

    def _format_phone(self, phone: str) -> str:
        """電話番号をハイフン付きフォーマットに変換"""
        if not phone:
            return ''

        # 数字のみ抽出
        digits = re.sub(r'[^\d]', '', phone)

        # フリーダイヤル
        if digits.startswith('0120'):
            if len(digits) == 10:
                return f"{digits[:4]}-{digits[4:6]}-{digits[6:]}"
            return digits

        # 携帯電話（070/080/090）
        if digits.startswith(('070', '080', '090')) and len(digits) == 11:
            return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"

        # 固定電話（市外局番2桁: 03, 06など）
        if digits.startswith(('03', '06')) and len(digits) == 10:
            return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"

        # 固定電話（市外局番3桁）
        if len(digits) == 10:
            return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"

        # その他
        return digits

    def get_csv_preview(self, jobs: List[Dict[str, Any]], limit: int = 5) -> str:
        """CSVプレビュー（最初の数行）を取得"""
        headers = [col[1] for col in self.CSV_COLUMNS]

        lines = [",".join(headers)]

        for job in jobs[:limit]:
            processed = self._process_job(job)
            row = [self._get_value(processed, col[0]) for col in self.CSV_COLUMNS]
            # 長い値は省略
            row = [v[:50] + "..." if len(v) > 50 else v for v in row]
            lines.append(",".join(row))

        return "\n".join(lines)
