"""
求人情報リポジトリ
データベースへの求人情報の保存・取得・検索を担当
"""
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple
import re
import logging
from urllib.parse import urlparse, urlunparse
import hashlib

from .db_manager import DatabaseManager

logger = logging.getLogger(__name__)


class JobRepository:
    """求人情報リポジトリ"""

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    def save_job(self, job_data: Dict[str, Any], source_name: str) -> Tuple[int, bool]:
        """求人情報を保存（UPSERT）

        Returns:
            Tuple[int, bool]: (job_id, is_new) - 保存されたジョブのIDと新規かどうかのフラグ
        """
        source_id = self.db.get_source_id(source_name)
        if not source_id:
            raise ValueError(f"Unknown source: {source_name}")

        # 電話番号の正規化
        phone_normalized = self._normalize_phone(job_data.get('phone_number', ''))

        # 住所の分解
        address_parts = self._parse_address(job_data.get('location', ''))

        now = datetime.now()
        normalized_url = self._normalize_url(job_data.get('page_url') or job_data.get('url', ''))
        job_id_value = (
            job_data.get('job_id')
            or job_data.get('job_number')
            or normalized_url
            or self._generate_fallback_id(job_data)
        )
        page_url_value = normalized_url

        with self.db.get_connection() as conn:
            cursor = conn.cursor()

            # 既存レコードの確認
            cursor.execute("""
                SELECT id, crawled_at FROM jobs
                WHERE source_id = ? AND job_id = ?
            """, (source_id, job_id_value))

            existing = cursor.fetchone()

            if existing:
                # 更新
                cursor.execute("""
                    UPDATE jobs SET
                        company_name = ?,
                        company_name_kana = ?,
                        postal_code = ?,
                        address_pref = ?,
                        address_city = ?,
                        address_detail = ?,
                        phone_number = ?,
                        phone_number_normalized = ?,
                        fax_number = ?,
                        job_title = ?,
                        employment_type = ?,
                        salary = ?,
                        salary_min = ?,
                        salary_max = ?,
                        working_hours = ?,
                        holidays = ?,
                        work_location = ?,
                        business_description = ?,
                        job_description = ?,
                        requirements = ?,
                        hiring_count = ?,
                        contact_person = ?,
                        contact_email = ?,
                        page_url = ?,
                        employee_count = ?,
                        updated_at = ?,
                        is_new = 0
                    WHERE id = ?
                """, (
                    job_data.get('company', ''),
                    job_data.get('company_kana', ''),
                    job_data.get('postal_code', ''),
                    address_parts.get('pref', ''),
                    address_parts.get('city', ''),
                    address_parts.get('detail', ''),
                    job_data.get('phone_number', ''),
                    phone_normalized,
                    job_data.get('fax', ''),
                    job_data.get('title', ''),
                    job_data.get('employment_type', ''),
                    job_data.get('salary', ''),
                    self._parse_salary_min(job_data.get('salary', '')),
                    self._parse_salary_max(job_data.get('salary', '')),
                    job_data.get('working_hours', ''),
                    job_data.get('holidays', ''),
                    job_data.get('location', ''),
                    job_data.get('business_content', ''),
                    job_data.get('job_description', ''),
                    job_data.get('requirements', ''),
                    job_data.get('hiring_count'),
                    job_data.get('recruiter', ''),
                    job_data.get('recruiter_email', ''),
                    page_url_value,
                    job_data.get('employee_count'),
                    now,
                    existing['id']
                ))
                job_id = existing['id']
                is_new = False
            else:
                # 新規挿入
                cursor.execute("""
                    INSERT INTO jobs (
                        job_id, source_id, company_name, company_name_kana,
                        postal_code, address_pref, address_city, address_detail,
                        phone_number, phone_number_normalized, fax_number,
                        job_title, employment_type, salary, salary_min, salary_max,
                        working_hours, holidays, work_location,
                        business_description, job_description, requirements,
                        hiring_count, contact_person, contact_email, page_url,
                        employee_count, crawled_at, updated_at, is_new
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    job_id_value,
                    source_id,
                    job_data.get('company', ''),
                    job_data.get('company_kana', ''),
                    job_data.get('postal_code', ''),
                    address_parts.get('pref', ''),
                    address_parts.get('city', ''),
                    address_parts.get('detail', ''),
                    job_data.get('phone_number', ''),
                    phone_normalized,
                    job_data.get('fax', ''),
                    job_data.get('title', ''),
                    job_data.get('employment_type', ''),
                    job_data.get('salary', ''),
                    self._parse_salary_min(job_data.get('salary', '')),
                    self._parse_salary_max(job_data.get('salary', '')),
                    job_data.get('working_hours', ''),
                    job_data.get('holidays', ''),
                    job_data.get('location', ''),
                    job_data.get('business_content', ''),
                    job_data.get('job_description', ''),
                    job_data.get('requirements', ''),
                    job_data.get('hiring_count'),
                    job_data.get('recruiter', ''),
                    job_data.get('recruiter_email', ''),
                    page_url_value,
                    job_data.get('employee_count'),
                    now,
                    now,
                    True
                ))
                job_id = cursor.lastrowid
                is_new = True

            conn.commit()
            return job_id, is_new

    def save_jobs_bulk(self, jobs_data: List[Dict[str, Any]], source_name: str) -> int:
        """複数の求人情報を一括保存"""
        saved_count = 0
        for job_data in jobs_data:
            try:
                self.save_job(job_data, source_name)
                saved_count += 1
            except Exception as e:
                logger.warning(f"Failed to save job: {e}")
        return saved_count

    def get_jobs(
        self,
        source_name: Optional[str] = None,
        keyword: Optional[str] = None,
        prefecture: Optional[str] = None,
        employment_type: Optional[str] = None,
        is_new: Optional[bool] = None,
        is_filtered: Optional[bool] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """求人情報を検索"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()

            query = """
                SELECT j.*, s.display_name as source_display_name
                FROM jobs j
                JOIN sources s ON j.source_id = s.id
                WHERE 1=1
            """
            params = []

            if source_name:
                query += " AND s.name = ?"
                params.append(source_name)

            if keyword:
                query += " AND (j.job_title LIKE ? OR j.company_name LIKE ? OR j.job_description LIKE ?)"
                kw = f"%{keyword}%"
                params.extend([kw, kw, kw])

            if prefecture:
                query += " AND j.address_pref = ?"
                params.append(prefecture)

            if employment_type:
                query += " AND j.employment_type LIKE ?"
                params.append(f"%{employment_type}%")

            if is_new is not None:
                query += " AND j.is_new = ?"
                params.append(1 if is_new else 0)

            if is_filtered is not None:
                query += " AND j.is_filtered = ?"
                params.append(1 if is_filtered else 0)

            query += " ORDER BY j.crawled_at DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])

            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def get_jobs_by_ids(self, ids: List[int]) -> List[Dict[str, Any]]:
        """ID指定で求人情報を取得"""
        if not ids:
            return []

        placeholders = ",".join(["?"] * len(ids))
        query = f"""
            SELECT j.*, s.display_name as source_display_name
            FROM jobs j
            JOIN sources s ON j.source_id = s.id
            WHERE j.id IN ({placeholders})
            ORDER BY j.crawled_at DESC
        """

        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, ids)
            return [dict(row) for row in cursor.fetchall()]

    def get_job_count(
        self,
        source_name: Optional[str] = None,
        is_new: Optional[bool] = None,
        is_filtered: Optional[bool] = None
    ) -> int:
        """求人件数を取得"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()

            query = """
                SELECT COUNT(*) as count
                FROM jobs j
                JOIN sources s ON j.source_id = s.id
                WHERE 1=1
            """
            params = []

            if source_name:
                query += " AND s.name = ?"
                params.append(source_name)

            if is_new is not None:
                query += " AND j.is_new = ?"
                params.append(1 if is_new else 0)

            if is_filtered is not None:
                query += " AND j.is_filtered = ?"
                params.append(1 if is_filtered else 0)

            cursor.execute(query, params)
            return cursor.fetchone()['count']

    def _generate_fallback_id(self, job_data: Dict[str, Any]) -> str:
        """job_idもURLもない場合の同一性判定用ハッシュ"""
        # 変動しやすい給与は除外し、会社名・職種・勤務地で安定したキーを作る
        def norm(text: str) -> str:
            return re.sub(r'\s+', ' ', (text or '').strip().lower())

        fields = [
            norm(job_data.get('company') or job_data.get('company_name') or ''),
            norm(job_data.get('title') or job_data.get('job_title') or ''),
            norm(job_data.get('location') or job_data.get('work_location') or ''),
        ]
        key = "|".join(fields)
        return hashlib.md5(key.encode('utf-8')).hexdigest()

    def get_new_jobs_since(self, since: datetime, source_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """指定日時以降の新着求人を取得"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()

            query = """
                SELECT j.*, s.display_name as source_display_name
                FROM jobs j
                JOIN sources s ON j.source_id = s.id
                WHERE j.crawled_at >= ? AND j.is_new = 1
            """
            params = [since]

            if source_name:
                query += " AND s.name = ?"
                params.append(source_name)

            query += " ORDER BY j.crawled_at DESC"

            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def mark_jobs_as_old(self, before: datetime):
        """指定日時より前の求人を「新着でない」に更新"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE jobs SET is_new = 0
                WHERE crawled_at < ? AND is_new = 1
            """, (before,))
            conn.commit()
            return cursor.rowcount

    def delete_old_jobs(self, days: int = 90) -> int:
        """古い求人を削除"""
        cutoff = datetime.now() - timedelta(days=days)
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM jobs WHERE crawled_at < ?", (cutoff,))
            conn.commit()
            return cursor.rowcount

    def _normalize_url(self, url: str) -> str:
        """クエリ・フラグメントを除去し、末尾スラッシュを揃えたURLを返す"""
        if not url:
            return ""
        parsed = urlparse(url)
        path = parsed.path or "/"
        path = path.rstrip("/") or "/"
        return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))

    def _normalize_phone(self, phone: str) -> str:
        """電話番号を正規化（数字のみ）"""
        if not phone:
            return ""
        # 全角→半角変換
        trans = str.maketrans('０１２３４５６７８９', '0123456789')
        phone = phone.translate(trans)
        # 数字以外を除去
        return re.sub(r'[^\d]', '', phone)

    def _parse_address(self, address: str) -> Dict[str, str]:
        """住所を都道府県・市区町村・詳細に分解"""
        result = {'pref': '', 'city': '', 'detail': ''}
        if not address:
            return result

        # 都道府県パターン
        pref_pattern = r'^(北海道|東京都|大阪府|京都府|.{2,3}県)'
        match = re.match(pref_pattern, address)
        if match:
            result['pref'] = match.group(1)
            address = address[len(result['pref']):]

        # 市区町村パターン
        city_pattern = r'^(.+?[市区町村])'
        match = re.match(city_pattern, address)
        if match:
            result['city'] = match.group(1)
            result['detail'] = address[len(result['city']):]
        else:
            result['detail'] = address

        return result

    def _parse_salary_min(self, salary: str) -> Optional[int]:
        """給与文字列から最低額を抽出"""
        if not salary:
            return None
        # 「時給1000円」「月給20万円」などから数値を抽出
        match = re.search(r'(\d{1,3}(?:,\d{3})*|\d+)(?:円|万円)', salary)
        if match:
            value = int(match.group(1).replace(',', ''))
            if '万' in salary:
                value *= 10000
            return value
        return None

    def _parse_salary_max(self, salary: str) -> Optional[int]:
        """給与文字列から最高額を抽出"""
        if not salary:
            return None
        # 「〜」「-」で区切られた範囲の後半を取得
        match = re.search(r'[〜~\-～].*?(\d{1,3}(?:,\d{3})*|\d+)(?:円|万円)', salary)
        if match:
            value = int(match.group(1).replace(',', ''))
            if '万' in salary:
                value *= 10000
            return value
        return self._parse_salary_min(salary)  # 範囲がなければ最低額と同じ
