"""
Upload Service

Business logic layer for file upload operations.
Handles:
- File validation (format, size, encoding, structure)
- Schema detection from file contents
- File processing and preview generation
- Hive external table creation orchestration

Supports: CSV, Excel (xlsx/xls), JSON, Parquet, Text files
"""

import os
import csv
import json
import io
import logging
import chardet
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict, field
from datetime import datetime

from django.conf import settings
from ..repositories.upload_kudu_repository import upload_kudu_repository, UploadKuduRepository
from ..repositories.datasource_repository import datasource_repository

logger = logging.getLogger('upload')


def build_average_cost_sql(
    quantity_expr: str,
    cost_expr: str,
    fallback_expr: str,
    dec_type: str = 'DECIMAL(30,8)',
) -> str:
    """Return SQL to derive average cost from cost / quantity when available."""
    return f"""
                    CASE
                        WHEN CAST({quantity_expr} AS {dec_type}) > 0
                             AND {cost_expr} IS NOT NULL
                             AND CAST({cost_expr} AS {dec_type}) > 0
                            THEN CAST(
                                CAST({cost_expr} AS {dec_type}) / CAST({quantity_expr} AS {dec_type})
                            AS {dec_type})
                        WHEN {fallback_expr} IS NOT NULL
                            THEN CAST({fallback_expr} AS {dec_type})
                        ELSE CAST(0 AS {dec_type})
                    END"""


# ---------------------------------------------------------------------------
# ETL cancellation registry
# ---------------------------------------------------------------------------
# Lets the "Stop" button on the upload detail page signal a running
# background ETL thread (started via a bare threading.Thread in
# upload/views.py, not Celery) to stop between staging steps rather than
# running to completion or hanging indefinitely. A plain in-process dict is
# sufficient here since the thread and the request that can cancel it always
# run in the same process/worker.
import threading as _etl_threading

_etl_cancel_events: Dict[str, '_etl_threading.Event'] = {}
_etl_cancel_lock = _etl_threading.Lock()


class EtlCancelled(Exception):
    """Raised inside run_position_etl when a Stop request is observed between steps."""
    pass


def register_etl_run(upload_id: str) -> '_etl_threading.Event':
    """Create and register a cancellation Event for a new ETL run. Call this
    right before starting the background thread; pass nothing further into
    run_position_etl itself -- it looks itself up by upload_id."""
    ev = _etl_threading.Event()
    with _etl_cancel_lock:
        _etl_cancel_events[upload_id] = ev
    return ev


def unregister_etl_run(upload_id: str) -> None:
    """Remove the cancellation Event once a run has finished (success, failure,
    or cancellation) so the registry doesn't grow unbounded."""
    with _etl_cancel_lock:
        _etl_cancel_events.pop(upload_id, None)


def request_etl_cancel(upload_id: str) -> bool:
    """Signal a running ETL to stop at its next step boundary.
    Returns True if a live run was found and signalled, False if there was
    nothing registered for this upload_id (e.g. it already finished)."""
    with _etl_cancel_lock:
        ev = _etl_cancel_events.get(upload_id)
    if ev is None:
        return False
    ev.set()
    return True


def is_etl_cancel_requested(upload_id: str) -> bool:
    with _etl_cancel_lock:
        ev = _etl_cancel_events.get(upload_id)
    return ev.is_set() if ev is not None else False


@dataclass
class FileValidationResult:
    """Result of file validation."""
    is_valid: bool = False
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    file_type: str = ''
    encoding: str = 'UTF-8'
    delimiter: str = ','
    has_header: bool = True
    row_count: int = 0
    column_count: int = 0
    columns: List[Dict[str, str]] = field(default_factory=list)
    sample_data: List[Dict[str, Any]] = field(default_factory=list)
    file_size: int = 0
    # Full parsed data rows (all rows, not capped). Populated by
    # validate_with_datasource_config so upload_create can ingest
    # immediately without re-reading the file.
    all_data: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class UploadDTO:
    """Data Transfer Object for Upload."""
    upload_id: str = ''
    file_name: str = ''
    original_file_name: str = ''
    file_path: str = ''
    file_size: int = 0
    file_type: str = ''
    mime_type: str = ''
    row_count: int = 0
    column_count: int = 0
    delimiter: str = ','
    has_header: bool = True
    encoding: str = 'UTF-8'
    description: str = ''
    target_table_name: str = ''
    target_database: str = settings.IMPALA_CONFIG['DATABASE']
    hdfs_path: str = ''
    schema: List[Dict[str, str]] = field(default_factory=list)
    sample_data: List[Dict[str, Any]] = field(default_factory=list)
    validation_errors: List[str] = field(default_factory=list)
    status: str = 'PENDING'

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class FileValidationService:
    """Service for validating uploaded files."""

    # Supported file extensions and their MIME types
    SUPPORTED_EXTENSIONS = {
        'csv': ['text/csv', 'application/csv', 'text/plain'],
        'tsv': ['text/tab-separated-values', 'text/plain'],
        'txt': ['text/plain'],
        'json': ['application/json', 'text/json'],
        'parquet': ['application/octet-stream', 'application/parquet'],
        'xlsx': ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'],
        'xls': ['application/vnd.ms-excel'],
    }

    # Maximum file size (100 MB)
    MAX_FILE_SIZE = 100 * 1024 * 1024

    # Maximum rows to preview
    MAX_PREVIEW_ROWS = 100

    # Common delimiters to detect
    DELIMITERS = [',', '\t', ';', '|', ':']

    def __init__(self):
        self.repository = upload_kudu_repository

    def validate_file(self, file_obj, file_name: str) -> FileValidationResult:
        """
        Validate uploaded file.

        Args:
            file_obj: File object (Django UploadedFile or file-like object)
            file_name: Original file name

        Returns:
            FileValidationResult with validation status and detected properties
        """
        result = FileValidationResult()
        result.file_size = 0

        try:
            # Get file extension
            ext = self._get_extension(file_name)
            result.file_type = ext

            # Validate extension
            if ext not in self.SUPPORTED_EXTENSIONS:
                result.errors.append(f"Unsupported file format: .{ext}. Supported formats: {', '.join(self.SUPPORTED_EXTENSIONS.keys())}")
                return result

            # Read file content
            file_obj.seek(0)
            content = file_obj.read()
            result.file_size = len(content)

            # Validate file size
            if result.file_size > self.MAX_FILE_SIZE:
                result.errors.append(f"File size ({self._format_size(result.file_size)}) exceeds maximum allowed ({self._format_size(self.MAX_FILE_SIZE)})")
                return result

            if result.file_size == 0:
                result.errors.append("File is empty")
                return result

            # Detect encoding
            result.encoding = self._detect_encoding(content)

            # Validate and parse based on file type
            if ext in ['csv', 'tsv', 'txt']:
                self._validate_csv(content, result, ext)
            elif ext == 'json':
                self._validate_json(content, result)
            elif ext in ['xlsx', 'xls']:
                self._validate_excel(file_obj, result)
            elif ext == 'parquet':
                self._validate_parquet(file_obj, result)

            # If no errors, mark as valid
            if not result.errors:
                result.is_valid = True

        except Exception as e:
            logger.error(f"File validation error: {str(e)}")
            result.errors.append(f"Validation error: {str(e)}")

        return result

    def _get_extension(self, file_name: str) -> str:
        """Get file extension in lowercase."""
        if '.' in file_name:
            return file_name.rsplit('.', 1)[-1].lower()
        return ''

    def _format_size(self, size_bytes: int) -> str:
        """Format file size for display."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"

    def _detect_encoding(self, content: bytes) -> str:
        """Detect file encoding."""
        try:
            detected = chardet.detect(content[:10000])  # Check first 10KB
            encoding = detected.get('encoding', 'UTF-8')
            confidence = detected.get('confidence', 0)

            if confidence < 0.5:
                return 'UTF-8'  # Default to UTF-8 if low confidence

            # Normalize encoding names
            encoding_map = {
                'ascii': 'UTF-8',
                'iso-8859-1': 'ISO-8859-1',
                'windows-1252': 'Windows-1252',
            }
            return encoding_map.get(encoding.lower(), encoding) if encoding else 'UTF-8'

        except Exception:
            return 'UTF-8'

    def _detect_delimiter(self, sample_lines: List[str]) -> str:
        """Detect CSV delimiter from sample lines."""
        if not sample_lines:
            return ','

        delimiter_counts = {}
        for delim in self.DELIMITERS:
            counts = [line.count(delim) for line in sample_lines[:10]]
            if counts and min(counts) > 0 and max(counts) == min(counts):
                # Consistent count across lines suggests this is the delimiter
                delimiter_counts[delim] = sum(counts)

        if delimiter_counts:
            return max(delimiter_counts, key=delimiter_counts.get)

        return ','

    def _validate_csv(self, content: bytes, result: FileValidationResult, ext: str) -> None:
        """Validate CSV/TSV/TXT file."""
        try:
            # Decode content
            text_content = content.decode(result.encoding, errors='replace')
            lines = text_content.splitlines()

            if not lines:
                result.errors.append("File contains no data")
                return

            # Detect delimiter
            if ext == 'tsv':
                result.delimiter = '\t'
            else:
                result.delimiter = self._detect_delimiter(lines)

            # Parse CSV
            reader = csv.reader(io.StringIO(text_content), delimiter=result.delimiter)
            rows = list(reader)

            if not rows:
                result.errors.append("No data rows found")
                return

            # Detect header
            first_row = rows[0]
            result.has_header = self._detect_header(first_row, rows[1] if len(rows) > 1 else None)

            # Get column info
            header_row = rows[0] if result.has_header else [f'col_{i+1}' for i in range(len(rows[0]))]
            data_rows = rows[1:] if result.has_header else rows

            result.column_count = len(header_row)
            result.row_count = len(data_rows)

            # Infer column types
            result.columns = self._infer_column_types(header_row, data_rows[:100])

            # Generate sample data using cleaned column names from schema
            # IMPORTANT: Use the cleaned names from result.columns to match the template
            cleaned_col_names = [col['name'] for col in result.columns]
            result.sample_data = []
            for row in data_rows[:self.MAX_PREVIEW_ROWS]:
                row_dict = {}
                for idx, col_name in enumerate(cleaned_col_names):
                    if idx < len(row):
                        row_dict[col_name] = row[idx]
                    else:
                        row_dict[col_name] = ''
                result.sample_data.append(row_dict)

                # Track inconsistent column counts
                if len(row) > len(cleaned_col_names):
                    if "inconsistent column count" not in str(result.warnings):
                        result.warnings.append(f"Some rows have inconsistent column count")

            # Validate consistency
            if result.row_count == 0:
                result.errors.append("File contains only a header row with no data. Please add data rows before uploading.")

            # Check for duplicate rows in source file
            duplicate_count = self._count_duplicate_rows(data_rows)
            if duplicate_count > 0:
                result.warnings.append(f"Found {duplicate_count} duplicate row(s) in source file")

        except Exception as e:
            result.errors.append(f"CSV parsing error: {str(e)}")

    def _count_duplicate_rows(self, rows: List[List[str]]) -> int:
        """
        Count duplicate rows in the data.

        Args:
            rows: List of data rows (each row is a list of values)

        Returns:
            Number of duplicate rows found
        """
        try:
            # Convert rows to tuples for hashing
            row_tuples = [tuple(row) for row in rows]
            unique_rows = set(row_tuples)

            # If unique count differs from total, we have duplicates
            total_rows = len(row_tuples)
            unique_count = len(unique_rows)
            duplicate_count = total_rows - unique_count

            if duplicate_count > 0:
                logger.warning(f"Found {duplicate_count} duplicate rows in source file")

            return duplicate_count
        except Exception as e:
            logger.error(f"Error counting duplicates: {str(e)}")
            return 0

    def _detect_header(self, first_row: List[str], second_row: Optional[List[str]]) -> bool:
        """Detect if first row is a header."""
        if not first_row:
            return False

        # Check if all values in first row look like column names
        name_like = 0
        for val in first_row:
            val = str(val).strip()
            # Column names are typically: short, no numbers only, no special chars
            if val and len(val) < 50 and not val.replace('.', '').replace('-', '').isdigit():
                name_like += 1

        # If most look like names, consider it a header
        return name_like >= len(first_row) * 0.7

    def _infer_column_types(self, headers: List[str], sample_rows: List[List[str]],
                              force_string: bool = False) -> List[Dict[str, str]]:
        """
        Infer Hive column types from sample data.

        Args:
            headers: List of column header names
            sample_rows: Sample data rows for type inference
            force_string: If True, all columns will be STRING type (for external tables)

        Returns:
            List of column definitions with name, type, nullable
        """
        columns = []

        for i, header in enumerate(headers):
            col_name = self._clean_column_name(header)
            col_type = 'STRING'  # Default type

            # Only infer type if not forcing STRING
            if not force_string:
                # Collect non-empty values from this column
                values = []
                for row in sample_rows:
                    if i < len(row) and row[i].strip():
                        values.append(row[i].strip())

                if values:
                    col_type = self._infer_type(values)

            columns.append({
                'name': col_name,
                'original_name': header,
                'type': col_type,
                'nullable': True
            })

        return columns

    def _clean_column_name(self, name: str) -> str:
        """Clean column name for Hive compatibility."""
        # Remove leading/trailing whitespace
        clean = name.strip()
        # Replace special chars with underscore
        clean = ''.join(c if c.isalnum() or c == '_' else '_' for c in clean)
        # Remove consecutive underscores
        while '__' in clean:
            clean = clean.replace('__', '_')
        # Remove leading underscores
        clean = clean.lstrip('_')
        # Ensure not empty
        if not clean:
            clean = 'column'
        # Ensure doesn't start with number
        if clean[0].isdigit():
            clean = 'col_' + clean
        return clean.lower()

    def _infer_type(self, values: List[str]) -> str:
        """Infer Hive type from sample values."""
        # Try to determine type from values
        int_count = 0
        float_count = 0
        bool_count = 0
        date_count = 0

        for val in values[:50]:  # Check first 50 values
            val_lower = val.lower()

            # Check boolean
            if val_lower in ['true', 'false', 'yes', 'no', '1', '0']:
                bool_count += 1
                continue

            # Check integer
            try:
                int(val.replace(',', ''))
                int_count += 1
                continue
            except ValueError:
                pass

            # Check float
            try:
                float(val.replace(',', ''))
                float_count += 1
                continue
            except ValueError:
                pass

            # Check date patterns
            if self._looks_like_date(val):
                date_count += 1

        total = len(values[:50])
        threshold = 0.8  # 80% of values must match type

        if bool_count / total >= threshold:
            return 'BOOLEAN'
        if int_count / total >= threshold:
            return 'BIGINT'
        if (int_count + float_count) / total >= threshold:
            return 'DECIMAL(30,8)'
        if date_count / total >= threshold:
            return 'STRING'  # Keep dates as STRING for flexibility

        return 'STRING'

    def _looks_like_date(self, val: str) -> bool:
        """Check if value looks like a date."""
        import re
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # 2024-01-15
            r'\d{2}/\d{2}/\d{4}',  # 01/15/2024
            r'\d{2}-\d{2}-\d{4}',  # 15-01-2024
            r'\d{4}/\d{2}/\d{2}',  # 2024/01/15
        ]
        for pattern in date_patterns:
            if re.match(pattern, val):
                return True
        return False

    def _validate_json(self, content: bytes, result: FileValidationResult) -> None:
        """Validate JSON file."""
        try:
            text_content = content.decode(result.encoding, errors='replace')
            data = json.loads(text_content)

            result.delimiter = ''
            result.has_header = False

            if isinstance(data, list):
                result.row_count = len(data)
                if data and isinstance(data[0], dict):
                    result.column_count = len(data[0].keys())
                    result.columns = [
                        {'name': self._clean_column_name(k), 'original_name': k, 'type': 'STRING', 'nullable': True}
                        for k in data[0].keys()
                    ]
                    result.sample_data = data[:self.MAX_PREVIEW_ROWS]
            elif isinstance(data, dict):
                # Single object or nested structure
                result.row_count = 1
                result.column_count = len(data.keys())
                result.columns = [
                    {'name': self._clean_column_name(k), 'original_name': k, 'type': 'STRING', 'nullable': True}
                    for k in data.keys()
                ]
                result.sample_data = [data]
            else:
                result.errors.append("JSON must be an array of objects or a single object")

        except json.JSONDecodeError as e:
            result.errors.append(f"Invalid JSON: {str(e)}")

    def _validate_excel(self, file_obj, result: FileValidationResult) -> None:
        """Validate Excel file (xlsx/xls)."""
        try:
            import openpyxl

            file_obj.seek(0)
            wb = openpyxl.load_workbook(file_obj, read_only=True, data_only=True)
            sheet = wb.active

            result.delimiter = ''

            # Read all rows
            rows = list(sheet.iter_rows(values_only=True))

            if not rows:
                result.errors.append("Excel file is empty")
                return

            # First row as header
            header_row = [str(cell) if cell is not None else f'col_{i+1}' for i, cell in enumerate(rows[0])]
            data_rows = rows[1:]

            result.has_header = True
            result.column_count = len(header_row)
            result.row_count = len(data_rows)

            if result.row_count == 0:
                result.errors.append("File contains only a header row with no data. Please add data rows before uploading.")
                return

            # Infer column types
            str_rows = [[str(cell) if cell is not None else '' for cell in row] for row in data_rows[:100]]
            result.columns = self._infer_column_types(header_row, str_rows)

            # Generate sample data using cleaned column names from schema
            cleaned_col_names = [col['name'] for col in result.columns]
            result.sample_data = []
            for row in data_rows[:self.MAX_PREVIEW_ROWS]:
                row_dict = {}
                for i, col_name in enumerate(cleaned_col_names):
                    if i < len(row):
                        row_dict[col_name] = str(row[i]) if row[i] is not None else ''
                    else:
                        row_dict[col_name] = ''
                result.sample_data.append(row_dict)

            wb.close()

        except ImportError:
            result.errors.append("Excel support requires openpyxl library. Install with: pip install openpyxl")
        except Exception as e:
            result.errors.append(f"Excel parsing error: {str(e)}")

    def _validate_parquet(self, file_obj, result: FileValidationResult) -> None:
        """Validate Parquet file."""
        try:
            import pyarrow.parquet as pq

            file_obj.seek(0)
            table = pq.read_table(file_obj)

            result.delimiter = ''
            result.has_header = True
            result.row_count = table.num_rows
            result.column_count = table.num_columns

            # Get schema
            result.columns = []
            col_name_map = {}  # Map original -> cleaned names
            for field in table.schema:
                hive_type = self._arrow_to_hive_type(str(field.type))
                cleaned_name = self._clean_column_name(field.name)
                col_name_map[field.name] = cleaned_name
                result.columns.append({
                    'name': cleaned_name,
                    'original_name': field.name,
                    'type': hive_type,
                    'nullable': field.nullable
                })

            # Sample data - rename columns to match cleaned schema names
            df = table.slice(0, min(self.MAX_PREVIEW_ROWS, table.num_rows)).to_pandas()
            df = df.rename(columns=col_name_map)
            result.sample_data = df.to_dict('records')

        except ImportError:
            result.errors.append("Parquet support requires pyarrow library. Install with: pip install pyarrow")
        except Exception as e:
            result.errors.append(f"Parquet parsing error: {str(e)}")

    def _arrow_to_hive_type(self, arrow_type: str) -> str:
        """Convert PyArrow type to Hive type."""
        type_map = {
            'int8': 'TINYINT',
            'int16': 'SMALLINT',
            'int32': 'INT',
            'int64': 'BIGINT',
            'float': 'FLOAT',
            'double': 'DOUBLE',
            'bool': 'BOOLEAN',
            'string': 'STRING',
            'date32': 'DATE',
            'timestamp': 'TIMESTAMP',
        }
        arrow_lower = arrow_type.lower()
        for key, val in type_map.items():
            if key in arrow_lower:
                return val
        return 'STRING'


def _fix_mojibake(s: str) -> str:
    """
    Recover UTF-8 text that was mis-decoded as Latin-1 (mojibake).
    e.g. 'TÃ¼rkiye' → 'Türkiye', 'RÃ©union' → 'Réunion', 'CÃ´te' → 'Côte'.
    Safe: if s is already valid UTF-8/ASCII the encode→decode round-trip raises
    UnicodeDecodeError and we return s unchanged.
    """
    try:
        return s.encode('latin-1').decode('utf-8')
    except (UnicodeDecodeError, UnicodeEncodeError):
        return s


def _normalize_country_key(name: str) -> str:
    """
    Normalize a country full_name to a safe ASCII key for SQL comparison.

    Steps (must mirror what _build_norm_expr() does in Impala):
      1. Fix mojibake — gmp_cis_sta_dly_country stores UTF-8 bytes as Latin-1
         ('TÃ¼rkiye' → 'Türkiye', 'CÃ´te' → 'Côte', etc.)
      2. Strip accents via NFKD decomposition (Ô→O, é→e, Ç→C, etc.)
      3. Drop non-ASCII bytes
      4. Upper-case
      5. Punctuation ( ) , . / : – — - → space
      6. Apostrophe / double-quote / backslash → space
      7. Collapse whitespace
    """
    import re
    import unicodedata
    s = _fix_mojibake(name)
    s = unicodedata.normalize('NFKD', s)
    s = s.encode('ascii', errors='ignore').decode('ascii')
    s = s.upper()
    s = re.sub(r"[().,/:–—-]+",      ' ', s)
    s = re.sub(r"['‘’ʼ\"\\\\]+", ' ', s)  # all apostrophe variants + quotes
    s = re.sub(r'\s+',               ' ', s).strip()
    return s


def _trim_country_qualifier(s: str) -> str:
    """Trim at the first '(' or ',' — mirrors the upload-side _resolve_country()
    trim, so a LUT full_name carrying its own trailing qualifier (e.g.
    'Taiwan (Republic of China)', 'Korea (South), Republic of') still matches
    a plain upload value like 'Taiwan'. Comparison-only: the stored
    gmp_cis_sta_dly_country.full_name value is never altered."""
    import re
    return re.split(r'[(,]', s, 1)[0].strip()


def _build_country_map_for_format5(impala_manager, db: str, processing_date: str, src_id: str) -> dict:
    """
    Fetch the complete country name → label mapping from gmp_cis_sta_dly_country.

    The table has ~247 rows and often contains mojibake (UTF-8 bytes stored as
    Latin-1, e.g. 'TÃ¼rkiye' instead of 'Türkiye').  Upload CSVs may arrive
    with clean ASCII ('Turkiye'), proper Unicode ('Türkiye'), or the same
    mojibake as the DB.

    To match all variants without fragile Impala-side regex, we register up to
    FIVE keys per label in the returned dict:
      1. raw     — UPPER(TRIM(full_name)) exactly as stored in DB (mojibake form)
      2. fixed   — mojibake decoded back to proper Unicode, then UPPER/TRIM
      3. norm    — fully ASCII-normalized (NFKD accent-strip + punct → space)
      4. trimmed — raw, truncated at the first "(" or "," (e.g. a DB full_name
                   of 'Taiwan (Republic of China)' also registers 'TAIWAN')
      5. trimmed_fixed — same truncation applied to the mojibake-fixed form

    Variants 4/5 are registered with setdefault (not overwrite) so an exact
    raw/fixed/norm match for another row's full name always wins over a
    truncated collision, regardless of row iteration order.

    The Impala CASE WHEN then uses a plain UPPER(TRIM(col)) comparison with no
    regex at all — simple, fast, and immune to encoding edge-cases.

    Returns dict: {key_variant: label}  (multiple keys may map to same label)
    """
    rows = impala_manager.execute_query(
        f"""
        SELECT UPPER(TRIM(full_name)) AS full_name, MIN(label) AS label
        FROM {db}.gmp_cis_sta_dly_country
        WHERE processing_date = (
            SELECT MAX(processing_date) FROM {db}.gmp_cis_sta_dly_country
        )
        GROUP BY UPPER(TRIM(full_name))
        """,
        database=db
    ) or []

    def _is_safe_key(s: str) -> bool:
        """Return True if s can be embedded in a SQL string literal without quoting issues.
        Keys with apostrophes/backslashes are technically escapable but the CSV is
        unlikely to contain them verbatim, so skip to avoid false matches."""
        return bool(s) and "'" not in s and "\\" not in s

    result = {}
    for r in rows:
        raw = (r.get('full_name') or '').strip().upper()
        label = (r.get('label') or '').strip()
        if not raw or not label:
            continue

        # Variant 1 — raw DB value (mojibake, already UPPER/TRIM'd by SQL).
        # Only register if it contains no apostrophes — otherwise a CSV value
        # is extremely unlikely to match the raw DB form verbatim.
        if _is_safe_key(raw):
            result[raw] = label

        # Variant 2 — mojibake decoded to proper Unicode.
        # e.g. 'TÃ¼RKIYE' → 'TÜRKIYE'
        fixed = _fix_mojibake(raw).upper().strip()
        if fixed and fixed != raw and _is_safe_key(fixed):
            result[fixed] = label

        # Variant 3 — fully ASCII-normalized (NFKD accent-strip + punct → space).
        # e.g. 'TÜRKIYE' → 'TURKIYE', "CÔTE D'IVOIRE" → 'COTE D IVOIRE'
        # The norm form always has apostrophes/parens removed so _is_safe_key is
        # always True here — this is the universal fallback variant.
        norm = _normalize_country_key(raw)
        if norm and norm not in (raw, fixed):
            result[norm] = label

        # Variant 4 — raw truncated at first "(" or "," (comparison-only;
        # does not alter what's stored in gmp_cis_sta_dly_country).
        trimmed = _trim_country_qualifier(raw)
        if trimmed and trimmed != raw and _is_safe_key(trimmed):
            result.setdefault(trimmed, label)

        # Variant 5 — mojibake-fixed form, same truncation.
        trimmed_fixed = _trim_country_qualifier(fixed)
        if trimmed_fixed and trimmed_fixed not in (raw, fixed, trimmed) and _is_safe_key(trimmed_fixed):
            result.setdefault(trimmed_fixed, label)

    return result


def _inject_country_case_when(std_select: str, country_map: dict, db: str) -> str:
    """
    Replace the country_lut CTE + 4 LEFT JOINs in the format-5 std_select with
    inline CASE WHEN expressions using pre-fetched literal values.
    Removes gmp_cis_sta_dly_country entirely from the INSERT execution plan.

    Both the upload column and the dict keys are normalized with the same
    regexp_replace (strip apostrophes, parens, hyphens → space, collapse spaces)
    so no special characters ever appear inside Impala string literals.
    """
    import re

    def _sql_str(s: str) -> str:
        """
        Escape a value for embedding in an Impala single-quoted string literal.
        Handles all quote variants: ASCII apostrophe (U+0027), right single
        quotation mark (U+2019 ’), left single quotation mark (U+2018),
        and prime (U+02BC) — any of these break Impala string literals if bare.
        """
        # Normalise all curly/fancy apostrophes to ASCII first
        s = s.replace('’', "'").replace('‘', "'").replace('ʼ', "'")
        # Impala uses C-style \' escaping, not doubled quotes — backslash first
        # so the backslash just introduced by the quote-escape isn't re-escaped.
        s = s.replace("\\", "\\\\")
        return s.replace("'", "\\'")

    def _case_expr(col: str) -> str:
        """
        Build a CASE WHEN expression comparing UPPER(TRIM(col)) against every
        key variant in country_map.  No regex on the Impala side — all encoding
        variants (raw mojibake, fixed Unicode, ASCII-normalized) are pre-registered
        as separate keys in the dict by _build_country_map_for_format5(), so a
        plain string equality check is sufficient and fast.
        """
        if not country_map:
            return "CAST(NULL AS STRING)"
        norm_col = f"UPPER(TRIM(CAST({col} AS STRING)))"
        branches = '\n'.join(
            f"        WHEN {norm_col} = '{_sql_str(k)}' THEN '{_sql_str(v)}'"
            for k, v in country_map.items()
        )
        return f"CASE\n{branches}\n        ELSE CAST(NULL AS STRING)\n    END"

    # Remove the WITH country_lut AS (...) CTE block
    std_select = re.sub(
        r'WITH\s+country_lut\s+AS\s*\(.*?\)\s*(?=SELECT)',
        '',
        std_select,
        flags=re.DOTALL | re.IGNORECASE,
    )
    # Replace cn_*.label column references with CASE WHEN literals.
    # Use lambda replacements so re.sub never interprets backslashes in the
    # generated CASE WHEN expression (e.g. country names with \ would otherwise
    # raise "bad escape" or silently corrupt the output).
    exc_expr = _case_expr('p5.country_of_exchange')
    inc_expr = _case_expr('p5.country_of_incorporation')
    rsk_expr = _case_expr('p5.country_of_risk')
    opr_expr = _case_expr('p5.country_of_operation')

    std_select = re.sub(
        r'cn_exc\.label\s+AS\s+exchange',
        lambda m: exc_expr + '  AS exchange',
        std_select
    )
    std_select = re.sub(
        r'cn_exc\.label\s+AS\s+country_of_exchange',
        lambda m: exc_expr + '  AS country_of_exchange',
        std_select
    )
    std_select = re.sub(
        r'cn_inc\.label\s+AS\s+country_of_incorporation',
        lambda m: inc_expr + '  AS country_of_incorporation',
        std_select
    )
    std_select = re.sub(
        r'cn_rsk\.label\s+AS\s+country_of_risk',
        lambda m: rsk_expr + '  AS country_of_risk',
        std_select
    )
    std_select = re.sub(
        r'cn_opr\.label\s+AS\s+country_of_operation',
        lambda m: opr_expr + '  AS country_of_operation',
        std_select
    )
    # Remove the LEFT JOIN country_lut lines
    std_select = re.sub(
        r'LEFT\s+JOIN\s+country_lut\s+cn_\w+\s+ON\s+cn_\w+\.full_name\s*=\s*UPPER\(TRIM\(CAST\(p5\.\w+\s+AS\s+STRING\)\)\)\s*',
        '',
        std_select,
        flags=re.IGNORECASE
    )
    return std_select


class UploadService:
    """Service class for upload operations."""

    # Class-level cache for abbreviated (normalized) cis_security name → list of
    # candidate security dicts (security_id, security_name, isin, exchange_code,
    # country_of_exchange, currency_code). A list (not a single id) so tiers 5/9
    # of the security-matching cascade can detect MULTIPLE_MATCH on a normalized
    # name collision instead of silently keeping the first one found.
    # Shared across all instances/ETL runs; rebuilt when TTL expires.
    _cis_abbrev_cache: dict = {}
    _cis_abbrev_cache_ts: float = 0.0
    _CIS_ABBREV_CACHE_TTL: int = 300  # seconds (5 minutes)

    def __init__(self):
        self.repository = upload_kudu_repository
        self.validation_service = FileValidationService()

    def get_all_uploads(
        self,
        status: Optional[str] = None,
        file_type: Optional[str] = None,
        search: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get all uploads with optional filters."""
        return self.repository.get_all_uploads(
            status=status,
            file_type=file_type,
            search=search
        )

    def get_upload_by_id(self, upload_id: str) -> Optional[Dict[str, Any]]:
        """Get upload by ID."""
        return self.repository.get_upload_by_id(upload_id)

    def validate_and_create_upload(
        self,
        file_obj,
        file_name: str,
        description: str,
        created_by: str
    ) -> Tuple[Optional[str], FileValidationResult]:
        """
        Validate file and create upload record.

        Returns:
            Tuple of (upload_id or None, FileValidationResult)
        """
        # Validate file
        validation_result = self.validation_service.validate_file(file_obj, file_name)

        if not validation_result.is_valid:
            return None, validation_result

        # Create upload DTO
        upload_dto = UploadDTO(
            file_name=file_name,
            original_file_name=file_name,
            file_size=validation_result.file_size,
            file_type=validation_result.file_type,
            row_count=validation_result.row_count,
            column_count=validation_result.column_count,
            delimiter=validation_result.delimiter,
            has_header=validation_result.has_header,
            encoding=validation_result.encoding,
            description=description,
            schema=validation_result.columns,
            sample_data=validation_result.sample_data,
            validation_errors=validation_result.errors + validation_result.warnings,
            status=UploadKuduRepository.STATUS_VALIDATED
        )

        # Create upload record
        upload_id = self.repository.create_upload(upload_dto.to_dict(), created_by)

        return upload_id, validation_result

    def update_upload(
        self,
        upload_id: str,
        update_data: Dict[str, Any],
        updated_by: str
    ) -> bool:
        """Update upload record."""
        return self.repository.update_upload(upload_id, update_data, updated_by)

    def delete_upload(self, upload_id: str, deleted_by: str) -> bool:
        """Soft delete upload."""
        return self.repository.soft_delete(upload_id, deleted_by)

    def ingest_to_hive(
        self,
        upload_id: str,
        table_name: Optional[str],
        columns: List[Dict[str, str]],
        file_path: str,
        updated_by: str
    ) -> Tuple[bool, str]:
        """
        Ingest uploaded file to Hive external table.

        Args:
            upload_id: Upload record ID
            table_name: Custom table name (or None to auto-generate)
            columns: Column definitions
            file_path: HDFS path to file
            updated_by: User performing ingestion

        Returns:
            Tuple of (success, message)
        """
        try:
            # Get upload record
            upload = self.repository.get_upload_by_id(upload_id)
            if not upload:
                return False, "Upload not found"

            # Update status to ingesting
            self.repository.update_status(upload_id, UploadKuduRepository.STATUS_INGESTING, updated_by)

            # Generate table name if not provided
            if not table_name:
                table_name = upload.get('target_table_name') or self.repository.generate_table_name(upload.get('file_name', 'upload'))

            # Get file properties
            file_type = upload.get('file_type', 'csv')
            delimiter = upload.get('delimiter', ',')
            has_header = upload.get('has_header', True)
            hdfs_path = file_path or upload.get('hdfs_path', '')

            # Create external table
            success = self.repository.create_external_table(
                table_name=table_name,
                columns=columns,
                hdfs_path=hdfs_path,
                file_format=file_type,
                delimiter=delimiter,
                has_header=has_header
            )

            if success:
                # Update upload record
                self.repository.update_upload(upload_id, {
                    'status': UploadKuduRepository.STATUS_COMPLETED,
                    'target_table_name': table_name,
                    'hdfs_path': hdfs_path
                }, updated_by)

                # Get row count from new table
                row_count = self.repository.get_table_row_count(table_name)
                if row_count > 0:
                    self.repository.update_upload(upload_id, {'row_count': row_count}, updated_by)

                return True, f"Successfully created table {table_name} with {row_count} rows"
            else:
                self.repository.update_status(
                    upload_id,
                    UploadKuduRepository.STATUS_FAILED,
                    updated_by,
                    "Failed to create external table"
                )
                return False, "Failed to create external table"

        except Exception as e:
            logger.error(f"Ingestion error: {str(e)}")
            safe_err = f"{type(e).__name__}: {str(e)[:200]}"
            self.repository.update_status(upload_id, UploadKuduRepository.STATUS_FAILED, updated_by, safe_err)
            return False, f"Ingestion error: {str(e)}"

    def get_statistics(self) -> Dict[str, Any]:
        """Get upload statistics."""
        return self.repository.get_upload_statistics()

    def get_status_choices(self) -> List[Tuple[str, str]]:
        """Get status choices for dropdowns."""
        return [
            (UploadKuduRepository.STATUS_PENDING, 'Pending'),
            (UploadKuduRepository.STATUS_VALIDATING, 'Validating'),
            (UploadKuduRepository.STATUS_VALIDATED, 'Validated'),
            (UploadKuduRepository.STATUS_VALIDATION_FAILED, 'Validation Failed'),
            (UploadKuduRepository.STATUS_INGESTING, 'Ingesting'),
            (UploadKuduRepository.STATUS_COMPLETED, 'Completed'),
            (UploadKuduRepository.STATUS_INGESTED, 'Ingested (ETL Pending)'),
            (UploadKuduRepository.STATUS_ETL_RUNNING, 'ETL Running'),
            (UploadKuduRepository.STATUS_ETL_COMPLETE, 'ETL Complete'),
            (UploadKuduRepository.STATUS_FAILED, 'Failed'),
            (UploadKuduRepository.STATUS_CANCELLED, 'Cancelled'),
        ]

    def get_file_type_choices(self) -> List[Tuple[str, str]]:
        """Get file type choices for dropdowns."""
        return [
            ('csv', 'CSV'),
            ('tsv', 'TSV'),
            ('txt', 'Text'),
            ('json', 'JSON'),
            ('xlsx', 'Excel (xlsx)'),
            ('xls', 'Excel (xls)'),
            ('parquet', 'Parquet'),
        ]

    # =========================================================================
    # METADATA-DRIVEN INGESTION (using cis_datasource_mng)
    # =========================================================================

    def get_datasource_config(self, file_name: str) -> Optional[Dict[str, Any]]:
        """
        Get datasource configuration for a file from cis_datasource_mng.

        Args:
            file_name: Name of the uploaded file

        Returns:
            Datasource configuration dict or None if not found
        """
        return datasource_repository.get_datasource_by_name(file_name)

    def validate_with_datasource_config(
        self,
        file_obj,
        file_name: str,
        datasource_config: Dict[str, Any]
    ) -> FileValidationResult:
        """
        Validate file using datasource configuration from cis_datasource_mng.

        Uses predefined separator, columns, and skip lines from metadata.

        Args:
            file_obj: File object
            file_name: File name
            datasource_config: Configuration from cis_datasource_mng

        Returns:
            FileValidationResult with validation status
        """
        result = FileValidationResult()

        try:
            # Get file properties from datasource config
            separator = datasource_config.get('separator', ',')
            skip_lines = int(datasource_config.get('no_of_skip_line', 0) or 0)
            has_header = datasource_repository.parse_header_flag(datasource_config)

            # Get predefined columns
            intake_columns = datasource_repository.parse_intake_columns(datasource_config)

            # Read file content
            file_obj.seek(0)
            content = file_obj.read()
            result.file_size = len(content)

            if result.file_size == 0:
                result.errors.append("File is empty")
                return result

            if result.file_size > self.validation_service.MAX_FILE_SIZE:
                result.errors.append(f"File size exceeds maximum allowed")
                return result

            # Detect encoding
            result.encoding = self.validation_service._detect_encoding(content)

            # Decode and parse
            text_content = content.decode(result.encoding, errors='replace')
            lines = text_content.splitlines()

            # Skip initial lines if configured
            if skip_lines > 0:
                lines = lines[skip_lines:]

            if not lines:
                result.errors.append("File contains no data after skipping lines")
                return result

            # Parse CSV with configured separator
            import csv
            import io
            reader = csv.reader(io.StringIO('\n'.join(lines)), delimiter=separator)
            rows = list(reader)

            if not rows:
                result.errors.append("No data rows found")
                return result

            # Get file header row for column names
            file_header = rows[0] if has_header else [f'col_{i+1}' for i in range(len(rows[0]))]
            data_rows = rows[1:] if has_header else rows

            # Columns that are injected at ingest time and are NOT present in the
            # uploaded file. These are excluded from file column validation and
            # their default values are injected into sample_data for preview.
            SERVER_INJECTED_COLS = {'position_basis'}

            # Determine position_basis default for this datasource's target table
            POSITION_TABLE_BASIS = {
                'cis_user_sta_adhoc_position_1': 'TRADED',
                'cis_user_sta_adhoc_position_2': 'TRADED',
                'cis_user_sta_adhoc_position_3': 'TRADED',
                'cis_user_sta_adhoc_position_4': 'SETTLED',
                'cis_user_sta_adhoc_position_5': 'SETTLED',
            }
            target_table = datasource_config.get('target_table', '')
            target_table_key = target_table.lower().split('.')[-1]
            injected_defaults = {}
            if target_table_key in POSITION_TABLE_BASIS:
                injected_defaults['position_basis'] = POSITION_TABLE_BASIS[target_table_key]

            # Use intake_columns from datasource config OR file header
            if intake_columns:
                # Split intake_columns into file columns (present in file) and
                # server-injected columns (not in file, added at ingest time).
                file_intake_columns = [
                    col for col in intake_columns
                    if col['name'].lower().strip() not in SERVER_INJECTED_COLS
                ]
                injected_intake_columns = [
                    col for col in intake_columns
                    if col['name'].lower().strip() in SERVER_INJECTED_COLS
                ]

                # Strict column validation against FILE columns only
                expected_col_names = [col['name'].lower().strip() for col in file_intake_columns]
                actual_col_names = [self.validation_service._clean_column_name(h).lower().strip() for h in file_header]

                # Check column count (file columns only)
                if len(file_intake_columns) != len(file_header):
                    result.is_valid = False
                    result.errors.append(
                        f"Column count mismatch: File has {len(file_header)} columns, "
                        f"but datasource config expects {len(file_intake_columns)} columns. "
                        f"Expected columns: {', '.join(expected_col_names)}"
                    )
                    logger.error(
                        f"INVALID: Column count mismatch for {file_name}. "
                        f"Expected {len(file_intake_columns)}, got {len(file_header)}."
                    )
                    return result

                # Check column names match (order matters, file columns only)
                mismatched_columns = []
                for idx, (expected, actual) in enumerate(zip(expected_col_names, actual_col_names)):
                    if expected != actual:
                        mismatched_columns.append(f"Column {idx+1}: expected '{expected}', got '{actual}'")

                if mismatched_columns:
                    result.is_valid = False
                    result.errors.append(
                        f"Column name mismatch: {'; '.join(mismatched_columns)}. "
                        f"File columns must match datasource configuration exactly."
                    )
                    logger.error(
                        f"INVALID: Column name mismatch for {file_name}. "
                        f"Mismatches: {mismatched_columns}"
                    )
                    return result

                # All validations passed - use full intake_columns (including injected)
                result.columns = intake_columns
                result.column_count = len(result.columns)
                logger.info(
                    f"Column validation passed for {file_name}: "
                    f"{len(file_intake_columns)} file columns match"
                    + (f", {len(injected_intake_columns)} server-injected col(s) skipped from file check" if injected_intake_columns else "")
                )
            else:
                # Fall back to detection - but force STRING for external tables
                result.columns = self.validation_service._infer_column_types(
                    file_header, data_rows[:100], force_string=True
                )
                result.column_count = len(result.columns)

            # Count rows
            result.row_count = len(data_rows)

            # Set properties
            result.file_type = 'csv'
            result.delimiter = separator
            result.has_header = has_header

            # Generate sample data.
            # File columns are mapped by position; injected columns get their
            # default value so the preview table shows what will actually land
            # in the target table after ingest.
            file_col_names = [
                col['name'] for col in result.columns
                if col['name'].lower() not in SERVER_INJECTED_COLS
            ]
            result.sample_data = []
            result.all_data = []
            for row_idx, row in enumerate(data_rows):
                row_dict = {}
                # Map file columns by position
                for idx, col_name in enumerate(file_col_names):
                    row_dict[col_name] = row[idx] if idx < len(row) else ''
                # Inject server-side defaults (position_basis etc.)
                for col_name, default_val in injected_defaults.items():
                    row_dict[col_name] = default_val
                result.all_data.append(row_dict)
                # Sample data (preview only — bracketed defaults, capped at MAX_PREVIEW_ROWS)
                if row_idx < self.validation_service.MAX_PREVIEW_ROWS:
                    preview_dict = dict(row_dict)
                    for col_name, default_val in injected_defaults.items():
                        preview_dict[col_name] = f'[{default_val}]'
                    result.sample_data.append(preview_dict)

            logger.info(f"validate_with_datasource_config: {len(result.all_data)} total rows, {len(result.sample_data)} preview rows for {file_name}")

            if result.row_count == 0:
                result.errors.append("File contains only a header row with no data. Please add data rows before uploading.")
                return result

            result.is_valid = True

        except Exception as e:
            logger.error(f"Validation with datasource config error: {str(e)}")
            result.errors.append(f"Validation error: {str(e)}")

        return result

    # HDFS base path for uploaded files — must match upload_kudu_repository.HDFS_BASE_PATH
    HDFS_STAGING_PATH = '/mrw/cis/staging'

    def ingest_to_target_table(
        self,
        upload_id: str,
        datasource_config: Dict[str, Any],
        updated_by: str,
        processing_date: str = None,
        temp_file_path: str = None,
        sample_data: List[Dict[str, Any]] = None,
        ingestion_mode: str = 'overwrite'
    ) -> Tuple[bool, str]:
        """
        Ingest uploaded file to pre-defined target table using datasource config.

        This method:
        1. Uploads local file to HDFS /mrw/cis/staging/ if temp_file_path provided
        2. Creates staging external table for uploaded file
        3. Reads processing_date from HDFS /mrw/cis/MRA_PC_DATE.txt
        4. Inserts data into target table with additional columns:
           - src_id (from datasource.source_id)
           - src_system = 'cis'
           - data_cat = 'sta'
           - data_frq = 'adhoc'
           - processing_date (from MRA_PC_DATE.txt)

        Args:
            upload_id: Upload record ID
            datasource_config: Configuration from cis_datasource_mng
            updated_by: User performing ingestion
            processing_date: Override processing date (optional)
            temp_file_path: Local file path for session-based uploads
            sample_data: Sample data from session (used if file not available)
            ingestion_mode: 'overwrite' to replace existing data, 'append' to add to existing

        Returns:
            Tuple of (success, message)
        """
        is_session_upload = False  # default; set properly after DB lookup
        try:
            # Get upload record (may be None for session uploads)
            upload = self.repository.get_upload_by_id(upload_id)

            # For session uploads, we won't have a database record
            is_session_upload = upload is None

            if not is_session_upload:
                # Update status to ingesting
                self.repository.update_status(upload_id, UploadKuduRepository.STATUS_INGESTING, updated_by)

            # Get target table from datasource config
            target_table = datasource_config.get('target_table')
            if not target_table:
                return False, "No target_table defined in datasource configuration"

            # Get processing_date, preferring the uploaded file's OWN reporting_date
            # column over the system/HDFS/calendar date -- keeps the ETL's
            # processing_date partition consistent with the reporting_date values
            # the rows themselves carry (see normalise_date for the formats this
            # accepts). Falls back to the old GMP-system-date/HDFS/calendar-today
            # chain if no explicit override was passed and no reporting_date can
            # be found/parsed from a sample row, so behavior is unchanged for
            # uploads that don't carry that column.
            if not processing_date:
                _preview_rows = sample_data
                if not _preview_rows and upload:
                    _raw_sample = upload.get('sample_data_json', [])
                    if isinstance(_raw_sample, str):
                        try:
                            import json as _json_mod
                            _preview_rows = _json_mod.loads(_raw_sample)
                        except Exception:
                            _preview_rows = []
                    else:
                        _preview_rows = _raw_sample or []

                if _preview_rows:
                    _first_row = _preview_rows[0] or {}
                    _rd_key = next(
                        (k for k in _first_row if str(k).strip().lower() == 'reporting_date'),
                        None
                    )
                    if _rd_key and _first_row.get(_rd_key):
                        _rd_val = self.normalise_date(str(_first_row[_rd_key]))
                        if _rd_val and len(_rd_val) == 8 and _rd_val.isdigit():
                            processing_date = _rd_val
                            logger.info(
                                f"[ingest:svc] processing_date derived from uploaded "
                                f"file's reporting_date column: {processing_date}"
                            )
                        else:
                            logger.warning(
                                f"[ingest:svc] Found reporting_date column but could not "
                                f"parse its value ('{_first_row[_rd_key]}') to YYYYMMDD -- "
                                f"falling back to system processing_date"
                            )

            if not processing_date:
                processing_date = datasource_repository.get_processing_date()

            # Get file properties
            separator = datasource_config.get('separator', ',')
            has_header = datasource_repository.parse_header_flag(datasource_config)
            skip_lines = int(datasource_config.get('no_of_skip_line', 0) or 0)

            # Use Hybrid connection - Hive for external table operations
            from core.repositories.hybrid_connection import hybrid_manager

            # Create staging table name
            staging_table = f"stg_upload_{upload_id.replace('-', '_').lower()}"

            # Get columns from datasource config
            intake_columns = datasource_repository.parse_intake_columns(datasource_config)
            if not intake_columns:
                # Try to get columns from target table
                target_table_info = datasource_repository.get_table_info(target_table, self.repository.DATABASE)
                if target_table_info.get('columns'):
                    # Filter out the additional columns we add
                    additional_cols = {'src_id', 'src_system', 'data_cat', 'data_frq', 'processing_date'}
                    intake_columns = [
                        {'name': col['name'], 'type': 'STRING'}
                        for col in target_table_info['columns']
                        if col['name'].lower() not in additional_cols
                    ]

            if not intake_columns:
                return False, "No columns defined in datasource configuration or target table"

            # Resolve the best available data source, in priority order:
            #   P1. Local temp file (same-node upload or DB-persisted path)
            #   P2. Impala external table over HDFS path (Cloudera/MRW — no hdfs shell needed)
            #   P3. sample_data from session (session-based uploads only)
            #   P4. sample_data_json from DB record (last resort — capped at 20 rows)
            local_file = None
            _hdfs_tmp = None
            _hdfs_impala_done = False  # True if P2 Impala path handled ingest directly

            logger.debug(f"[ingest:svc] FILE RESOLUTION upload_id={upload_id} temp_file_path={temp_file_path!r}")

            if temp_file_path and os.path.exists(temp_file_path):
                local_file = temp_file_path
                logger.debug(f"[ingest:svc] P1 — local temp file: {local_file}")
            else:
                # ----------------------------------------------------------------
                # P2: HDFS file available — create a temporary Hive external table
                #     pointing at the HDFS staging directory and INSERT directly
                #     into the target raw table via Impala SELECT.
                #     No hdfs shell command needed — works on any Cloudera node.
                # ----------------------------------------------------------------
                hdfs_path_db = (upload.get('hdfs_path', '') or '').strip() if upload else ''
                if hdfs_path_db and not is_session_upload:
                    logger.debug(f"[ingest:svc] P2 — Impala external table over HDFS: {hdfs_path_db}")
                    p2_success, p2_msg = self._ingest_via_hdfs_impala(
                        hdfs_path=hdfs_path_db,
                        target_table=target_table,
                        datasource_config=datasource_config,
                        intake_columns=intake_columns,
                        processing_date=processing_date,
                        upload_id=upload_id,
                        updated_by=updated_by,
                        ingestion_mode=ingestion_mode,
                    )
                    if p2_success:
                        logger.debug(f"[ingest:svc] P2 OK: {p2_msg}")
                        if not is_session_upload:
                            self.repository.update_status(upload_id, UploadKuduRepository.STATUS_COMPLETED, updated_by)
                        return True, p2_msg
                    else:
                        logger.warning(f"[ingest:svc] P2 FAILED — {p2_msg}")

            if not local_file:
                if sample_data:
                    logger.debug(f"[ingest:svc] P3 — session sample_data ({len(sample_data)} rows)")
                else:
                    upload_sample = upload.get('sample_data_json', []) if upload else []
                    if upload_sample:
                        if isinstance(upload_sample, str):
                            try:
                                import json as json_module
                                sample_data = json_module.loads(upload_sample)
                            except Exception as _je:
                                logger.warning(f"[ingest:svc] P4 parse error: {_je}")
                                sample_data = []
                        else:
                            sample_data = upload_sample
                    if not sample_data:
                        logger.error(f"[ingest:svc] ALL PRIORITIES FAILED — no data source available for upload_id={upload_id}")

            # If we have either a local file or sample_data, use INSERT VALUES
            if local_file or sample_data:
                try:
                    # ----------------------------------------------------------------
                    # Kudu UPSERT path: cis_equity_price is a Kudu table, not a
                    # Hive external/partitioned table — INSERT OVERWRITE won't work.
                    # Route to dedicated Kudu upsert method instead.
                    # ----------------------------------------------------------------
                    if target_table.lower() in ('cis_equity_price', 'gmp_cis.cis_equity_price'):
                        return self._ingest_kudu_equity_price(
                            datasource_config=datasource_config,
                            intake_columns=intake_columns,
                            sample_data=sample_data if sample_data else [],
                            updated_by=updated_by,
                            upload_id=upload_id,
                            is_session_upload=is_session_upload,
                            temp_file_path=local_file,
                        )

                    return self._ingest_using_insert_values(
                        target_table=target_table,
                        datasource_config=datasource_config,
                        intake_columns=intake_columns,
                        sample_data=sample_data if sample_data else [],
                        processing_date=processing_date,
                        updated_by=updated_by,
                        upload_id=upload_id,
                        is_session_upload=is_session_upload,
                        temp_file_path=local_file,
                        ingestion_mode=ingestion_mode
                    )
                finally:
                    # Clean up the HDFS-downloaded temp file (not the user's original temp)
                    if _hdfs_tmp and os.path.exists(_hdfs_tmp):
                        try:
                            os.unlink(_hdfs_tmp)
                            logger.info(f"[ingest] Cleaned up HDFS temp file: {_hdfs_tmp}")
                        except OSError:
                            pass

            return False, "No file or sample data available for ingestion. Please re-upload the file."

        except Exception as e:
            logger.error(f"Metadata-driven ingestion error: {str(e)}")
            if not is_session_upload:
                safe_err = f"{type(e).__name__}: {str(e)[:200]}"
                self.repository.update_status(upload_id, UploadKuduRepository.STATUS_FAILED, updated_by, safe_err)
            return False, f"Ingestion error: {str(e)}"

    def _ingest_kudu_equity_price(
        self,
        datasource_config: Dict[str, Any],
        intake_columns: List[Dict[str, str]],
        sample_data: List[Dict[str, Any]],
        updated_by: str,
        upload_id: str,
        is_session_upload: bool = False,
        temp_file_path: str = None,
    ) -> Tuple[bool, str]:
        """
        Ingest equity price CSV rows directly into cis_equity_price (Kudu) via UPSERT.

        cis_equity_price is a Kudu table — INSERT OVERWRITE / partitions don't apply.
        Each row is upserted using the existing EquityPriceHiveRepository so that
        currency_code + isin are looked up from cis_security and all audit fields
        are populated consistently.

        CSV columns used (case-insensitive via intake_columns):
            price_date, security_label, closing_price
            (shares_outstanding and market_value are ignored)

        Args:
            datasource_config: Datasource configuration from cis_datasource_mng
            intake_columns:    Parsed column list from datasource config
            sample_data:       Rows already parsed (used if temp_file_path not available)
            updated_by:        Username performing the ingestion
            upload_id:         Upload record ID for status tracking
            is_session_upload: True when upload record lives in session, not DB
            temp_file_path:    Path to local temp file (full data, preferred over sample_data)

        Returns:
            Tuple of (success, message)
        """
        from decimal import Decimal, InvalidOperation
        from market_data.repositories.equity_price_hive_repository import equity_price_hive_repository
        from security.repositories.security_hive_repository import SecurityHiveRepository
        import time as _time

        counters = {'inserted': 0, 'skipped': 0, 'errors': []}

        # ---- Build full data list from file or sample_data ----
        separator = datasource_config.get('separator', ',')
        has_header = str(datasource_config.get('header', 'true')).lower() == 'true'
        col_names = [col['name'] for col in intake_columns]

        all_data = list(sample_data) if sample_data else []

        if temp_file_path and os.path.exists(temp_file_path):
            logger.info(f"[equity_price] Reading full file from {temp_file_path}")
            try:
                all_data = []
                with open(temp_file_path, 'r', encoding='utf-8-sig', errors='replace') as f:
                    reader = csv.reader(f, delimiter=separator or ',')
                    rows = list(reader)
                if has_header and rows:
                    rows = rows[1:]
                for row in rows:
                    row_dict = {col: (row[i] if i < len(row) else '') for i, col in enumerate(col_names)}
                    all_data.append(row_dict)
                logger.info(f"[equity_price] Read {len(all_data)} rows from file")
            except Exception as e:
                logger.error(f"[equity_price] Failed to read file: {e}")
                all_data = list(sample_data) if sample_data else []

        if not all_data:
            return False, "No data to ingest"

        # ---- Column name map (normalise to lowercase for lookup) ----
        # intake_columns names are already cleaned by FileValidationService._clean_column_name
        # Expected cleaned names from CSV headers:
        #   "Price Date"       -> "price_date"
        #   "Security label"   -> "security_label"
        #   "Closing Price"    -> "closing_price"
        def _find_col(row_dict: dict, *candidates) -> str:
            """Return value for the first matching key (case-insensitive)."""
            lower_map = {k.lower(): v for k, v in row_dict.items()}
            for cand in candidates:
                v = lower_map.get(cand.lower(), '')
                if v:
                    return str(v).strip()
            return ''

        # Per-upload security lookup cache (avoids N Impala round-trips for same security)
        security_cache: Dict[str, Optional[Dict]] = {}

        for row_num, row in enumerate(all_data, start=2):
            price_date    = _find_col(row, 'price_date', 'price date')
            security_label = _find_col(row, 'security_label', 'security label', 'security_name')
            closing_price_str = _find_col(row, 'closing_price', 'closing price', 'main_closing_price')

            # Basic validation
            if not price_date or not security_label or not closing_price_str:
                counters['errors'].append({
                    'row': row_num,
                    'reason': 'Missing price_date, security_label, or closing_price'
                })
                counters['skipped'] += 1
                continue

            try:
                price_decimal = Decimal(closing_price_str)
                if price_decimal <= 0:
                    raise ValueError("non-positive")
            except (InvalidOperation, ValueError):
                counters['errors'].append({
                    'row': row_num,
                    'reason': f"Invalid closing price: '{closing_price_str}'"
                })
                counters['skipped'] += 1
                continue

            # Security lookup for currency_code + isin
            if security_label not in security_cache:
                try:
                    securities = SecurityHiveRepository.get_all_securities(
                        search=security_label, limit=5
                    )
                    match = next(
                        (s for s in securities
                         if s.get('security_name', '').strip() == security_label),
                        None
                    )
                    security_cache[security_label] = match
                except Exception as e:
                    security_cache[security_label] = None
                    logger.warning(f"[equity_price] Security lookup failed for '{security_label}': {e}")

            sec_rec = security_cache.get(security_label)
            currency_code = sec_rec.get('currency_code', '') if sec_rec else ''
            isin = sec_rec.get('isin', '') if sec_rec else ''

            if not currency_code:
                logger.warning(
                    f"[equity_price] Row {row_num}: no currency_code for '{security_label}', using empty"
                )

            equity_price_data = {
                'currency_code': currency_code,
                'security_label': security_label,
                'price_date': price_date,
                'main_closing_price': float(price_decimal),
                'isin': isin,
                'src_system': 'CIS',
                'created_by': updated_by,
                'price_timestamp': int(_time.time() * 1000),
            }

            try:
                success = equity_price_hive_repository.upsert_equity_price(
                    equity_price_data, username=updated_by
                )
                if success:
                    counters['inserted'] += 1
                else:
                    counters['errors'].append({'row': row_num, 'reason': 'Upsert returned False'})
                    counters['skipped'] += 1
            except Exception as e:
                counters['errors'].append({'row': row_num, 'reason': str(e)})
                counters['skipped'] += 1

        # ---- Update upload record status ----
        if not is_session_upload:
            if counters['inserted'] > 0:
                self.repository.update_upload(upload_id, {
                    'status': UploadKuduRepository.STATUS_COMPLETED,
                    'target_table_name': 'cis_equity_price',
                    'row_count': counters['inserted'],
                    'description': (
                        f"Kudu UPSERT: {counters['inserted']} inserted, "
                        f"{counters['skipped']} skipped"
                    ),
                }, updated_by)
            else:
                self.repository.update_status(
                    upload_id, UploadKuduRepository.STATUS_FAILED, updated_by,
                    f"0 rows inserted. Errors: {str(counters['errors'][:3])[:200]}"
                )

        # ---- Trigger position market value refresh ----
        if counters['inserted'] > 0:
            try:
                from trade.repositories.trade_kudu_repository import trade_kudu_repository
                trade_kudu_repository.refresh_market_values()
            except Exception:
                pass  # Non-blocking

        # ---- Clear equity price cache ----
        if counters['inserted'] > 0:
            try:
                from market_data.services.equity_price_service import EquityPriceService
                EquityPriceService.clear_cache()
            except Exception:
                pass

        logger.info(
            f"[equity_price] upload_id={upload_id} by {updated_by}: "
            f"inserted={counters['inserted']}, skipped={counters['skipped']}, "
            f"errors={len(counters['errors'])}"
        )

        if counters['inserted'] > 0:
            msg = (
                f"Successfully upserted {counters['inserted']} equity price"
                f"{'s' if counters['inserted'] != 1 else ''} into cis_equity_price"
            )
            if counters['skipped']:
                msg += f" ({counters['skipped']} skipped — see upload detail for errors)"
            return True, msg

        return False, (
            f"No rows inserted into cis_equity_price. "
            f"{counters['skipped']} skipped. "
            f"First error: {counters['errors'][0]['reason'] if counters['errors'] else 'unknown'}"
        )

    def _upload_file_to_hdfs(self, local_path: str, hdfs_dir: str) -> bool:
        """
        Upload a local file to HDFS using the hdfs CLI.
        Tries several common full paths for the hdfs binary on Cloudera CML.

        Args:
            local_path: Path to local file
            hdfs_dir: HDFS directory to upload to (LOCATION for external table)

        Returns:
            True if successful, False otherwise
        """
        import subprocess
        import shutil

        # Resolve hdfs binary — try PATH first, then common Cloudera install locations
        hdfs_bin = shutil.which('hdfs')
        if not hdfs_bin:
            for candidate in [
                '/usr/bin/hdfs',
                '/usr/local/bin/hdfs',
                '/opt/cloudera/parcels/CDH/bin/hdfs',
                '/opt/hadoop/bin/hdfs',
            ]:
                if os.path.isfile(candidate):
                    hdfs_bin = candidate
                    break

        if not hdfs_bin:
            logger.error("[hdfs:put] hdfs binary not found — cannot upload to HDFS")
            return False

        logger.info(f"[hdfs:put] using hdfs binary: {hdfs_bin}")
        try:
            # Create HDFS directory
            mkdir_cmd = [hdfs_bin, 'dfs', '-mkdir', '-p', hdfs_dir]
            logger.info(f"[hdfs:put] {' '.join(mkdir_cmd)}")
            result = subprocess.run(mkdir_cmd, capture_output=True, text=True, timeout=60)
            logger.info(f"[hdfs:put] mkdir rc={result.returncode} stderr={result.stderr[:200]}")
            if result.returncode != 0:
                logger.error(f"[hdfs:put] Failed to create HDFS directory: {result.stderr}")
                return False

            # Upload file to HDFS
            put_cmd = [hdfs_bin, 'dfs', '-put', '-f', local_path, hdfs_dir]
            logger.info(f"[hdfs:put] {' '.join(put_cmd)}")
            result = subprocess.run(put_cmd, capture_output=True, text=True, timeout=300)
            logger.info(f"[hdfs:put] put rc={result.returncode} stderr={result.stderr[:200]}")
            if result.returncode != 0:
                logger.error(f"[hdfs:put] Failed to upload file: {result.stderr}")
                return False

            logger.info(f"[hdfs:put] SUCCESS {local_path} → {hdfs_dir}")
            return True

        except subprocess.TimeoutExpired:
            logger.error("[hdfs:put] timed out")
            return False
        except Exception as e:
            logger.error(f"[hdfs:put] error: {e}", exc_info=True)
            return False

    def _cleanup_hdfs_staging(self, hdfs_path: str) -> bool:
        """
        Clean up HDFS staging directory.

        Args:
            hdfs_path: HDFS path to clean up

        Returns:
            True if successful, False otherwise
        """
        import subprocess

        try:
            # Only clean up if path is under staging directory
            if self.HDFS_STAGING_PATH not in hdfs_path:
                return True

            rm_cmd = ['hdfs', 'dfs', '-rm', '-r', '-f', hdfs_path]
            result = subprocess.run(rm_cmd, capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                logger.info(f"Cleaned up HDFS staging: {hdfs_path}")
                return True
            else:
                logger.warning(f"Could not clean up HDFS staging: {result.stderr}")
                return False

        except Exception as e:
            logger.warning(f"HDFS cleanup error: {e}")
            return False

    def _ingest_via_hdfs_impala(
        self,
        hdfs_path: str,
        target_table: str,
        datasource_config: Dict[str, Any],
        intake_columns: List[Dict[str, Any]],
        processing_date: str,
        upload_id: str,
        updated_by: str,
        ingestion_mode: str = 'overwrite',
    ) -> Tuple[bool, str]:
        """
        Ingest a CSV/pipe-delimited file on HDFS directly into the raw target table
        using Impala — no hdfs shell command required.

        Strategy:
          1. CREATE EXTERNAL TABLE stg_... STORED AS TEXTFILE LOCATION '<hdfs_path>'
          2. INSERT OVERWRITE/INTO <target_table> PARTITION(processing_date, src_id)
                SELECT col1, col2, ..., '<processing_date>' FROM stg_...
             skipping the header row via a WHERE clause on a ROW_NUMBER() window.
          3. DROP TABLE stg_... (external — does not delete HDFS data)
        """
        from core.repositories.impala_connection import impala_manager
        from ..repositories.datasource_repository import datasource_repository

        db = self.repository.DATABASE
        separator = datasource_config.get('separator', ',')
        has_header = datasource_repository.parse_header_flag(datasource_config)
        source_id = datasource_config.get('source_id', target_table)
        src_system = datasource_config.get('src_system', 'USER_UPLOAD')
        sub_system = datasource_config.get('sub_system', 'user')
        data_cat = datasource_config.get('data_cat', 'sta')
        data_frq = datasource_config.get('data_frq', 'adhoc')

        # Safe staging table name — unique per upload
        stg_table = f"stg_hdfs_{upload_id.replace('-', '_').lower()}"

        # Impala field terminator: pipe needs escaping
        field_term = separator if separator not in ('|',) else '\\|'

        col_names = [col['name'] for col in intake_columns]
        # Build SELECT col list: col1, col2, ... from the staging table
        # All staging columns are STRING (TEXTFILE external table)
        stg_col_select = ',\n                    '.join(col_names)

        # Additional columns appended to target
        POSITION_TABLE_BASIS = {
            'cis_user_sta_adhoc_position_1': 'TRADED',
            'cis_user_sta_adhoc_position_2': 'TRADED',
            'cis_user_sta_adhoc_position_3': 'TRADED',
            'cis_user_sta_adhoc_position_4': 'SETTLED',
            'cis_user_sta_adhoc_position_5': 'SETTLED',
        }
        table_lower = target_table.lower().split('.')[-1]
        position_basis = POSITION_TABLE_BASIS.get(table_lower, '')

        try:
            # Step 1: Drop any leftover staging table
            impala_manager.execute_write(f"DROP TABLE IF EXISTS {db}.{stg_table}", database=db)

            # Step 2: Create external table over the HDFS staging directory.
            # The LOCATION is a directory — Hive reads ALL files in it.
            # To prevent accumulation from re-runs, wipe the directory first
            # then re-upload. Since we only hold one file per upload_id dir,
            # this is safe.
            create_cols = ',\n    '.join(f"`{c}` STRING" for c in col_names)
            ok = impala_manager.execute_write(f"""
                CREATE EXTERNAL TABLE {db}.{stg_table} (
                    {create_cols}
                )
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY '{field_term}'
                STORED AS TEXTFILE
                LOCATION '{hdfs_path}'
                TBLPROPERTIES ('skip.header.line.count'='{"1" if has_header else "0"}')
            """, database=db)
            if not ok:
                return False, f"Could not CREATE EXTERNAL TABLE {stg_table} over {hdfs_path}"

            impala_manager.execute_write(f"INVALIDATE METADATA {db}.{stg_table}", database=db)

            # Step 3: Count rows to verify — also guards against multi-file accumulation
            cnt_rows = impala_manager.execute_query(
                f"SELECT COUNT(*) AS cnt FROM {db}.{stg_table}", database=db
            )
            row_count = cnt_rows[0].get('cnt', 0) if cnt_rows else 0
            logger.info(f"[hdfs:impala] staging table {stg_table} has {row_count} rows at {hdfs_path}")

            if row_count == 0:
                impala_manager.execute_write(f"DROP TABLE IF EXISTS {db}.{stg_table}", database=db)
                return False, f"HDFS staging table {stg_table} read 0 rows from {hdfs_path} — file may be empty or path wrong"

            # Build extra column expressions (only columns NOT already in the file).
            # position_basis defaulted here; src_id goes into PARTITION clause.
            extra_cols = [
                f"'{src_system}' AS src_system",
                f"'{sub_system}' AS sub_system",
                f"'{data_cat}'   AS data_cat",
                f"'{data_frq}'   AS data_frq",
            ]
            col_names_lower = [c.lower() for c in col_names]
            if position_basis and 'position_basis' not in col_names_lower:
                extra_cols.append(f"'{position_basis}' AS position_basis")

            # Step 4+5: INSERT OVERWRITE into the target partition — atomically
            # replaces exactly the (processing_date) partition so re-runs never
            # double-insert. DROP + INSERT INTO was previously used but is not
            # safe: if DROP fails or a concurrent upload shares the same date,
            # rows accumulate. INSERT OVERWRITE PARTITION is the safe alternative.
            extra_cols_sql = ',\n                    '.join(extra_cols)
            ok = impala_manager.execute_write(f"""
                INSERT OVERWRITE {db}.{target_table}
                PARTITION (processing_date='{processing_date}')
                SELECT
                    {stg_col_select},
                    {extra_cols_sql}
                FROM {db}.{stg_table}
            """, database=db)

            if not ok:
                impala_manager.execute_write(f"DROP TABLE IF EXISTS {db}.{stg_table}", database=db)
                return False, f"INSERT from {stg_table} into {target_table} failed — check Impala logs"

            impala_manager.execute_write(
                f"REFRESH {db}.{target_table} PARTITION (processing_date='{processing_date}')",
                database=db
            )

            # Step 6: Confirm rows landed — filter by processing_date only
            # (src_id is a data column, not a partition key, on these tables)
            inserted = impala_manager.execute_query(f"""
                SELECT COUNT(*) AS cnt FROM {db}.{target_table}
                WHERE processing_date = '{processing_date}'
            """, database=db)
            inserted_count = inserted[0].get('cnt', 0) if inserted else 0
            logger.info(f"[hdfs:impala] inserted {inserted_count} rows into {target_table} partition processing_date={processing_date}")

            # Step 7: Drop staging table (external — HDFS data preserved for audit)
            impala_manager.execute_write(f"DROP TABLE IF EXISTS {db}.{stg_table}", database=db)

            return True, f"Ingested {inserted_count} rows from HDFS into {target_table} (processing_date={processing_date})"

        except Exception as e:
            logger.error(f"[hdfs:impala] error: {e}", exc_info=True)
            try:
                impala_manager.execute_write(f"DROP TABLE IF EXISTS {db}.{stg_table}", database=db)
            except Exception:
                pass
            return False, f"HDFS Impala ingest error: {e}"

    def _download_from_hdfs(self, hdfs_path: str) -> Optional[str]:
        """
        Download an HDFS file to a local temp file and return the local path.
        Returns None if hdfs command is unavailable or the download fails.
        """
        from django.conf import settings as django_settings
        import subprocess
        import tempfile

        logger.info(f"[hdfs:download] called with hdfs_path={hdfs_path!r}")
        try:
            suffix = os.path.basename(hdfs_path)
            tmp_dir = os.path.join(django_settings.BASE_DIR, 'temp_uploads')
            os.makedirs(tmp_dir, exist_ok=True)
            tmp = tempfile.NamedTemporaryFile(
                delete=False, suffix=f'_{suffix}', dir=tmp_dir
            )
            tmp.close()
            get_cmd = ['hdfs', 'dfs', '-get', '-f', hdfs_path, tmp.name]
            logger.info(f"[hdfs:download] running: {' '.join(get_cmd)}")
            result = subprocess.run(get_cmd, capture_output=True, text=True, timeout=300)
            logger.info(f"[hdfs:download] returncode={result.returncode}")
            if result.stdout:
                logger.info(f"[hdfs:download] stdout={result.stdout[:500]}")
            if result.stderr:
                logger.info(f"[hdfs:download] stderr={result.stderr[:500]}")
            if result.returncode == 0:
                file_size = os.path.getsize(tmp.name)
                logger.info(f"[hdfs:download] SUCCESS → {tmp.name} ({file_size} bytes)")
                return tmp.name
            logger.error(f"[hdfs:download] FAILED returncode={result.returncode} stderr={result.stderr[:500]}")
            try:
                os.unlink(tmp.name)
            except OSError:
                pass
            return None
        except FileNotFoundError:
            logger.warning("[hdfs:download] 'hdfs' command not found — hdfs client not installed on this node")
            return None
        except subprocess.TimeoutExpired:
            logger.error("[hdfs:download] timed out after 300s")
            return None
        except Exception as e:
            logger.error(f"[hdfs:download] unexpected error: {e}", exc_info=True)
            return None

    # Columns whose values should be normalised to YYYYMMDD before ingest.
    # Matched case-insensitively against the target table column name.
    DATE_COLUMNS = {
        'trade_date', 'reporting_date', 'maturity_date', 'price_date',
        'settlement_date', 'settle_date', 'value_date', 'effective_date',
        'start_date', 'end_date', 'expiry_date',
    }

    @staticmethod
    def normalise_date(val: str) -> str:
        """
        Convert a date string from any common format to YYYYMMDD.

        Handles (examples):
            30/01/2026  -> 20260130   (DD/MM/YYYY)
            01-30-2026  -> 20260130   (MM-DD-YYYY)
            2026-01-30  -> 20260130   (YYYY-MM-DD  — already ISO)
            20260130    -> 20260130   (already YYYYMMDD)
            30-Jan-2026 -> 20260130   (DD-Mon-YYYY)
            Jan 30 2026 -> 20260130
            30 January 2026 -> 20260130

        Returns the original value unchanged if it cannot be parsed,
        so ingest never fails hard on a bad date — the raw value lands
        in the table and can be flagged by the ETL report.
        """
        if not val or not isinstance(val, str):
            return val or ''

        cleaned = val.strip()
        if not cleaned:
            return ''

        # Already YYYYMMDD — return immediately
        if len(cleaned) == 8 and cleaned.isdigit():
            return cleaned

        from datetime import datetime

        # Ordered list of format strings to try
        DATE_FORMATS = [
            '%d/%m/%Y',    # 30/01/2026
            '%d-%m-%Y',    # 30-01-2026
            '%m/%d/%Y',    # 01/30/2026  (US)
            '%m-%d-%Y',    # 01-30-2026  (US)
            '%Y-%m-%d',    # 2026-01-30  (ISO)
            '%Y/%m/%d',    # 2026/01/30
            '%d-%b-%Y',    # 30-Jan-2026
            '%d-%B-%Y',    # 30-January-2026
            '%d %b %Y',    # 30 Jan 2026
            '%d %B %Y',    # 30 January 2026
            '%b %d %Y',    # Jan 30 2026
            '%B %d %Y',    # January 30 2026
            '%b %d, %Y',   # Jan 30, 2026
            '%B %d, %Y',   # January 30, 2026
            '%Y%m%d',      # 20260130 (already caught above, safety fallback)
        ]

        for fmt in DATE_FORMATS:
            try:
                dt = datetime.strptime(cleaned, fmt)
                return dt.strftime('%Y%m%d')
            except ValueError:
                continue

        # Could not parse — return original so ingest doesn't break
        logger.warning(f"[date_normalise] Could not parse date value: '{cleaned}' — storing as-is")
        return cleaned

    def _ingest_using_insert_values(
        self,
        target_table: str,
        datasource_config: Dict[str, Any],
        intake_columns: List[Dict[str, str]],
        sample_data: List[Dict[str, Any]],
        processing_date: str,
        updated_by: str,
        upload_id: str,
        is_session_upload: bool = False,
        temp_file_path: str = None,
        ingestion_mode: str = 'overwrite'
    ) -> Tuple[bool, str]:
        """
        Ingest data to Hive external Parquet table using Impala INSERT.

        For external Parquet tables with partitions:
        1. Read data from file or sample_data
        2. Deduplicate rows
        3. Based on ingestion_mode:
           - 'overwrite': INSERT OVERWRITE to replace existing partition data
           - 'append': INSERT INTO to add to existing partition data
        4. Refresh table metadata

        Args:
            target_table: Target table name
            datasource_config: Datasource configuration
            intake_columns: Column definitions
            sample_data: Data rows to insert (used if temp_file_path not available)
            processing_date: Processing date (partition value)
            updated_by: User performing ingestion
            upload_id: Upload ID
            is_session_upload: Whether this is a session-based upload
            temp_file_path: Path to local temp file (if available, reads full file)
            ingestion_mode: 'overwrite' to replace data, 'append' to add to existing

        Returns:
            Tuple of (success, message)
        """
        from core.repositories.impala_connection import impala_manager
        from ..repositories.datasource_repository import datasource_repository
        import csv
        import re as _re

        try:
            # Validate processing_date before it touches any SQL
            if not processing_date or not _re.match(r'^\d{8}$', str(processing_date)):
                return False, f"Invalid processing_date '{processing_date}' — must be YYYYMMDD"

            source_id = datasource_config.get('source_id', '')
            col_names = [col['name'] for col in intake_columns]
            separator = datasource_config.get('separator', ',')
            has_header = datasource_repository.parse_header_flag(datasource_config)

            # Get target table columns to ensure we match exactly
            target_table_info = datasource_repository.get_table_info(target_table, self.repository.DATABASE)
            target_table_cols = [col['name'] for col in target_table_info.get('columns', [])]

            if not target_table_cols:
                return False, f"Could not get column info for target table {target_table}"

            # Determine src_system from datasource config or target table name
            # USER_UPLOAD tables: cis_user_sta_adhoc_position_* (contain 'user' in name)
            # AMS_STREET tables: gmp_cis_sta_*_ams_* (contain 'ams' in name)
            src_system = datasource_config.get('src_system', '')
            if not src_system:
                table_lower = target_table.lower()
                if 'ams' in table_lower:
                    src_system = 'AMS_STREET'
                elif 'user' in table_lower or 'cis_user' in table_lower:
                    src_system = 'USER_UPLOAD'
                else:
                    src_system = 'USER_UPLOAD'  # Default

            sub_system = datasource_config.get('sub_system', '')
            if not sub_system:
                sub_system = 'user' if src_system == 'USER_UPLOAD' else 'ams'

            # Additional columns map (excluding processing_date as it's a partition column)
            # Use target_table name as src_id for traceability
            additional_cols_map = {
                'src_id': target_table,  # Use table name as source identifier
                'src_system': src_system,
                'sub_system': sub_system,
                'data_cat': datasource_config.get('data_cat', 'sta'),
                'data_frq': datasource_config.get('data_frq', 'adhoc'),
            }

            # position_basis: defaulted at ingest time — not present in uploaded file.
            # Tables 1-3 (trade date based) → TRADED
            # Tables 4-5 (settled date based) → SETTLED
            POSITION_TABLE_BASIS = {
                'cis_user_sta_adhoc_position_1': 'TRADED',
                'cis_user_sta_adhoc_position_2': 'TRADED',
                'cis_user_sta_adhoc_position_3': 'TRADED',
                'cis_user_sta_adhoc_position_4': 'SETTLED',
                'cis_user_sta_adhoc_position_5': 'SETTLED',
            }
            table_lower = target_table.lower().split('.')[-1]  # strip db prefix if present
            if table_lower in POSITION_TABLE_BASIS and 'position_basis' in [c.lower() for c in target_table_cols]:
                additional_cols_map['position_basis'] = POSITION_TABLE_BASIS[table_lower]
                logger.info(f"Defaulting position_basis='{additional_cols_map['position_basis']}' for {target_table}")

            logger.info(f"Using src_system={src_system}, sub_system={sub_system} for {target_table}")

            # Read data from file or use sample_data
            all_data = sample_data or []
            logger.info(f"[insert_values] temp_file_path={temp_file_path!r} exists={os.path.exists(temp_file_path) if temp_file_path else 'N/A'}")
            logger.info(f"[insert_values] sample_data rows={len(sample_data) if sample_data else 0}")
            logger.info(f"[insert_values] separator={separator!r} has_header={has_header} col_names={col_names}")
            if temp_file_path and os.path.exists(temp_file_path):
                file_size = os.path.getsize(temp_file_path)
                logger.info(f"[insert_values] reading full file {temp_file_path} ({file_size} bytes)")
                try:
                    all_data = []
                    with open(temp_file_path, 'r', encoding='utf-8-sig', errors='replace') as f:
                        delim = separator if separator else ','
                        reader = csv.reader(f, delimiter=delim)
                        rows = list(reader)

                    logger.info(f"[insert_values] raw row count from file={len(rows)} has_header={has_header}")
                    if has_header and len(rows) > 0:
                        logger.info(f"[insert_values] header row={rows[0]}")
                        data_rows = rows[1:]
                    else:
                        data_rows = rows

                    logger.info(f"[insert_values] data_rows to process={len(data_rows)}")
                    # Convert to list of dicts using intake column names
                    for row in data_rows:
                        # Skip blank/whitespace-only rows (common in files with trailing newlines)
                        if not any(str(c).strip() for c in row):
                            continue
                        row_dict = {}
                        for idx, col in enumerate(col_names):
                            if idx < len(row):
                                row_dict[col] = row[idx]
                            else:
                                row_dict[col] = ''
                        all_data.append(row_dict)

                    logger.info(f"[insert_values] parsed {len(all_data)} rows from file")
                except Exception as e:
                    logger.error(f"[insert_values] FAILED to read file {temp_file_path}: {e}", exc_info=True)
                    all_data = sample_data or []
            else:
                logger.warning(f"[insert_values] temp_file_path not usable — using sample_data ({len(all_data)} rows)")

            if not all_data:
                return False, "No data to insert"

            # Deduplicate data before insertion
            original_count = len(all_data)
            all_data, duplicate_count = self._deduplicate_data(all_data, col_names)
            deduplicated_count = len(all_data)

            if duplicate_count > 0:
                logger.warning(f"Removed {duplicate_count} duplicate rows from {original_count} total rows")

            logger.info(f"Ingesting {deduplicated_count} unique rows to {target_table} partition={processing_date} (mode={ingestion_mode})")

            # Get non-partition columns (exclude processing_date)
            non_partition_cols = [col for col in target_table_cols if col.lower() != 'processing_date']

            # Step 1: For overwrite mode, delete the partition first using Impala
            # DROP PARTITION, which is the only safe way to clear a Hive partition
            # without relying on SELECT * column ordering.
            if ingestion_mode != 'append':
                try:
                    drop_sql = (
                        f"ALTER TABLE {self.repository.DATABASE}.{target_table} "
                        f"DROP IF EXISTS PARTITION (processing_date='{processing_date}')"
                    )
                    impala_manager.execute_write(drop_sql, database=self.repository.DATABASE)
                    logger.info(f"Dropped partition processing_date='{processing_date}' for overwrite")
                except Exception as _dp:
                    logger.warning(f"Could not drop partition (may not exist yet): {_dp}")

            # Step 2: One INSERT … SELECT per row.
            # Using a single-row SELECT with named aliases is the only form that
            # works across all Impala versions regardless of column count:
            #   INSERT INTO t PARTITION (p='v') SELECT lit AS col, ...
            # UNION ALL inside an inline view causes "duplicate alias" errors.
            # An explicit column list after PARTITION(…) is a ParseException.
            rows_inserted = 0

            def _sql_str(val) -> str:
                """Escape a value for Impala single-quoted string literals.
                Impala uses C-style escaping: \' for single-quote, not ''."""
                s = str(val)
                s = s.replace('\\', '\\\\')   # backslash first (must be first)
                s = s.replace("'", "\\'")      # single-quote → backslash-quote
                s = s.replace('\n', '\\n')     # newline
                s = s.replace('\r', '\\r')     # carriage return
                s = s.replace('\0', '')        # null byte — strip entirely
                return s

            def _build_row_sql(row):
                col_exprs = []
                for col in non_partition_cols:
                    col_lower = col.lower()
                    if col_lower in additional_cols_map:
                        val = additional_cols_map[col_lower]
                    elif col_lower in [c.lower() for c in col_names]:
                        matching_col = next((c for c in col_names if c.lower() == col_lower), None)
                        val = row.get(matching_col, '') if matching_col else ''
                    else:
                        val = ''
                    if col_lower in self.DATE_COLUMNS and val:
                        val = self.normalise_date(str(val))
                    if val is None or str(val).strip() == '':
                        lit = "''"
                    else:
                        lit = f"'{_sql_str(val)}'"
                    col_exprs.append(f"{lit} AS `{col}`")
                return (
                    f"INSERT INTO {self.repository.DATABASE}.{target_table} "
                    f"PARTITION (processing_date='{processing_date}') "
                    f"SELECT {', '.join(col_exprs)}"
                )

            rows_failed = 0
            for row_num, row in enumerate(all_data):
                insert_sql = _build_row_sql(row)
                success = impala_manager.execute_write(insert_sql, database=self.repository.DATABASE)
                if success:
                    rows_inserted += 1
                    if rows_inserted % 50 == 0:
                        logger.info(f"Inserted {rows_inserted}/{len(all_data)} rows into {target_table}")
                else:
                    rows_failed += 1
                    logger.error(f"[insert_values] row {row_num} failed — skipping. SQL was: {insert_sql[:300]}")
                    if rows_failed > 10:
                        return False, f"Too many row failures ({rows_failed}) — aborting at row {row_num}"

            if rows_inserted == 0:
                return False, f"No rows were inserted (all {rows_failed} rows failed)"

            # Step 3: Refresh metadata
            try:
                impala_manager.execute_write(
                    f"REFRESH {self.repository.DATABASE}.{target_table}",
                    database=self.repository.DATABASE
                )
                logger.info(f"Refreshed metadata for {target_table}")
            except Exception as e:
                logger.warning(f"Could not refresh metadata: {e}")

            # Success - update status
            if not is_session_upload:
                mode_label = 'APPEND' if ingestion_mode == 'append' else 'OVERWRITE'
                # Use INGESTED for position files (ETL step still pending);
                # COMPLETED for all other file types.
                _upload_rec = self.repository.get_upload_by_id(upload_id)
                _is_pos = self.is_position_upload(_upload_rec or {})
                new_status = (
                    UploadKuduRepository.STATUS_INGESTED if _is_pos
                    else UploadKuduRepository.STATUS_COMPLETED
                )
                # Preserve user's original description — append ingest notes as suffix.
                _orig_desc = (_upload_rec or {}).get('description', '') or ''
                ingest_note = f"Ingested {rows_inserted} rows → {target_table} [{mode_label}; processing_date={processing_date}"
                if duplicate_count > 0:
                    ingest_note += f"; {duplicate_count} duplicates removed"
                ingest_note += "]"
                new_desc = f"{_orig_desc}\n{ingest_note}".strip() if _orig_desc else ingest_note
                update_data = {
                    'status': new_status,
                    'target_table_name': target_table,
                    'row_count': rows_inserted,
                    'description': new_desc[:2000],
                }
                self.repository.update_upload(upload_id, update_data, updated_by)

            # Build success message
            mode_label = 'appended' if ingestion_mode == 'append' else 'ingested'
            success_msg = f"Successfully {mode_label} {rows_inserted} rows to {target_table} with processing_date={processing_date}"
            if duplicate_count > 0:
                success_msg += f" ({duplicate_count} duplicate rows removed from source)"

            return True, success_msg

        except Exception as e:
            logger.error(f"Ingestion error: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False, f"Ingestion error: {str(e)}"

    def _deduplicate_data(
        self,
        data: List[Dict[str, Any]],
        key_columns: List[str]
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Remove duplicate rows from data.

        Duplicates are identified by comparing all values in key_columns.
        Only the first occurrence of each unique row is kept.

        Args:
            data: List of row dictionaries
            key_columns: List of column names to use for duplicate detection

        Returns:
            Tuple of (deduplicated_data, duplicate_count)
        """
        if not data:
            return data, 0

        seen = set()
        unique_data = []
        duplicate_count = 0

        for row in data:
            # Create a tuple of values for comparison
            # Use all key columns to identify duplicates
            key_values = tuple(str(row.get(col, '')).strip() for col in key_columns)

            if key_values not in seen:
                seen.add(key_values)
                unique_data.append(row)
            else:
                duplicate_count += 1
                logger.debug(f"Duplicate row found: {key_values[:3]}...")  # Log first 3 values

        if duplicate_count > 0:
            logger.info(f"Deduplication: {len(data)} -> {len(unique_data)} rows ({duplicate_count} duplicates removed)")

        return unique_data, duplicate_count

    def get_all_datasource_configs(self) -> List[Dict[str, Any]]:
        """Get all datasource configurations for dropdown."""
        return datasource_repository.get_all_datasources()

    # =========================================================================
    # POSITION UPLOAD ETL — Run transform pipeline & report download
    # =========================================================================

    # Position source tables that trigger the AVP transform pipeline
    POSITION_TARGET_TABLES = {
        'cis_user_sta_adhoc_position_1',
        'cis_user_sta_adhoc_position_2',
        'cis_user_sta_adhoc_position_3',
        'cis_user_sta_adhoc_position_4',
        'cis_user_sta_adhoc_position_5',
    }

    def supports_partial_position_upload(self, src_id: str) -> bool:
        """Return True when this ETL source supports opt-in partial mode."""
        return (src_id or '').lower().split('.')[-1] in self.POSITION_TARGET_TABLES

    def get_position_reconciliation_mode(self, src_id: str, partial_upload: bool = False) -> str:
        """Return FULL (default) or PARTIAL for the current ETL source."""
        return 'PARTIAL' if partial_upload and self.supports_partial_position_upload(src_id) else 'FULL'

    @staticmethod
    def _sql_literal(value: Any) -> str:
        """Escape a value for embedding in an Impala single-quoted literal."""
        if value is None or str(value).strip() == '':
            return 'NULL'
        s = str(value).replace('’', "'").replace('‘', "'").replace('ʼ', "'")
        s = s.replace("\\", "\\\\")
        return "'" + s.replace("'", "\\'") + "'"

    @staticmethod
    def _sql_decimal(value: Any, dec_type: str = 'DECIMAL(30,8)', default: str = '0') -> str:
        """Return a DECIMAL literal with NULL/blank fallback."""
        if value is None or str(value).strip() == '':
            return f'CAST({default} AS {dec_type})'
        return f'CAST({str(value).strip()} AS {dec_type})'

    def _find_authoritative_missing_positions(
        self,
        *,
        db: str,
        staging_table: str,
    ) -> List[Dict[str, Any]]:
        """Return latest prior positions in scope that are absent from the incoming snapshot."""
        from core.repositories.impala_connection import impala_manager

        query = f"""
            WITH incoming_scope AS (
                SELECT DISTINCT
                    COALESCE(valid_portfolio, portfolio) AS portfolio,
                    position_basis,
                    CAST(reporting_date AS STRING) AS reporting_date,
                    src_system
                FROM {staging_table}
                WHERE COALESCE(valid_portfolio, portfolio) IS NOT NULL
                  AND TRIM(COALESCE(valid_portfolio, portfolio)) != ''
                  AND position_basis IS NOT NULL
                  AND TRIM(position_basis) != ''
                  AND reporting_date IS NOT NULL
                  AND CAST(reporting_date AS STRING) != ''
                  AND portfolio_status = 'PASS'
            ),
            incoming_presence AS (
                SELECT DISTINCT
                    COALESCE(valid_portfolio, portfolio) AS portfolio,
                    position_basis,
                    CAST(reporting_date AS STRING) AS reporting_date,
                    src_system,
                    NULLIF(TRIM(COALESCE(final_isin, isin)), '') AS incoming_isin,
                    NULLIF(TRIM(COALESCE(
                        matched_security_name,
                        security_full_name,
                        security_short_name
                    )), '') AS incoming_security_label
                FROM {staging_table}
                WHERE COALESCE(valid_portfolio, portfolio) IS NOT NULL
                  AND TRIM(COALESCE(valid_portfolio, portfolio)) != ''
                  AND position_basis IS NOT NULL
                  AND TRIM(position_basis) != ''
                  AND reporting_date IS NOT NULL
                  AND CAST(reporting_date AS STRING) != ''
                  AND portfolio_status = 'PASS'
            ),
            prior_ranked AS (
                SELECT
                    s.portfolio AS scope_portfolio,
                    s.position_basis AS scope_position_basis,
                    s.reporting_date AS scope_reporting_date,
                    s.src_system AS scope_src_system,
                    p.portfolio,
                    p.security_label,
                    p.position_basis,
                    p.position_date,
                    p.src_system,
                    p.processing_date,
                    p.isin,
                    p.source_table,
                    p.realized_pnl_fc,
                    p.realized_pnl_lc,
                    p.provision_fc,
                    p.provision_lc,
                    p.dividend_fc,
                    p.dividend_lc,
                    p.uncall_fc,
                    p.uncall_lc,
                    p.pipeline_fc,
                    p.pipeline_lc,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            s.portfolio,
                            s.position_basis,
                            s.reporting_date,
                            s.src_system,
                            p.security_label
                        ORDER BY
                            CAST(p.position_date AS STRING) DESC,
                            COALESCE(p.version_id, CAST(0 AS BIGINT)) DESC
                    ) AS rn
                FROM incoming_scope s
                JOIN {db}.cis_position p
                  ON p.portfolio = s.portfolio
                 AND p.position_basis = s.position_basis
                 AND p.src_system = s.src_system
                 AND CAST(p.position_date AS STRING) <= s.reporting_date
                 AND (p.is_latest = true OR p.is_latest IS NULL)
                 AND COALESCE(CAST(p.quantity AS DECIMAL(30,8)), CAST(0 AS DECIMAL(30,8))) != CAST(0 AS DECIMAL(30,8))
            )
            SELECT
                p.portfolio,
                p.security_label,
                p.position_basis,
                p.position_date AS previous_position_date,
                p.scope_reporting_date AS close_position_date,
                p.src_system,
                p.processing_date AS previous_processing_date,
                p.isin,
                p.source_table,
                p.realized_pnl_fc,
                p.realized_pnl_lc,
                p.provision_fc,
                p.provision_lc,
                p.dividend_fc,
                p.dividend_lc,
                p.uncall_fc,
                p.uncall_lc,
                p.pipeline_fc,
                p.pipeline_lc
            FROM prior_ranked p
            LEFT JOIN incoming_presence i_isin
              ON i_isin.portfolio = p.scope_portfolio
             AND i_isin.position_basis = p.scope_position_basis
             AND i_isin.reporting_date = p.scope_reporting_date
             AND i_isin.src_system = p.scope_src_system
             AND i_isin.incoming_isin IS NOT NULL
             AND p.isin IS NOT NULL
             AND TRIM(p.isin) != ''
             AND i_isin.incoming_isin = TRIM(p.isin)
            LEFT JOIN incoming_presence i_name
              ON i_name.portfolio = p.scope_portfolio
             AND i_name.position_basis = p.scope_position_basis
             AND i_name.reporting_date = p.scope_reporting_date
             AND i_name.src_system = p.scope_src_system
             AND i_name.incoming_security_label IS NOT NULL
             AND i_name.incoming_security_label = p.security_label
            WHERE p.rn = 1
              AND i_isin.portfolio IS NULL
              AND i_name.portfolio IS NULL
            ORDER BY p.scope_reporting_date, p.portfolio, p.position_basis, p.security_label
        """
        return impala_manager.execute_query(query, database=db) or []

    def _upsert_authoritative_close_rows(
        self,
        *,
        db: str,
        rows: List[Dict[str, Any]],
        processing_date: str,
    ) -> int:
        """Write zero-quantity closure rows for positions absent from an authoritative snapshot."""
        from core.repositories.impala_connection import impala_manager

        if not rows:
            return 0

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        values = []
        for row in rows:
            portfolio = row.get('portfolio')
            security = row.get('security_label')
            basis = row.get('position_basis')
            position_date = str(row.get('close_position_date') or '')[:10]
            src_system = row.get('src_system')
            pos_id = (
                "ABS(CAST(fnv_hash(CONCAT_WS('|', "
                f"{self._sql_literal(portfolio)}, "
                f"{self._sql_literal(security)}, "
                f"{self._sql_literal(basis)}, "
                f"{self._sql_literal(position_date)}, "
                f"{self._sql_literal(src_system)}"
                ")) AS BIGINT))"
            )
            values.append(
                f"""(
                    {pos_id},
                    CAST(UNIX_TIMESTAMP() * 1000 AS BIGINT),
                    {self._sql_literal(portfolio)},
                    {self._sql_literal(security)},
                    {self._sql_literal(basis)},
                    {self._sql_literal(position_date)},
                    {self._sql_literal(src_system)},
                    {self._sql_literal(processing_date)},
                    CAST(0 AS DECIMAL(30,8)),
                    CAST(0 AS DECIMAL(30,8)),
                    CAST(0 AS DECIMAL(30,8)),
                    CAST(0 AS DECIMAL(30,8)),
                    CAST(0 AS DECIMAL(30,8)),
                    CAST(0 AS DECIMAL(30,8)),
                    CAST(0 AS DECIMAL(30,8)),
                    CAST(0 AS DECIMAL(30,8)),
                    CAST(0 AS DECIMAL(30,8)),
                    CAST(0 AS DECIMAL(30,8)),
                    {self._sql_decimal(row.get('provision_lc'))},
                    {self._sql_decimal(row.get('provision_fc'))},
                    {self._sql_decimal(row.get('dividend_fc'))},
                    {self._sql_decimal(row.get('dividend_lc'))},
                    {self._sql_decimal(row.get('realized_pnl_fc'))},
                    {self._sql_decimal(row.get('realized_pnl_lc'))},
                    {self._sql_literal(row.get('isin'))},
                    CAST(0 AS DECIMAL(30,8)),
                    {self._sql_literal(row.get('source_table'))},
                    {self._sql_literal(timestamp)},
                    {self._sql_decimal(row.get('uncall_fc'))},
                    {self._sql_decimal(row.get('uncall_lc'))},
                    {self._sql_decimal(row.get('pipeline_fc'))},
                    {self._sql_decimal(row.get('pipeline_lc'))},
                    'INT',
                    true
                )"""
            )

        impala_manager.execute_write(
            f"""
            UPSERT INTO {db}.cis_position (
                position_id,
                version_id,
                portfolio,
                security_label,
                position_basis,
                position_date,
                src_system,
                processing_date,
                quantity,
                average_cost_fc,
                cost_fc,
                market_value_fc,
                net_book_value_fc,
                unrealized_pnl_fc,
                cost_lc,
                market_value_lc,
                net_book_value_lc,
                unrealized_pnl_lc,
                provision_lc,
                provision_fc,
                dividend_fc,
                dividend_lc,
                realized_pnl_fc,
                realized_pnl_lc,
                isin,
                average_cost_lc,
                source_table,
                processing_timestamp,
                uncall_fc,
                uncall_lc,
                pipeline_fc,
                pipeline_lc,
                position_type,
                is_latest
            ) VALUES {', '.join(values)}
            """,
            database=db
        )
        return len(rows)

    def _carry_forward_authoritative_close_rows(
        self,
        *,
        db: str,
        rows: List[Dict[str, Any]],
        processing_date: str,
    ) -> int:
        """Carry backdated zero-quantity closure rows forward like normal USER_UPLOAD INT rows."""
        from datetime import date as _calendar_date
        from core.repositories.impala_connection import impala_manager

        backdated_rows = [
            row for row in rows
            if (row.get('src_system') == 'USER_UPLOAD'
                and str(row.get('close_position_date') or '')[:10] < _calendar_date.today().isoformat())
        ]
        if not backdated_rows:
            return 0

        min_close_date = min(str(row.get('close_position_date') or '')[:10] for row in backdated_rows)
        if not min_close_date:
            return 0

        today_iso = _calendar_date.today().isoformat()
        min_close_key = min_close_date.replace('-', '')
        today_key = today_iso.replace('-', '')
        biz_dates_rows = impala_manager.execute_query(
            f"""
            SELECT contextual_today AS biz_date
            FROM {db}.gmp_cis_sta_dly_alldatesinfo
            WHERE src_system = 'gmp'
              AND sub_system  = 'cis'
              AND data_frq    = 'dly'
              AND record_type = 'D'
              AND CAST(contextual_today AS BIGINT) > {min_close_key}
              AND CAST(contextual_today AS BIGINT) <= {today_key}
            ORDER BY contextual_today ASC
            """,
            database=db
        ) or []

        def _to_iso(value: Any) -> str:
            raw = str(value).strip()[:10].replace('-', '').replace('/', '')
            if len(raw) == 8 and raw.isdigit():
                return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
            return str(value)[:10]

        biz_dates = [_to_iso(row.get('biz_date')) for row in biz_dates_rows if row.get('biz_date')]
        carried = 0

        for row in backdated_rows:
            portfolio = row.get('portfolio', '')
            security = row.get('security_label', '')
            basis = row.get('position_basis', '')
            upload_date = str(row.get('close_position_date') or '')[:10]
            if not portfolio or not security or not upload_date:
                continue

            for biz_date in biz_dates:
                if biz_date <= upload_date:
                    continue
                exists_rows = impala_manager.execute_query(
                    f"""
                    SELECT COUNT(*) AS cnt
                    FROM {db}.cis_position
                    WHERE portfolio       = {self._sql_literal(portfolio)}
                      AND security_label  = {self._sql_literal(security)}
                      AND position_basis  = {self._sql_literal(basis)}
                      AND src_system      = 'USER_UPLOAD'
                      AND CAST(position_date AS STRING) = {self._sql_literal(biz_date)}
                      AND (is_latest = true OR is_latest IS NULL)
                    """,
                    database=db
                ) or [{}]
                if int(exists_rows[0].get('cnt', 0) or 0) > 0:
                    break

                carry_row = dict(row)
                carry_row['close_position_date'] = biz_date
                self._upsert_authoritative_close_rows(
                    db=db,
                    rows=[carry_row],
                    processing_date=processing_date,
                )
                carried += 1

        return carried

    def is_position_upload(self, upload: Dict[str, Any]) -> bool:
        """Return True if this upload's target table is one of the 5 position sources."""
        target = (upload.get('target_table_name') or '').lower().split('.')[-1]
        return target in self.POSITION_TARGET_TABLES

    def run_position_etl(
        self,
        upload_id: str,
        src_id: str,
        processing_date: str,
        updated_by: str,
        auto_create_security: bool = False,
        partial_upload: bool = False,
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Execute the position upload transform pipeline for a given partition.

        auto_create_security: defaults to False. When False (default), Step 5B
          does not create any new cis_security records — every row that would
          otherwise have been 'NOT_FOUND: Create new security' is failed
          instead, and position_upload_report reflects it as INVALID. Pass
          True to opt in to auto-creating new securities AND parties (Step 5C).

        Runs the Hive/Impala equivalent of position_upload_transform_optimized.sql:
          Step 1 — build pos_stage_1_base_{ETL_SFX} from position_upload_standardized
          Step 2 — portfolio validation
          Step 3 — security ISIN match
          Step 4 — security fallback match
          Step 5 — price lookup
          Step 5B — auto-create new cis_security records (when auto_create_security=True)
          Step 5C — auto-create new cis_party (issuer) records for non-GMP uploads
                    (when auto_create_security=True); reuses existing parties where
                    possible; never creates duplicates; skipped for GMP src_ids.
          Step 6 — consolidated staging
          Step 7A — upsert into cis_position
          Step 7B — write position_upload_report

        Args:
            upload_id: Upload record ID (for status tracking)
            src_id:    Source partition value (e.g. 'cis_user_sta_adhoc_position_1')
            processing_date: YYYYMMDD partition value
            updated_by: Username triggering the ETL
            partial_upload: When True on user-upload sources, skip authoritative
                missing-position closure and treat the file as a partial patch.

        Returns:
            Tuple of (success, message, result_dict)
        """
        logger.info(f"[position_etl] run_position_etl ENTERED src_id={src_id} date={processing_date} user={updated_by}")

        from core.repositories.impala_connection import impala_manager
        from core.notifications import notify_user, notify_admins
        from core.notifications.constants import (
            EVT_UPLOAD_STARTED, EVT_UPLOAD_STEP,
            EVT_UPLOAD_COMPLETED, EVT_UPLOAD_FAILED,
        )

        result = {'src_id': src_id, 'processing_date': processing_date}

        _notif_base = {
            'upload_id':       upload_id,
            'src_id':          src_id,
            'processing_date': processing_date,
        }

        try:
            import time as _etl_time
            _etl_t0 = _etl_time.time()
            logger.info(
                f"[position_etl] Starting ETL for src_id={src_id} "
                f"processing_date={processing_date} by {updated_by}"
            )
            notify_user(updated_by, EVT_UPLOAD_STARTED, {
                **_notif_base,
                'message': f'Position ETL started for {src_id} ({processing_date})',
            })

            db = settings.IMPALA_CONFIG['DATABASE']
            reconciliation_mode = self.get_position_reconciliation_mode(
                src_id, partial_upload=partial_upload
            )
            result['reconciliation_mode'] = reconciliation_mode
            result['partial_upload'] = reconciliation_mode == 'PARTIAL'

            # Namespaces every pos_stage_* staging table by upload_id so two
            # concurrent ETL runs never collide on the same physical Kudu
            # table (previously all runs shared fixed table names like
            # pos_stage_1_base -- a second upload's DROP/CREATE TABLE against
            # the same name could block indefinitely on a lock held by a
            # first, possibly-dead, run; a 30-hour-stuck upload was traced to
            # exactly this). Sanitized to a valid identifier fragment since
            # upload_id may contain hyphens or other characters Impala/Kudu
            # table names don't allow unquoted.
            import re as _etl_re
            ETL_SFX = _etl_re.sub(r'[^A-Za-z0-9]', '_', str(upload_id))[:40]

            # Register this run so the "Stop" button (see api_cancel_etl in
            # upload/views.py) can signal it. _step_time checks this after
            # every step and raises EtlCancelled if a Stop was requested.
            register_etl_run(upload_id)

            # ETL steps in order — used to compute % progress for the UI progress bar.
            _ETL_STEPS = [
                'Step 0', 'Step 1', 'Step 2', 'Step 3', 'Step 4',
                'Step 4B', 'Step 5', 'Step 5B', 'Step 5C', 'Step 6', 'Step 6B',
                'Step 6C', 'Step 7A', 'Step 7A2', 'Step 7B',
            ]
            _step_index = [0]  # mutable counter for closure

            def _step_time(label: str, t_start: float) -> float:
                elapsed = _etl_time.time() - t_start
                logger.info(f"[position_etl] {label} — {elapsed:.1f}s")
                _step_index[0] += 1
                pct = min(int(_step_index[0] / len(_ETL_STEPS) * 100), 95)
                try:
                    # persist=False: skip Kudu UPSERT for transient progress ticks.
                    # WS delivers it instantly; in-memory pending covers same-worker
                    # fallback. No Impala round-trip in the ETL hot path.
                    notify_user(updated_by, EVT_UPLOAD_STEP, {
                        **_notif_base,
                        'step':    label,
                        'elapsed': round(elapsed, 1),
                        'pct':     pct,
                        'message': f'{label} ({elapsed:.1f}s)',
                    }, persist=False)
                except Exception:
                    pass
                # Checked once per step (not mid-step) -- a Stop request takes
                # effect at the next natural step boundary rather than
                # interrupting an in-flight Impala query.
                if is_etl_cancel_requested(upload_id):
                    raise EtlCancelled(f"Stopped by user after: {label}")
                return _etl_time.time()

            def _cleanup_staging_tables() -> None:
                """Drop this run's namespaced staging tables. Shared by the
                normal completion path and the Stop/cancel path -- on cancel
                these tables would otherwise be orphaned in Kudu forever
                (no other job ever revisits a specific upload_id's tables)."""
                for tbl in [
                    f'pos_stage_1_base_{ETL_SFX}', f'pos_stage_1b_country_{ETL_SFX}', f'pos_stage_2_portfolio_{ETL_SFX}',
                    f'pos_stage_3_security_{ETL_SFX}',
                    f'pos_stage_4_security_fallback_{ETL_SFX}', f'pos_stage_4b_abbrev_match_{ETL_SFX}',
                    f'pos_stage_4_tier_update_{ETL_SFX}', f'pos_stage_4_collision_update_{ETL_SFX}',
                    f'pos_stage_5_price_{ETL_SFX}', f'pos_stage_5b_candidates_{ETL_SFX}',
                    f'position_upload_staging_{ETL_SFX}',
                ]:
                    try:
                        impala_manager.execute_write(
                            f"DROP TABLE IF EXISTS {tbl}", database=db
                        )
                    except Exception:
                        pass

            def _count(table: str, where: str = '') -> int:
                """Return COUNT(*) from a staging table; -1 on error (non-fatal)."""
                try:
                    clause = f"WHERE {where}" if where else ''
                    rows = impala_manager.execute_query(
                        f"SELECT COUNT(*) AS n FROM {table} {clause}", database=db
                    )
                    return int((rows or [{}])[0].get('n', 0))
                except Exception as _ce:
                    logger.debug(f"[position_etl] _count({table}) failed: {_ce}")
                    return -1

            def _breakdown(table: str, col: str) -> str:
                """Return 'VAL1:N VAL2:N ...' for quick per-value counts; '' on error."""
                try:
                    rows = impala_manager.execute_query(
                        f"SELECT {col}, COUNT(*) AS n FROM {table} GROUP BY {col} ORDER BY n DESC",
                        database=db
                    )
                    return '  '.join(f"{r.get(col,'?')}:{r.get('n',0)}" for r in (rows or []))
                except Exception as _be:
                    logger.debug(f"[position_etl] _breakdown({table},{col}) failed: {_be}")
                    return ''

            def _sample_fails(table: str, status_col: str, limit: int = 5,
                               portfolio_expr: str = 'portfolio',
                               name_expr: str = 'security_full_name',
                               isin_expr: str = 'isin',
                               from_clause: str = None) -> None:
                """Log up to `limit` failing rows — portfolio + security + reason.

                `from_clause` lets callers whose table lacks a bare
                portfolio/isin column (e.g. pos_stage_4_security_fallback_<suffix>,
                which only has row_id + upload_isin) supply a JOIN back to a
                table that has it, with matching *_expr overrides.
                """
                try:
                    rows = impala_manager.execute_query(
                        f"""
                        SELECT {portfolio_expr} AS portfolio, {name_expr} AS security_full_name,
                               {isin_expr} AS isin, {status_col}
                        FROM {from_clause or table}
                        WHERE {status_col} LIKE 'FAIL%'
                           OR {status_col} LIKE 'NOT_FOUND%'
                        LIMIT {limit}
                        """,
                        database=db
                    )
                    for r in (rows or []):
                        logger.warning(
                            f"[position_etl]   FAIL sample — "
                            f"portfolio={r.get('portfolio')} "
                            f"isin={r.get('isin')} "
                            f"name={r.get('security_full_name')} "
                            f"status={r.get(status_col)}"
                        )
                except Exception as _se:
                    logger.debug(f"[position_etl] _sample_fails({table}) failed: {_se}")

            # ------------------------------------------------------------------
            # safe_decimal(col, dec_type): generates SQL that handles every
            # real-world dirty numeric format before CAST to DECIMAL:
            #   1. CAST source col to STRING (handles numeric source types)
            #   2. TRIM whitespace
            #   3. Strip commas (1,234.56 → 1234.56)
            #   4. Strip currency symbols and percent signs ($, £, %, etc.)
            #   5. Convert parentheses-negatives: (123.45) → -123.45
            #   6. Convert lone dash/en-dash meaning zero → '0'
            #   7. regexp_extract to pull only the leading -?digits.digits part
            #   8. NULLIF empty string → NULL
            #   9. Final CAST to target DECIMAL type
            #  10. Integer-digit-count guard: if the cleaned value has more
            #      integer digits than dec_type's precision-scale allows,
            #      NULL it out before the CAST instead of letting Impala
            #      raise "UDF ERROR: String to Decimal cast overflowed" --
            #      a runtime error step 9 alone does NOT protect against,
            #      and which aborts the entire batch INSERT, not just the
            #      offending row.
            # ------------------------------------------------------------------
            def safe_decimal(col: str, dec_type: str) -> str:
                import re as _re_sd
                cleaned = (
                    f"NULLIF(regexp_extract("
                    f"regexp_replace("
                    f"regexp_replace("
                    f"regexp_replace("
                    f"regexp_replace("
                    f"TRIM(CAST({col} AS STRING)),"
                    f" ',', ''),"           # strip thousands separator
                    f" '[\\\\$£€¥%]', '')," # strip currency/percent symbols
                    f" '^\\\\(([0-9]+\\\\.?[0-9]*)\\\\)$', '-\\\\1')," # (123.45) → -123.45
                    f" '^[-–—]+$', '0'),"   # lone dash/en-dash → 0
                    f" '^-?[0-9]+(\\\\.?[0-9]*)?([eE][+-]?[0-9]+)?', 0),"
                    f" '')"
                )
                _m = _re_sd.match(r'DECIMAL\((\d+),\s*(\d+)\)', dec_type)
                if not _m:
                    return f"CAST({cleaned} AS {dec_type})"
                _max_int_digits = int(_m.group(1)) - int(_m.group(2))
                return (
                    f"CAST(CASE WHEN LENGTH(REGEXP_EXTRACT({cleaned}, '^-?([0-9]+)', 1)) > {_max_int_digits} "
                    f"THEN NULL ELSE {cleaned} END AS {dec_type})"
                )

            def clean_isin(col: str) -> str:
                """Return NULL when col is a known placeholder; otherwise return TRIM(UPPER(col))."""
                return (
                    f"NULLIF(NULLIF(NULLIF(NULLIF(NULLIF(NULLIF(NULLIF("
                    f"UPPER(TRIM(CAST({col} AS STRING))),"
                    f" 'NA'), 'N/A'), 'NIL'), 'NONE'), '-'), 'N.A.'), 'NAP')"
                )

            def clean_ticker(col: str) -> str:
                """Normalize Bloomberg ticker suffix (ISO→Bloomberg) then null-out placeholders.

                Examples:
                  'DBS SG'  → 'DBS SP'   (Singapore: ISO SG → Bloomberg SP)
                  'MAY MY'  → 'MAY MK'   (Malaysia:  ISO MY → Bloomberg MK)
                  'NA'      → NULL
                  'DBS SP'  → 'DBS SP'   (already correct, unchanged)
                """
                # ISO-to-Bloomberg exchange suffix map.
                # Key = ISO alpha-2 or other non-Bloomberg code that may appear in upload files.
                # Value = correct Bloomberg exchange suffix.
                _ISO_TO_BB = {
                    # Asia Pacific
                    'SG': 'SP',   # Singapore
                    'MY': 'MK',   # Malaysia
                    'ID': 'IJ',   # Indonesia
                    'TH': 'TB',   # Thailand
                    'PH': 'PM',   # Philippines
                    'VN': 'VN',   # Vietnam (same)
                    'IN': 'IS',   # India NSE  (IB = BSE, IS = NSE — default NSE)
                    'CN': 'CH',   # China Shanghai
                    'HK': 'HK',   # Hong Kong (same)
                    'TW': 'TT',   # Taiwan
                    'KR': 'KS',   # Korea
                    'JP': 'JT',   # Japan Tokyo
                    'AU': 'AT',   # Australia
                    'NZ': 'NZ',   # New Zealand (same)
                    # Europe
                    'GB': 'LN',   # UK London
                    'DE': 'GY',   # Germany Xetra
                    'FR': 'FP',   # France Euronext
                    'NL': 'NA',   # Netherlands
                    'CH': 'SW',   # Switzerland
                    'SE': 'SS',   # Sweden
                    'NO': 'NO',   # Norway (same)
                    'DK': 'DC',   # Denmark
                    'FI': 'FH',   # Finland
                    'IT': 'IM',   # Italy
                    'ES': 'SM',   # Spain
                    # Americas
                    'US': 'US',   # USA (same)
                    'CA': 'CN',   # Canada
                    'BR': 'BZ',   # Brazil
                    'MX': 'MM',   # Mexico
                    # Middle East / Africa
                    'AE': 'UH',   # UAE
                    'SA': 'AB',   # Saudi Arabia
                    'ZA': 'SJ',   # South Africa
                }
                # Build a SQL CASE that rewrites the suffix when it matches an ISO code.
                # Pattern: ticker = 'ROOT XX' where XX is the last word (2-3 chars).
                # We extract the root (everything before last space) and the suffix (last word),
                # then reassemble with the Bloomberg suffix if the ISO code is known.
                _when_clauses = '\n                    '.join(
                    f"WHEN REGEXP_EXTRACT(UPPER(TRIM(CAST({col} AS STRING))), "
                    f"'^(.+)\\\\s+{iso}$', 1) != '' "
                    f"THEN CONCAT(REGEXP_EXTRACT(UPPER(TRIM(CAST({col} AS STRING))), "
                    f"'^(.+)\\\\s+{iso}$', 1), ' {bb}')"
                    for iso, bb in _ISO_TO_BB.items()
                    if iso != bb   # only emit WHEN for codes that actually change
                )
                return (
                    f"NULLIF(NULLIF(NULLIF(NULLIF(NULLIF(NULLIF(NULLIF("
                    f"CASE\n                    {_when_clauses}\n"
                    f"                    ELSE UPPER(TRIM(CAST({col} AS STRING)))\n"
                    f"                END,"
                    f" 'NA'), 'N/A'), 'NIL'), 'NONE'), '-'), 'N.A.'), 'NAP')"
                )

            def abbreviate_security_name(name: str, max_len: int = 35) -> str:
                """Abbreviate a company name to max_len chars using a word-level dict,
                initialism fallback, then hard word-boundary truncation.

                Pre-processing strips punctuation (.,) so that variants like
                'CO.,LTD', 'CO. LTD.', 'CO LTD' all normalise to 'CO LTD'
                before dict substitution. Old GMP abbreviations (MGT) are also
                expanded back to their full form so the dict can re-abbreviate
                them consistently.
                """
                import re as _re
                _ABBREV = {
                    "CORPORATION":      "CORP",
                    "INCORPORATED":     "INC",
                    "BERHAD":           "BHD",
                    "SENDIRIAN":        "SDN",
                    # PRIVATE → PTE so 'PRIVATE LIMITED' and 'PTE LTD' both become 'PTE LTD'
                    "PRIVATE":          "PTE",
                    "LIMITED":          "LTD",
                    "COMPANY":          "CO",
                    "HOLDINGS":         "HLDGS",
                    "INTERNATIONAL":    "INTL",
                    "INVESTMENTS":      "INVT",
                    "INVESTMENT":       "INVT",
                    "MANAGEMENT":       "MGMT",
                    "INDUSTRIES":       "INDS",
                    "INDUSTRY":         "IND",
                    "TECHNOLOGIES":     "TECH",
                    "TECHNOLOGY":       "TECH",
                    "INFRASTRUCTURE":   "INFRA",
                    "DEVELOPMENT":      "DEV",
                    "ENTERPRISE":       "ENTPR",
                    "ENTERPRISES":      "ENTPR",
                    "RESOURCES":        "RES",
                    "PROPERTIES":       "PROP",
                    "CAPITAL":          "CAP",
                    "FINANCIAL":        "FIN",
                    "SERVICES":         "SVCS",
                    "SERVICE":          "SVC",
                    "GLOBAL":           "GLB",
                    "NATIONAL":         "NATL",
                    "REGIONAL":         "RGNL",
                    "INDUSTRIAL":       "INDL",
                    "MANUFACTURING":    "MFG",
                    "ENGINEERING":      "ENGG",
                    "CONSTRUCTION":     "CONST",
                    "DISTRIBUTION":     "DIST",
                    "ASSOCIATION":      "ASSOC",
                    "FOUNDATION":       "FNDN",
                    "EXCHANGE":         "EXCH",
                    "COMMUNICATIONS":   "COMM",
                    "COMMUNICATION":    "COMM",
                    "INSURANCE":        "INS",
                    "ASSURANCE":        "ASSUR",
                    "HEALTHCARE":       "HLTHCR",
                    "PHARMACEUTICALS":  "PHARMA",
                    "PHARMACEUTICAL":   "PHARMA",
                    "PLANTATIONS":      "PLANT",
                    "PLANTATION":       "PLANT",
                    "PETROLEUM":        "PETRO",
                    "BANK":             "BK",
                    "FUND":             "FD",
                    "GROUP":            "GRP",
                }
                # Old GMP abbreviations that differ from our standard form —
                # expand them first so the dict can re-abbreviate consistently.
                _EXPAND = {
                    "MGT": "MANAGEMENT",   # GMP stored 'MGT', we use 'MGMT'
                }
                if not name:
                    return name
                result = name.upper().strip()
                # Pre-pass: strip trailing/embedded punctuation from words
                # so 'CO.,LTD' → 'CO LTD', 'LTD.' → 'LTD', 'CO. LTD.' → 'CO LTD'
                result = _re.sub(r'[.,]+', ' ', result)
                result = ' '.join(result.split())
                # Expand old abbreviations back to full words before dict pass
                for old_abbr, full in _EXPAND.items():
                    result = _re.sub(r'\b' + old_abbr + r'\b', full, result)
                result = ' '.join(result.split())
                # Pass 1: whole-word dict substitution, longest words first
                for word, abbr in sorted(_ABBREV.items(), key=lambda x: -len(x[0])):
                    result = _re.sub(r'\b' + word + r'\b', abbr, result)
                result = ' '.join(result.split())
                if len(result) <= max_len:
                    return result
                # Pass 2: shorten remaining words > 6 chars, longest first
                _abbrev_values = set(_ABBREV.values())
                words = result.split()
                for i, w in sorted(enumerate(words), key=lambda x: -len(x[1])):
                    if len(result) <= max_len:
                        break
                    if len(w) > 6 and w not in _abbrev_values and '.' not in w:
                        words[i] = w[:4] + '.'
                        result = ' '.join(words)
                result = ' '.join(result.split())
                if len(result) <= max_len:
                    return result
                # Pass 3: hard truncate at last word boundary
                truncated = result[:max_len]
                last_space = truncated.rfind(' ')
                return truncated[:last_space] if last_space > 0 else truncated

            # ------------------------------------------------------------------
            # Step 0: Standardize — map raw source table columns into
            #         position_upload_standardized (equivalent of Position_insert.sql).
            #         This INSERT is partitioned by (src_id, processing_date).
            #         Each source table has a different column layout — we map
            #         them here to the common schema.
            # ------------------------------------------------------------------
            # Step 0 writes into position_upload_standardized whose numeric columns are DECIMAL —
            # safe_decimal() handles dirty formatting (commas, currency symbols, parentheses).
            # Step 1 reads those DECIMALs back and wraps them with COALESCE(col, 0).

            STANDARDIZE_SELECT = {
                # ------------------------------------------------------------------
                # position_1: pipe-separated file with 9 columns
                #   REPORTING_DATE|Portfolio|Client_Num|Exchange_Quoted|ISIN_Code|
                #   Counter|Quantity_Yesterday|Movement|Quantity_Today
                # column mapping per Position_insert.sql:
                #   counter → security_full_name
                #   isin_code → isin
                #   exchange_quoted → exchange_code
                #   quantity_today → quantity  (stored as STRING in standardized table)
                # ------------------------------------------------------------------
                # position_1: pipe-separated, 9 cols
                # REPORTING_DATE|Portfolio|Client_Num|Exchange_Quoted|ISIN_Code|Counter|Quantity_Yesterday|Movement|Quantity_Today
                'cis_user_sta_adhoc_position_1': f"""
                    SELECT
                        portfolio                                       AS portfolio,
                        counter                                         AS security_full_name,
                        NULL                                            AS security_short_name,
                        {clean_isin('isin_code')}                       AS isin,
                        NULL                                            AS ticker,
                        {safe_decimal('quantity_today', 'DECIMAL(30,8)')} AS quantity,
                        CAST(NULL AS DECIMAL(30,8))                     AS shares_outstanding,
                        CAST(NULL AS DECIMAL(30,8))                     AS shares_issued,
                        CAST(NULL AS DECIMAL(10,6))                     AS pct_holding,
                        CAST(NULL AS DECIMAL(30,8))                     AS market_price,
                        CAST(NULL AS DECIMAL(30,8))                     AS average_cost,
                        CAST(NULL AS DECIMAL(30,8))                     AS cost_fc,
                        CAST(NULL AS DECIMAL(30,8))                     AS market_value_fc,
                        CAST(NULL AS DECIMAL(30,8))                     AS net_book_value_fc,
                        CAST(NULL AS DECIMAL(30,8))                     AS unrealized_pnl_fc,
                        CAST(NULL AS DECIMAL(30,8))                     AS provision_fc,
                        CAST(NULL AS DECIMAL(30,8))                     AS cost_lc,
                        CAST(NULL AS DECIMAL(30,8))                     AS market_value_lc,
                        CAST(NULL AS DECIMAL(30,8))                     AS net_book_value_lc,
                        CAST(NULL AS DECIMAL(30,8))                     AS unrealized_pnl_lc,
                        CAST(NULL AS DECIMAL(30,8))                     AS provision_lc,
                        NULL                                            AS product_type,
                        NULL                                            AS security_type,
                        NULL                                            AS quoted_unquoted,
                        NULL                                            AS industry,
                        NULL                                            AS fin_nonfin_co,
                        NULL                                            AS issuer_type,
                        NULL                                            AS reits_or_fund_y_n,
                        p1.exchange_quoted                              AS exchange,
                        NULL                                            AS country_code,
                        exc.country_name                                AS country_of_exchange,
                        NULL                                            AS country_of_incorporation,
                        NULL                                            AS country_of_risk,
                        NULL                                            AS country_of_operation,
                        NULL                                            AS security_currency,
                        NULL                                            AS corp_code,
                        NULL                                            AS branch_code,
                        NULL                                            AS cost_centre,
                        NULL                                            AS cels,
                        NULL                                            AS bwcif_sg,
                        NULL                                            AS bwcif_ovs,
                        NULL                                            AS mas_6d_code_sg,
                        NULL                                            AS mas_6d_code_ovs,
                        'TRADED'                                        AS position_basis,
                        p1.reporting_date                               AS reporting_date,
                        NULL                                            AS maturity_date,
                        'USER_UPLOAD'                                   AS src_system,
                        'user'                                          AS sub_system,
                        'sta'                                           AS data_cat,
                        'adhoc'                                         AS data_frq,
                        'cis_user_sta_adhoc_position_1'                 AS source_table,
                        CURRENT_TIMESTAMP()                             AS etl_insert_ts,
                        'python_etl'                                    AS etl_batch_id,
                        CAST(NULL AS DECIMAL(30,8))                     AS dividend_fc
                    FROM {db}.cis_user_sta_adhoc_position_1 p1
                    LEFT JOIN (
                        -- Deduplicate LUT: one row per exchange_name.
                        -- When exchange maps to multiple countries (e.g. LSE→GB, LSE→LU),
                        -- prefer the country that exists in cis_security; else take MIN.
                        SELECT
                            exchange_name,
                            COALESCE(
                                MIN(CASE WHEN sec.exchange_code IS NOT NULL THEN lut.country_name END),
                                MIN(lut.country_name)
                            ) AS country_name
                        FROM (
                            SELECT
                                UPPER(TRIM(exchange_name)) AS exchange_name,
                                country_name
                            FROM {db}.cis_exchange_mapping_lut
                        ) lut
                        LEFT JOIN (
                            SELECT DISTINCT UPPER(TRIM(exchange_code)) AS exchange_code
                            FROM {db}.cis_security
                            WHERE is_active = true
                        ) sec ON lut.country_name = sec.exchange_code
                        GROUP BY lut.exchange_name
                    ) exc
                        ON UPPER(TRIM(p1.exchange_quoted)) = exc.exchange_name
                    WHERE p1.processing_date = '{processing_date}'
                      AND p1.src_id = '{src_id}'
                """,
                # position_2: portfolio_name, security_description, stock_name, isin_code, qty_held, shares_issued, pct_holding, country_id
                'cis_user_sta_adhoc_position_2': f"""
                    SELECT
                        portfolio_name                                  AS portfolio,
                        security_description                            AS security_full_name,
                        stock_name                                      AS security_short_name,
                        {clean_isin('isin_code')}                       AS isin,
                        NULL                                            AS ticker,
                        {safe_decimal('qty_held', 'DECIMAL(30,8)')} AS quantity,
                        {safe_decimal('shares_issued', 'DECIMAL(30,8)')} AS shares_outstanding,
                        CAST(NULL AS DECIMAL(30,8))                     AS shares_issued,
                        {safe_decimal('pct_holding', 'DECIMAL(10,6)')} AS pct_holding,
                        CAST(NULL AS DECIMAL(30,8))                     AS market_price,
                        CAST(NULL AS DECIMAL(30,8))                     AS average_cost,
                        CAST(NULL AS DECIMAL(30,8))                     AS cost_fc,
                        CAST(NULL AS DECIMAL(30,8))                     AS market_value_fc,
                        CAST(NULL AS DECIMAL(30,8))                     AS net_book_value_fc,
                        CAST(NULL AS DECIMAL(30,8))                     AS unrealized_pnl_fc,
                        CAST(NULL AS DECIMAL(30,8))                     AS provision_fc,
                        CAST(NULL AS DECIMAL(30,8))                     AS cost_lc,
                        CAST(NULL AS DECIMAL(30,8))                     AS market_value_lc,
                        CAST(NULL AS DECIMAL(30,8))                     AS net_book_value_lc,
                        CAST(NULL AS DECIMAL(30,8))                     AS unrealized_pnl_lc,
                        CAST(NULL AS DECIMAL(30,8))                     AS provision_lc,
                        NULL                                            AS product_type,
                        NULL                                            AS security_type,
                        NULL                                            AS quoted_unquoted,
                        NULL                                            AS industry,
                        NULL                                            AS fin_nonfin_co,
                        NULL                                            AS issuer_type,
                        NULL                                            AS reits_or_fund_y_n,
                        country_id                                      AS exchange,
                        country_id                                      AS country_code,
                        country_id                                      AS country_of_exchange,
                        country_id                                      AS country_of_incorporation,
                        NULL                                            AS country_of_risk,
                        NULL                                            AS country_of_operation,
                        NULL                                            AS security_currency,
                        NULL                                            AS corp_code,
                        NULL                                            AS branch_code,
                        NULL                                            AS cost_centre,
                        NULL                                            AS cels,
                        NULL                                            AS bwcif_sg,
                        NULL                                            AS bwcif_ovs,
                        NULL                                            AS mas_6d_code_sg,
                        NULL                                            AS mas_6d_code_ovs,
                        'TRADED'                                    AS position_basis,
                        reporting_date                                  AS reporting_date,
                        NULL                                            AS maturity_date,
                        'USER_UPLOAD'                                   AS src_system,
                        'user'                                          AS sub_system,
                        'sta'                                           AS data_cat,
                        'adhoc'                                         AS data_frq,
                        'cis_user_sta_adhoc_position_2'                 AS source_table,
                        CURRENT_TIMESTAMP()                             AS etl_insert_ts,
                        'python_etl'                                    AS etl_batch_id,
                        CAST(NULL AS DECIMAL(30,8))                     AS dividend_fc
                    FROM {db}.cis_user_sta_adhoc_position_2
                    WHERE processing_date = '{processing_date}'
                      AND src_id = '{src_id}'
                """,
                # position_3: account_name, asset_description_short, isin, shares_par_value, shares_outstanding_total, country_of_listing_code, reporting_date, position_basis
                # position_3 raw columns (confirmed live schema, 2026-08-04):
                #   ticker, security_desc, portfolio, quoted_unquoted, quantity_units,
                #   ccy, product_type, ctry_of_exchange, ctry_incorporation,
                #   total_cost_fc, mkt_value_fc, unrealised_pl_fc, total_cost_sgd,
                #   mkt_value_sgd, unrealised_pl_sgd, fx_rate (unused — no target
                #   column in position_upload_standardized, consistent with format 5),
                #   shares_outstanding_total, unit_cost, market_price, isin,
                #   position_basis, reporting_date
                'cis_user_sta_adhoc_position_3': f"""
                    SELECT
                        portfolio                                       AS portfolio,
                        security_desc                                   AS security_full_name,
                        NULL                                            AS security_short_name,
                        {clean_isin('isin')}                            AS isin,
                        {clean_ticker('ticker')}                        AS ticker,
                        {safe_decimal('quantity_units', 'DECIMAL(30,8)')} AS quantity,
                        {safe_decimal('shares_outstanding_total', 'DECIMAL(30,8)')} AS shares_outstanding,
                        {safe_decimal('shares_outstanding_total', 'DECIMAL(30,8)')} AS shares_issued,
                        CAST(NULL AS DECIMAL(10,6))                     AS pct_holding,
                        {safe_decimal('market_price', 'DECIMAL(30,8)')} AS market_price,
                        {safe_decimal('unit_cost', 'DECIMAL(30,8)')}    AS average_cost,
                        {safe_decimal('total_cost_fc', 'DECIMAL(30,8)')} AS cost_fc,
                        {safe_decimal('mkt_value_fc', 'DECIMAL(30,8)')} AS market_value_fc,
                        CAST(NULL AS DECIMAL(30,8))                     AS net_book_value_fc,
                        {safe_decimal('unrealised_pl_fc', 'DECIMAL(30,8)')} AS unrealized_pnl_fc,
                        CAST(NULL AS DECIMAL(30,8))                     AS provision_fc,
                        {safe_decimal('total_cost_sgd', 'DECIMAL(30,8)')} AS cost_lc,
                        {safe_decimal('mkt_value_sgd', 'DECIMAL(30,8)')} AS market_value_lc,
                        CAST(NULL AS DECIMAL(30,8))                     AS net_book_value_lc,
                        {safe_decimal('unrealised_pl_sgd', 'DECIMAL(30,8)')} AS unrealized_pnl_lc,
                        CAST(NULL AS DECIMAL(30,8))                     AS provision_lc,
                        product_type                                    AS product_type,
                        NULL                                            AS security_type,
                        quoted_unquoted                                 AS quoted_unquoted,
                        NULL                                            AS industry,
                        NULL                                            AS fin_nonfin_co,
                        NULL                                            AS issuer_type,
                        NULL                                            AS reits_or_fund_y_n,
                        ctry_of_exchange                                AS exchange,
                        NULL                                            AS country_code,
                        ctry_of_exchange                                AS country_of_exchange,
                        ctry_incorporation                              AS country_of_incorporation,
                        NULL                                            AS country_of_risk,
                        NULL                                            AS country_of_operation,
                        ccy                                             AS security_currency,
                        NULL                                            AS corp_code,
                        NULL                                            AS branch_code,
                        NULL                                            AS cost_centre,
                        NULL                                            AS cels,
                        NULL                                            AS bwcif_sg,
                        NULL                                            AS bwcif_ovs,
                        NULL                                            AS mas_6d_code_sg,
                        NULL                                            AS mas_6d_code_ovs,
                        'TRADED'                                    AS position_basis,
                        reporting_date                                  AS reporting_date,
                        NULL                                            AS maturity_date,
                        'USER_UPLOAD'                                   AS src_system,
                        'user'                                          AS sub_system,
                        'sta'                                           AS data_cat,
                        'adhoc'                                         AS data_frq,
                        'cis_user_sta_adhoc_position_3'                 AS source_table,
                        CURRENT_TIMESTAMP()                             AS etl_insert_ts,
                        'python_etl'                                    AS etl_batch_id,
                        CAST(NULL AS DECIMAL(30,8))                     AS dividend_fc
                    FROM {db}.cis_user_sta_adhoc_position_3
                    WHERE processing_date = '{processing_date}'
                      AND src_id = '{src_id}'
                """,
                # position_4: portfolio, security_full_name, product_type, security_type, gl_fund_type,
                #   quoted_unquoted, security_currency, quantity, cost_fc, net_book_value_fc,
                #   net_book_value_lc, local_currency_home_ccy, cost_lc, pct_holdings,
                #   no_of_shares_issues_by_the_company, country_of_incorporation, country_of_exchange,
                #   isin_code, ticker_code, industry, financial_non_financial_co, position_basis
                # Uses safe_decimal for dirty-data resilience (commas, currency symbols, parens).
                # If format 4 source is always clean numeric, replace with CAST for speed.
                'cis_user_sta_adhoc_position_4': f"""
                    SELECT
                        portfolio                                           AS portfolio,
                        security_full_name                                  AS security_full_name,
                        NULL                                                AS security_short_name,
                        {clean_isin('isin_code')}                           AS isin,
                        {clean_ticker('ticker_code')}                       AS ticker,
                        CAST(quantity AS DECIMAL(30,8))                     AS quantity,
                        CAST(no_of_shares_issues_by_the_company AS DECIMAL(30,8)) AS shares_outstanding,
                        CAST(no_of_shares_issues_by_the_company AS DECIMAL(30,8)) AS shares_issued,
                        CAST(pct_holdings AS DECIMAL(10,6))                 AS pct_holding,
                        CAST(NULL AS DECIMAL(30,8))                         AS market_price,
                        CAST(NULL AS DECIMAL(30,8))                         AS average_cost,
                        CAST(cost_fc AS DECIMAL(30,8))                      AS cost_fc,
                        CAST(market_value_fc AS DECIMAL(30,8))              AS market_value_fc,
                        CAST(NULL AS DECIMAL(30,8))                         AS net_book_value_fc,
                        CAST(NULL AS DECIMAL(30,8))                         AS unrealized_pnl_fc,
                        CAST(NULL AS DECIMAL(30,8))                         AS provision_fc,
                        CAST(cost_lc AS DECIMAL(30,8))                      AS cost_lc,
                        CAST(market_value_lc AS DECIMAL(30,8))              AS market_value_lc,
                        CAST(NULL AS DECIMAL(30,8))                         AS net_book_value_lc,
                        CAST(NULL AS DECIMAL(30,8))                         AS unrealized_pnl_lc,
                        CAST(NULL AS DECIMAL(30,8))                         AS provision_lc,
                        product_type                                        AS product_type,
                        security_type                                       AS security_type,
                        quoted_unquoted                                     AS quoted_unquoted,
                        industry                                            AS industry,
                        financial_non_financial_co                          AS fin_nonfin_co,
                        NULL                                                AS issuer_type,
                        NULL                                                AS reits_or_fund_y_n,
                        country_of_exchange                                 AS exchange,
                        NULL                                                AS country_code,
                        country_of_exchange                                 AS country_of_exchange,
                        country_of_incorporation                            AS country_of_incorporation,
                        NULL                                                AS country_of_risk,
                        NULL                                                AS country_of_operation,
                        security_currency                                   AS security_currency,
                        NULL                                                AS corp_code,
                        NULL                                                AS branch_code,
                        NULL                                                AS cost_centre,
                        NULL                                                AS cels,
                        NULL                                                AS bwcif_sg,
                        NULL                                                AS bwcif_ovs,
                        NULL                                                AS mas_6d_code_sg,
                        NULL                                                AS mas_6d_code_ovs,
                        'SETTLED'                                       AS position_basis,
                        reporting_date                                      AS reporting_date,
                        NULL                                                AS maturity_date,
                        'USER_UPLOAD'                                       AS src_system,
                        'user'                                              AS sub_system,
                        'sta'                                               AS data_cat,
                        'adhoc'                                             AS data_frq,
                        'cis_user_sta_adhoc_position_4'                     AS source_table,
                        CURRENT_TIMESTAMP()                                 AS etl_insert_ts,
                        'python_etl'                                        AS etl_batch_id,
                        CAST(NULL AS DECIMAL(30,8))                         AS dividend_fc
                    FROM {db}.cis_user_sta_adhoc_position_4
                    WHERE processing_date = '{processing_date}'
                      AND src_id = '{src_id}'
                """,
                # position_5: full schema with all numeric and MAS codes.
                # Country fields normalized via LEFT JOINs against
                # gmp_cis_sta_dly_country (full_name → label/code).
                # Impala does not support correlated scalar subqueries so each
                # country column gets its own aliased join (cn_exc, cn_inc, etc.).
                # CTE scans gmp_cis_sta_dly_country ONCE for the latest partition,
                # then the four country aliases (cn_exc/cn_inc/cn_rsk/cn_opr) join
                # against that single materialised result instead of repeating the
                # subquery four times — reduces the country table scan from 4× to 1×.
                'cis_user_sta_adhoc_position_5': f"""
                    WITH country_lut AS (
                        -- One row per full_name (deduped). If a country name maps to
                        -- multiple labels (e.g. 'United Kingdom' → 'GB' and 'UK'),
                        -- keep MIN(label) to avoid fan-out duplicates on the JOIN.
                        -- NOTE: processing_date is injected as a literal by the pre-resolve
                        -- step above (MAX(processing_date) resolved before plan compilation).
                        SELECT UPPER(TRIM(full_name)) AS full_name, MIN(label) AS label
                        FROM {db}.gmp_cis_sta_dly_country
                        WHERE processing_date = (
                            SELECT MAX(processing_date) FROM {db}.gmp_cis_sta_dly_country
                        )
                        GROUP BY UPPER(TRIM(full_name))
                    )
                    SELECT
                        p5.portfolio                                       AS portfolio,
                        p5.security_full_name                              AS security_full_name,
                        NULL                                               AS security_short_name,
                        {clean_isin('p5.isin_code')}                       AS isin,
                        {clean_ticker('p5.ticker_code')}                   AS ticker,
                        {safe_decimal('p5.quantity',                         'DECIMAL(30,8)')} AS quantity,
                        {safe_decimal('p5.no_of_shares_issues_by_the_company','DECIMAL(30,8)')} AS shares_outstanding,
                        {safe_decimal('p5.no_of_shares_issues_by_the_company','DECIMAL(30,8)')} AS shares_issued,
                        {safe_decimal('p5.pct_holdings',                     'DECIMAL(10,6)')} AS pct_holding,
                        {safe_decimal('p5.market_price_unit_fc',             'DECIMAL(30,8)')} AS market_price,
                        {safe_decimal('p5.unit_avg_cost_unit_fc',            'DECIMAL(30,8)')} AS average_cost,
                        {safe_decimal('p5.cost_fc',                          'DECIMAL(30,8)')} AS cost_fc,
                        {safe_decimal('p5.market_value_fc',                  'DECIMAL(30,8)')} AS market_value_fc,
                        {safe_decimal('p5.net_book_value_fc',                'DECIMAL(30,8)')} AS net_book_value_fc,
                        {safe_decimal('p5.unrealised_gain_loss_fc',          'DECIMAL(30,8)')} AS unrealized_pnl_fc,
                        {safe_decimal('p5.provision_fc',                     'DECIMAL(30,8)')} AS provision_fc,
                        {safe_decimal('p5.cost_lc',                          'DECIMAL(30,8)')} AS cost_lc,
                        {safe_decimal('p5.market_value_lc',                  'DECIMAL(30,8)')} AS market_value_lc,
                        {safe_decimal('p5.net_book_value_lc',                'DECIMAL(30,8)')} AS net_book_value_lc,
                        {safe_decimal('p5.unrealised_gain_loss_lc',          'DECIMAL(30,8)')} AS unrealized_pnl_lc,
                        {safe_decimal('p5.provision_lc',                     'DECIMAL(30,8)')} AS provision_lc,
                        p5.product_type, p5.security_type, p5.quoted_unquoted, p5.industry,
                        NULL                                               AS fin_nonfin_co,
                        p5.issuer_type, p5.reits_or_fund_y_n,
                        cn_exc.label                                       AS exchange,
                        NULL                                               AS country_code,
                        cn_exc.label                                       AS country_of_exchange,
                        cn_inc.label                                       AS country_of_incorporation,
                        cn_rsk.label                                       AS country_of_risk,
                        cn_opr.label                                       AS country_of_operation,
                        p5.security_currency_fc                            AS security_currency,
                        p5.corp_code, p5.branch_code, p5.cost_centre,
                        p5.cels_code                                       AS cels,
                        p5.bwcif_number_sg                                 AS bwcif_sg,
                        p5.bwcif_number_overseas                           AS bwcif_ovs,
                        p5.mas_6d_code_sg,
                        p5.mas_6d_code_overseas                            AS mas_6d_code_ovs,
                        'SETTLED'                                      AS position_basis,
                        p5.reporting_date, p5.maturity_date,
                        'USER_UPLOAD'                                      AS src_system,
                        'user'                                             AS sub_system,
                        'sta'                                              AS data_cat,
                        'adhoc'                                            AS data_frq,
                        'cis_user_sta_adhoc_position_5'                    AS source_table,
                        CURRENT_TIMESTAMP()                                AS etl_insert_ts,
                        'python_etl'                                       AS etl_batch_id,
                        CAST(NULL AS DECIMAL(30,8))                        AS dividend_fc
                    FROM {db}.cis_user_sta_adhoc_position_5 p5
                    LEFT JOIN country_lut cn_exc
                        ON cn_exc.full_name = UPPER(TRIM(CAST(p5.country_of_exchange AS STRING)))
                    LEFT JOIN country_lut cn_inc
                        ON cn_inc.full_name = UPPER(TRIM(CAST(p5.country_of_incorporation AS STRING)))
                    LEFT JOIN country_lut cn_rsk
                        ON cn_rsk.full_name = UPPER(TRIM(CAST(p5.country_of_risk AS STRING)))
                    LEFT JOIN country_lut cn_opr
                        ON cn_opr.full_name = UPPER(TRIM(CAST(p5.country_of_operation AS STRING)))
                    WHERE p5.processing_date = '{processing_date}'
                      AND p5.src_id = '{src_id}'
                """,
            }

            std_select = STANDARDIZE_SELECT.get(src_id)
            if not std_select:
                return False, f"Unknown src_id '{src_id}' — no standardization mapping defined", result

            # ── Hive external table guard helpers ─────────────────────────────
            # Hive external Parquet tables can have stale metastore entries after
            # partial/failed runs — the metastore believes a partition exists but
            # the parquet file is missing on HDFS, causing NoSuchFileException on
            # the very first read.  Always INVALIDATE + REFRESH + COUNT before
            # reading any Hive external table.
            #
            # Kudu tables (cis_security, cis_position, cis_exchange_mapping_lut,
            # cis_equity_price) are not affected — Kudu has no parquet files and
            # its metadata is always consistent.
            #
            # Hive external tables in this ETL:
            #   READ:  cis_user_sta_adhoc_position_{1-5}  (source, pre-Step 0)
            #          position_upload_standardized        (Step 1 source)
            #          gmp_cis_sta_dly_country             (Step 0 country map, format 5)
            #          gmp_cis_sta_dly_alldatesinfo        (Step 7A2 calendar)
            #   WRITE: position_upload_standardized        (Step 0 target → REFRESH after)
            #          position_upload_report              (Step 7B target → REFRESH after)

            def _hive_invalidate(table: str, step: str) -> None:
                """INVALIDATE METADATA for a Hive external table (full resync)."""
                logger.info(f"[position_etl] {step} INVALIDATE METADATA {db}.{table}")
                impala_manager.execute_write(f"INVALIDATE METADATA {db}.{table}", database=db)

            def _hive_refresh_partition(table: str, partition_clause: str, step: str) -> None:
                """REFRESH a specific partition of a Hive external table."""
                logger.info(f"[position_etl] {step} REFRESH {db}.{table} PARTITION ({partition_clause})")
                impala_manager.execute_write(
                    f"REFRESH {db}.{table} PARTITION ({partition_clause})", database=db
                )

            def _hive_refresh_table(table: str, step: str) -> None:
                """REFRESH a whole Hive external table (no partition key known)."""
                logger.info(f"[position_etl] {step} REFRESH {db}.{table}")
                impala_manager.execute_write(f"REFRESH {db}.{table}", database=db)

            def _hive_check_rows(table: str, where: str, step: str, abort_msg: str) -> int:
                """
                COUNT(*) a Hive partition after INVALIDATE+REFRESH.
                Returns the row count.  Raises RuntimeError with abort_msg if 0.
                """
                rows = impala_manager.execute_query(
                    f"SELECT COUNT(*) AS n FROM {db}.{table} WHERE {where}", database=db
                )
                n = int((rows or [{}])[0].get('n', 0))
                if n == 0:
                    raise RuntimeError(abort_msg)
                logger.info(f"[position_etl] {step} confirmed {n} rows in {db}.{table} WHERE {where}")
                return n

            # ── Pre-Step 0: resync source upload table ────────────────────────
            logger.info(f"[position_etl] PRE-STEP-0-A: INVALIDATE {src_id}")
            _hive_invalidate(src_id, "Pre-Step 0")

            logger.info(f"[position_etl] PRE-STEP-0-B: REFRESH PARTITION {src_id}")
            _hive_refresh_partition(
                src_id,
                f"processing_date='{processing_date}'",
                "Pre-Step 0"
            )

            logger.info(f"[position_etl] PRE-STEP-0-C: COUNT check {src_id}")
            try:
                _hive_check_rows(
                    src_id,
                    f"processing_date='{processing_date}'",
                    "Pre-Step 0",
                    f"Source partition {src_id}/processing_date={processing_date} has 0 rows "
                    f"after INVALIDATE+REFRESH — HDFS parquet file may be missing. "
                    f"Re-upload the file before running Position ETL."
                )
            except RuntimeError as _pre_err:
                return False, str(_pre_err), result
            logger.info(f"[position_etl] PRE-STEP-0-DONE: source partition confirmed")
            logger.info(f"[position_etl] SRC-ID-CHECK: src_id={repr(src_id)} match={src_id == 'cis_user_sta_adhoc_position_5'} std_select_len={len(std_select)}")

            # Formats 4 & 5: Impala query planner takes 150-300s even for small
            # row counts when the INSERT SELECT reads from a Hive external parquet
            # table. Solution for both: SELECT source rows into Python, build
            # VALUES literals, INSERT OVERWRITE ... VALUES — trivial plan, <1s.
            # Format 5 additionally resolves country columns via Python dict
            # (avoids a 3700-branch CASE WHEN / gmp_cis_sta_dly_country scan).
            if src_id == 'cis_user_sta_adhoc_position_4':
                try:
                    import time as _ct
                    _ct0 = _ct.time()
                    _t = _etl_t0

                    def _safe_str(v):
                        if v is None or str(v).strip() in ('', 'None', 'NULL', 'null'):
                            return 'NULL'
                        # Strip apostrophes and backslashes — Impala VALUES parser does
                        # not reliably handle '' escaping inside multi-row VALUES blocks.
                        s = str(v).replace("'", "").replace("\\", "")
                        return "'" + s + "'"

                    def _safe_dec(v, dec_type='DECIMAL(30,8)'):
                        """Return an explicit CAST(literal AS dec_type) SQL fragment.

                        Always wrapping in an explicit CAST (rather than a bare
                        numeric literal) lets Impala narrow precision freely --
                        a bare literal's inferred type is derived from its own
                        digit count, and if that exceeds the target column's
                        declared scale, Impala raises "Possible loss of
                        precision" / "incompatible type" on the implicit cast
                        (seen for pct_holding and market_price in SIT).
                        """
                        import re as _re
                        if v is None:
                            return f'CAST(NULL AS {dec_type})'
                        s = str(v).strip()
                        s = _re.sub(r'[,$£€¥%]', '', s)
                        s = _re.sub(r'^\(([0-9.]+)\)$', r'-\1', s)
                        s = _re.sub(r'^[-–—]+$', '0', s)
                        m = _re.match(r'^-?[0-9]+\.?[0-9]*([eE][+-]?[0-9]+)?$', s)
                        return f'CAST({s} AS {dec_type})' if m else f'CAST(NULL AS {dec_type})'

                    logger.info(f"[position_etl] F4-STEP0-A: reading source rows from {src_id}")
                    _src_rows = impala_manager.execute_query(
                        f"""
                        SELECT *
                        FROM {db}.{src_id}
                        WHERE processing_date = '{processing_date}'
                          AND src_id = '{src_id}'
                        """,
                        database=db
                    ) or []
                    logger.info(f"[position_etl] F4-STEP0-B: {len(_src_rows)} source rows fetched")

                    if not _src_rows:
                        return False, f"Step 0 (F4): no rows in {src_id} for processing_date={processing_date}", result

                    _val_rows = []
                    for _r in _src_rows:
                        _val_rows.append(f"""(
                            {_safe_str(_r.get('portfolio'))},
                            {_safe_str(_r.get('security_full_name'))},
                            NULL,
                            {_safe_str(_r.get('isin_code'))},
                            {_safe_str(_r.get('ticker_code'))},
                            {_safe_dec(_r.get('quantity'))},
                            {_safe_dec(_r.get('no_of_shares_issues_by_the_company'))},
                            {_safe_dec(_r.get('no_of_shares_issues_by_the_company'))},
                            {_safe_dec(_r.get('pct_holdings'), 'DECIMAL(10,6)')},
                            NULL,
                            NULL,
                            {_safe_dec(_r.get('cost_fc'))},
                            {_safe_dec(_r.get('market_value_fc'))},
                            NULL,
                            NULL,
                            NULL,
                            {_safe_dec(_r.get('cost_lc'))},
                            {_safe_dec(_r.get('market_value_lc'))},
                            NULL,
                            NULL,
                            NULL,
                            {_safe_str(_r.get('product_type'))},
                            {_safe_str(_r.get('security_type'))},
                            {_safe_str(_r.get('quoted_unquoted'))},
                            {_safe_str(_r.get('industry'))},
                            {_safe_str(_r.get('financial_non_financial_co'))},
                            NULL,
                            NULL,
                            {_safe_str(_r.get('country_of_exchange'))},
                            NULL,
                            {_safe_str(_r.get('country_of_exchange'))},
                            {_safe_str(_r.get('country_of_incorporation'))},
                            NULL,
                            NULL,
                            {_safe_str(_r.get('security_currency'))},
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            'SETTLED',
                            {_safe_str(_r.get('reporting_date'))},
                            NULL,
                            'USER_UPLOAD', 'user', 'sta', 'adhoc',
                            'cis_user_sta_adhoc_position_4',
                            now(), 'python_etl',
                            NULL
                        )""")

                    _values_sql = ',\n'.join(_val_rows)
                    logger.info(f"[position_etl] F4-STEP0-C: INSERT {len(_val_rows)} rows via VALUES")
                    ok = impala_manager.execute_write(
                        f"""
                        INSERT OVERWRITE {db}.position_upload_standardized
                        PARTITION (processing_date='{processing_date}', src_id='{src_id}')
                        VALUES {_values_sql}
                        """,
                        database=db
                    )
                    if not ok:
                        return False, "Step 0 (F4) INSERT VALUES into position_upload_standardized failed", result
                    impala_manager.execute_write(
                        f"REFRESH {db}.position_upload_standardized PARTITION (processing_date='{processing_date}', src_id='{src_id}')",
                        database=db
                    )
                    std_rows = len(_val_rows)
                    logger.info(f"[position_etl] F4-STEP0-DONE: {std_rows} rows in {_ct.time()-_ct0:.1f}s total")
                    _t = _step_time("Step 0 (F4 python-insert)", _t)
                    if std_rows == 0:
                        return False, "Step 0 (F4) produced 0 rows", result

                except Exception as _f4e:
                    logger.error(f"[position_etl] F4-STEP0-FAIL: {_f4e}", exc_info=True)
                    return False, f"Step 0 (F4) failed: {_f4e}", result

            elif src_id == 'cis_user_sta_adhoc_position_5':
                try:
                    import time as _ct
                    _ct0 = _ct.time()
                    _t = _etl_t0
                    logger.info(f"[position_etl] F5-STEP0-A: REFRESH gmp_cis_sta_dly_country")
                    _hive_refresh_table('gmp_cis_sta_dly_country', "Step 0 (country map)")

                    logger.info(f"[position_etl] F5-STEP0-B: fetching country map")
                    _country_map = _build_country_map_for_format5(
                        impala_manager, db, processing_date, src_id
                    )
                    logger.info(f"[position_etl] F5-STEP0-C: country map {len(_country_map)} keys in {_ct.time()-_ct0:.1f}s")

                    def _resolve_country(raw_val):
                        """Look up country label from raw upload value using the pre-built map.

                        Format 5 sometimes sends a country name with a trailing
                        qualifier -- e.g. "Jersey, Channel Islands", "Korea
                        (South), Republic of", "Taiwan (Republic of China)" --
                        that won't match the LUT's plain country name. Trim at
                        the first "(" or "," before lookup (SA feedback,
                        Venkata Narayana Adisetty, PORTIARP-6984 comment,
                        30/07/2026). _normalize_country_key() alone doesn't
                        solve this: it turns punctuation into spaces rather
                        than truncating, so "Jersey, Channel Islands" would
                        normalize to "JERSEY CHANNEL ISLANDS", not "JERSEY".
                        """
                        if not raw_val:
                            return None
                        import re as _re
                        key = str(raw_val).upper().strip()
                        key = _re.split(r'[(,]', key, 1)[0].strip()
                        return _country_map.get(key) or _country_map.get(_normalize_country_key(key))

                    def _safe_str(v):
                        """Return SQL string literal or NULL for None/empty values."""
                        if v is None or str(v).strip() in ('', 'None', 'NULL', 'null'):
                            return 'NULL'
                        # Strip apostrophes and backslashes — Impala VALUES parser does
                        # not reliably handle '' escaping inside multi-row VALUES blocks.
                        s = str(v).replace("'", "").replace("\\", "")
                        return "'" + s + "'"

                    def _safe_dec(v, dec_type='DECIMAL(30,8)'):
                        """Return an explicit CAST(literal AS dec_type) SQL fragment.

                        Always wrapping in an explicit CAST (rather than a bare
                        numeric literal) lets Impala narrow precision freely --
                        a bare literal's inferred type is derived from its own
                        digit count, and if that exceeds the target column's
                        declared scale, Impala raises "Possible loss of
                        precision" / "incompatible type" on the implicit cast
                        (seen for pct_holding and market_price in SIT).
                        """
                        import re as _re
                        if v is None:
                            return f'CAST(NULL AS {dec_type})'
                        s = str(v).strip()
                        s = _re.sub(r'[,$£€¥%]', '', s)
                        s = _re.sub(r'^\(([0-9.]+)\)$', r'-\1', s)
                        s = _re.sub(r'^[-–—]+$', '0', s)
                        m = _re.match(r'^-?[0-9]+\.?[0-9]*([eE][+-]?[0-9]+)?$', s)
                        return f'CAST({s} AS {dec_type})' if m else f'CAST(NULL AS {dec_type})'

                    logger.info(f"[position_etl] F5-STEP0-D: reading source rows from {src_id}")
                    _src_rows = impala_manager.execute_query(
                        f"""
                        SELECT *
                        FROM {db}.{src_id}
                        WHERE processing_date = '{processing_date}'
                          AND src_id = '{src_id}'
                        """,
                        database=db
                    ) or []
                    logger.info(f"[position_etl] F5-STEP0-E: {len(_src_rows)} source rows fetched")

                    if not _src_rows:
                        return False, f"Step 0 (F5): no rows in {src_id} for processing_date={processing_date}", result

                    # Build VALUES rows with country already resolved in Python
                    _val_rows = []
                    for _r in _src_rows:
                        _exc = _resolve_country(_r.get('country_of_exchange'))
                        _inc = _resolve_country(_r.get('country_of_incorporation'))
                        _rsk = _resolve_country(_r.get('country_of_risk'))
                        _opr = _resolve_country(_r.get('country_of_operation'))
                        _val_rows.append(f"""(
                            {_safe_str(_r.get('portfolio'))},
                            {_safe_str(_r.get('security_full_name'))},
                            NULL,
                            {_safe_str(_r.get('isin_code'))},
                            {_safe_str(_r.get('ticker_code'))},
                            {_safe_dec(_r.get('quantity'))},
                            {_safe_dec(_r.get('no_of_shares_issues_by_the_company'))},
                            {_safe_dec(_r.get('no_of_shares_issues_by_the_company'))},
                            {_safe_dec(_r.get('pct_holdings'), 'DECIMAL(10,6)')},
                            {_safe_dec(_r.get('market_price_unit_fc'))},
                            {_safe_dec(_r.get('unit_avg_cost_unit_fc'))},
                            {_safe_dec(_r.get('cost_fc'))},
                            {_safe_dec(_r.get('market_value_fc'))},
                            {_safe_dec(_r.get('net_book_value_fc'))},
                            {_safe_dec(_r.get('unrealised_gain_loss_fc'))},
                            {_safe_dec(_r.get('provision_fc'))},
                            {_safe_dec(_r.get('cost_lc'))},
                            {_safe_dec(_r.get('market_value_lc'))},
                            {_safe_dec(_r.get('net_book_value_lc'))},
                            {_safe_dec(_r.get('unrealised_gain_loss_lc'))},
                            {_safe_dec(_r.get('provision_lc'))},
                            {_safe_str(_r.get('product_type'))},
                            {_safe_str(_r.get('security_type'))},
                            {_safe_str(_r.get('quoted_unquoted'))},
                            {_safe_str(_r.get('industry'))},
                            NULL,
                            {_safe_str(_r.get('issuer_type'))},
                            {_safe_str(_r.get('reits_or_fund_y_n'))},
                            {_safe_str(_exc)},
                            NULL,
                            {_safe_str(_exc)},
                            {_safe_str(_inc)},
                            {_safe_str(_rsk)},
                            {_safe_str(_opr)},
                            {_safe_str(_r.get('security_currency_fc'))},
                            {_safe_str(_r.get('corp_code'))},
                            {_safe_str(_r.get('branch_code'))},
                            {_safe_str(_r.get('cost_centre'))},
                            {_safe_str(_r.get('cels_code'))},
                            {_safe_str(_r.get('bwcif_number_sg'))},
                            {_safe_str(_r.get('bwcif_number_overseas'))},
                            {_safe_str(_r.get('mas_6d_code_sg'))},
                            {_safe_str(_r.get('mas_6d_code_overseas'))},
                            'SETTLED',
                            {_safe_str(_r.get('reporting_date'))},
                            {_safe_str(_r.get('maturity_date'))},
                            'USER_UPLOAD', 'user', 'sta', 'adhoc',
                            'cis_user_sta_adhoc_position_5',
                            now(), 'python_etl',
                            NULL
                        )""")

                    _values_sql = ',\n'.join(_val_rows)
                    logger.info(f"[position_etl] F5-STEP0-F: INSERT {len(_val_rows)} rows via VALUES")
                    ok = impala_manager.execute_write(
                        f"""
                        INSERT OVERWRITE {db}.position_upload_standardized
                        PARTITION (processing_date='{processing_date}', src_id='{src_id}')
                        VALUES {_values_sql}
                        """,
                        database=db
                    )
                    if not ok:
                        return False, "Step 0 (F5) INSERT VALUES into position_upload_standardized failed", result
                    impala_manager.execute_write(
                        f"REFRESH {db}.position_upload_standardized PARTITION (processing_date='{processing_date}', src_id='{src_id}')",
                        database=db
                    )
                    std_rows = len(_val_rows)
                    logger.info(f"[position_etl] F5-STEP0-DONE: {std_rows} rows in {_ct.time()-_ct0:.1f}s total")
                    _t = _step_time("Step 0 (F5 python-resolve+insert)", _t)
                    if std_rows == 0:
                        return False, "Step 0 (F5) produced 0 rows", result

                except Exception as _f5e:
                    logger.error(f"[position_etl] F5-STEP0-FAIL: {_f5e}", exc_info=True)
                    return False, f"Step 0 (F5) failed: {_f5e}", result

            else:
                # ── All other formats: standard SQL INSERT OVERWRITE ──────────
                _t = _etl_t0
                logger.info(f"[position_etl] Step 0 starting — INSERT OVERWRITE position_upload_standardized for {src_id}")
                logger.info(f"[position_etl] Step 0 SQL length={len(std_select)} chars")
                ok = impala_manager.execute_write(
                    f"""
                    INSERT OVERWRITE {db}.position_upload_standardized
                    PARTITION (processing_date='{processing_date}', src_id='{src_id}')
                    {std_select}
                    """,
                    database=db
                )
                if not ok:
                    return False, f"Step 0 INSERT into position_upload_standardized failed — check Impala logs", result
                impala_manager.execute_write(
                    f"REFRESH {db}.position_upload_standardized PARTITION (processing_date='{processing_date}', src_id='{src_id}')",
                    database=db
                )
                std_count = impala_manager.execute_query(
                    f"""
                    SELECT COUNT(*) AS cnt
                    FROM {db}.position_upload_standardized
                    WHERE src_id = '{src_id}'
                      AND processing_date = '{processing_date}'
                    """,
                    database=db
                )
                std_rows = (std_count[0].get('cnt', 0) if std_count else 0)
                logger.info(f"[position_etl] Step 0 complete — {std_rows} rows standardized")
                _t = _step_time("Step 0 (standardize+refresh)", _t)
                if std_rows == 0:
                    return False, f"Standardization produced 0 rows — check src_id='{src_id}' processing_date='{processing_date}' in {src_id} table", result

            # ------------------------------------------------------------------
            # Step 1: Base staging table — read from position_upload_standardized
            #         (all STRING), apply DECIMAL casts, rename exchange_code →
            #         `exchange` for use as internal name throughout the pipeline.
            # ------------------------------------------------------------------
            # Guard: REFRESH the partition we just wrote in Step 0 so Impala
            # sees the new parquet file before Step 1 reads it.
            _hive_refresh_partition(
                'position_upload_standardized',
                f"processing_date='{processing_date}', src_id='{src_id}'",
                "Step 1 (pre-read)"
            )
            try:
                _hive_check_rows(
                    'position_upload_standardized',
                    f"src_id='{src_id}' AND processing_date='{processing_date}'",
                    "Step 1 (pre-read)",
                    f"position_upload_standardized partition src_id={src_id}/"
                    f"processing_date={processing_date} has 0 rows after REFRESH — "
                    f"Step 0 INSERT may have silently failed."
                )
            except RuntimeError as _s1_err:
                return False, str(_s1_err), result
            impala_manager.execute_write(
                f"DROP TABLE IF EXISTS pos_stage_1_base_{ETL_SFX}", database=db
            )
            ok = impala_manager.execute_write(
                f"""
                CREATE TABLE pos_stage_1_base_{ETL_SFX}
                STORED AS PARQUET AS
                SELECT
                    ROW_NUMBER() OVER (ORDER BY portfolio, security_full_name) AS row_id,
                    portfolio,
                    security_full_name,
                    security_short_name,
                    isin                                        AS isin,
                    ticker,
                    COALESCE(quantity,            CAST(0 AS DECIMAL(30,8))) AS quantity,
                    COALESCE(shares_outstanding,  CAST(0 AS DECIMAL(30,8))) AS shares_outstanding,
                    COALESCE(shares_issued,       CAST(0 AS DECIMAL(30,8))) AS shares_issued,
                    COALESCE(pct_holding,         CAST(0 AS DECIMAL(10,6))) AS pct_holding,
                    market_price,
                    average_cost,
                    COALESCE(cost_fc,             CAST(0 AS DECIMAL(30,8))) AS cost_fc,
                    COALESCE(market_value_fc,     CAST(0 AS DECIMAL(30,8))) AS market_value_fc,
                    COALESCE(net_book_value_fc,   CAST(0 AS DECIMAL(30,8))) AS net_book_value_fc,
                    COALESCE(unrealized_pnl_fc,   CAST(0 AS DECIMAL(30,8))) AS unrealized_pnl_fc,
                    COALESCE(cost_lc,             CAST(0 AS DECIMAL(30,8))) AS cost_lc,
                    COALESCE(market_value_lc,     CAST(0 AS DECIMAL(30,8))) AS market_value_lc,
                    COALESCE(net_book_value_lc,   CAST(0 AS DECIMAL(30,8))) AS net_book_value_lc,
                    COALESCE(unrealized_pnl_lc,   CAST(0 AS DECIMAL(30,8))) AS unrealized_pnl_lc,
                    COALESCE(provision_lc,        CAST(0 AS DECIMAL(30,8))) AS provision_lc,
                    COALESCE(provision_fc,        CAST(0 AS DECIMAL(30,8))) AS provision_fc,
                    product_type,
                    security_type,
                    -- Normalize to exactly 'QUOTED' / 'UNQUOTED' regardless of
                    -- how the raw upload sent it ('Q', 'Quoted', 'QUOTED',
                    -- 'UNQUOTED', 'U', 'Unquoted', ...). Unrecognized/blank
                    -- values stay NULL rather than guessing.
                    CASE
                        WHEN UPPER(TRIM(quoted_unquoted)) LIKE 'UNQ%'
                          OR UPPER(TRIM(quoted_unquoted)) = 'U' THEN 'UNQUOTED'
                        WHEN UPPER(TRIM(quoted_unquoted)) LIKE 'Q%' THEN 'QUOTED'
                        ELSE NULL
                    END AS quoted_unquoted,
                    industry,
                    fin_nonfin_co,
                    issuer_type,
                    reits_or_fund_y_n,
                    `exchange`                                  AS `exchange`,
                    country_code,
                    country_of_exchange,
                    country_of_incorporation,
                    country_of_risk,
                    country_of_operation,
                    security_currency,
                    corp_code,
                    branch_code,
                    cost_centre,
                    cels,
                    bwcif_sg,
                    bwcif_ovs,
                    mas_6d_code_sg,
                    mas_6d_code_ovs,
                    position_basis,
                    from_timestamp(
                        -- Broad, defensive date-format normalizer -- this is the ONE
                        -- centralized place all 5 upload formats' reporting_date values
                        -- pass through (Step 0 standardizes each format's raw column into
                        -- position_upload_standardized as STRING; this Step 1 CTAS is what
                        -- actually parses it), so adding a format here covers every
                        -- format/source uniformly rather than needing 5 separate edits.
                        -- Ordered most-specific-pattern-first; each branch is a no-op
                        -- (returns NULL, doesn't error) if reporting_date doesn't actually
                        -- match that format, so branches are safe to stack.
                        CASE
                            WHEN reporting_date LIKE '__/__/____' THEN
                                CAST(unix_timestamp(reporting_date, 'dd/MM/yyyy') AS TIMESTAMP)
                            WHEN reporting_date LIKE '__-__-____' THEN
                                CAST(unix_timestamp(reporting_date, 'dd-MM-yyyy') AS TIMESTAMP)
                            WHEN reporting_date LIKE '__.__.____' THEN
                                CAST(unix_timestamp(reporting_date, 'dd.MM.yyyy') AS TIMESTAMP)
                            WHEN reporting_date LIKE '____-__-__' AND length(reporting_date) = 10 THEN
                                CAST(unix_timestamp(reporting_date, 'yyyy-MM-dd') AS TIMESTAMP)
                            WHEN reporting_date LIKE '____/__/__' THEN
                                CAST(unix_timestamp(reporting_date, 'yyyy/MM/dd') AS TIMESTAMP)
                            WHEN reporting_date LIKE '____.__.__' THEN
                                CAST(unix_timestamp(reporting_date, 'yyyy.MM.dd') AS TIMESTAMP)
                            WHEN length(reporting_date) = 8 AND reporting_date RLIKE '^[0-9]{8}$' THEN
                                CAST(unix_timestamp(reporting_date, 'yyyyMMdd') AS TIMESTAMP)
                            WHEN reporting_date LIKE '%-%-%T%:%:%' OR reporting_date LIKE '%-%-% %:%:%' THEN
                                CAST(regexp_replace(reporting_date, '[T ].*', '') AS TIMESTAMP)
                            WHEN reporting_date RLIKE '^[0-9]{1,2} [A-Za-z]{3} [0-9]{4}$' THEN
                                CAST(unix_timestamp(reporting_date, 'dd MMM yyyy') AS TIMESTAMP)
                            WHEN reporting_date RLIKE '^[A-Za-z]{3} [0-9]{1,2},? [0-9]{4}$' THEN
                                CAST(unix_timestamp(regexp_replace(reporting_date, ',', ''), 'MMM dd yyyy') AS TIMESTAMP)
                            ELSE
                                -- Last resort: Impala's own implicit STRING->TIMESTAMP cast
                                -- (handles ISO-ish formats not caught above). Returns NULL
                                -- rather than erroring if it still doesn't match anything.
                                CAST(reporting_date AS TIMESTAMP)
                        END,
                        'yyyy-MM-dd'
                    ) AS reporting_date,
                    maturity_date,
                    src_system,
                    sub_system,
                    data_cat,
                    data_frq,
                    source_table,
                    etl_insert_ts,
                    etl_batch_id,
                    src_id,
                    processing_date
                FROM {db}.position_upload_standardized
                WHERE src_id = '{src_id}'
                  AND processing_date = '{processing_date}'
                """,
                database=db
            )
            if not ok:
                return False, f"Step 1 CREATE TABLE pos_stage_1_base_{ETL_SFX} failed — check Impala logs (likely column mismatch in position_upload_standardized)", result
            _s1_rows = _count(f'pos_stage_1_base_{ETL_SFX}')
            logger.info(f"[position_etl] Step 1 complete — {_s1_rows} rows in pos_stage_1_base")
            _t = _step_time("Step 1 (base staging)", _t)

            # ------------------------------------------------------------------
            # Step 1B: universal country full-name -> code resolution (ALL
            # formats). Previously only Format 5 resolved a country full name
            # (e.g. "Taiwan (Republic of China)") to its code via the
            # gmp_cis_sta_dly_country LUT -- formats 1-4 passed
            # country_of_exchange/incorporation/risk/operation straight
            # through. Applied here uniformly, after Step 1, so it works
            # regardless of format. Safe for formats whose raw value is
            # already a proper code: the LUT lookup simply finds no match and
            # the original value passes through unchanged.
            #
            # Uses the same trim-at-"("-or-","-then-lookup approach as
            # Format 5's _resolve_country(), against a small per-row Python
            # dict lookup (NOT a SQL CASE WHEN across the ~250-row LUT --
            # already found to be too slow for Impala's planner, see
            # _build_country_map_for_format5's docstring). Only rewrites
            # pos_stage_1_base when at least one row's value actually changed,
            # and the CASE WHEN only spans the affected rows, not the LUT.
            # ------------------------------------------------------------------
            _univ_country_map = _build_country_map_for_format5(
                impala_manager, db, processing_date, src_id
            )
            # Placeholder values meaning "not supplied" -- same list clean_isin()
            # already uses for the ISIN field. Country columns had no equivalent
            # cleanup: a literal "NA" (seen verbatim in Format 4's raw
            # country_of_exchange, alongside isin_code/ticker_code also "NA" on
            # the same row) is neither NULL nor '', so it read as "a country WAS
            # supplied" to the tier-matching gates -- failing Tier 4 (doesn't
            # match any real cis_security value) AND skipping the Tier 6-9
            # country-blank fallback (gate requires genuinely blank), even
            # though the security matched exactly on full name with a blank
            # country_of_exchange on the cis_security side. Always runs
            # (unlike the LUT lookup below) since blanking a placeholder
            # doesn't depend on the country-name LUT being populated.
            _COUNTRY_PLACEHOLDERS = {'NA', 'N/A', 'NIL', 'NONE', '-', 'N.A.', 'NAP'}
            if True:
                def _resolve_country_universal(raw_val):
                    if not raw_val:
                        return None
                    import re as _re_ucr
                    key = str(raw_val).upper().strip()
                    key = _re_ucr.split(r'[(,]', key, 1)[0].strip()
                    if key in _COUNTRY_PLACEHOLDERS:
                        return ''  # blank out the placeholder
                    if not _univ_country_map:
                        return None
                    return _univ_country_map.get(key) or _univ_country_map.get(_normalize_country_key(key))

                _country_cols = ('country_of_exchange', 'country_of_incorporation',
                                  'country_of_risk', 'country_of_operation')
                _s1b_rows = impala_manager.execute_query(
                    f"SELECT row_id, {', '.join(_country_cols)} FROM pos_stage_1_base_{ETL_SFX}",
                    database=db
                ) or []

                _overrides: dict = {}  # row_id -> {col: resolved_value}
                for _r in _s1b_rows:
                    _row_over = {}
                    for _col in _country_cols:
                        _raw = _r.get(_col)
                        _resolved = _resolve_country_universal(_raw)
                        # _resolved may legitimately be '' (blanking a
                        # placeholder) -- only None means "no change".
                        if _resolved is not None and _resolved != (_raw or ''):
                            _row_over[_col] = _resolved
                    if _row_over:
                        _overrides[int(_r['row_id'])] = _row_over

                if _overrides:
                    def _sql_str_1b(v):
                        if v in (None, ''):
                            return 'NULL'
                        s = str(v).replace('\\', '\\\\').replace("'", "\\'")
                        return "'" + s + "'"

                    def _case_for(col: str) -> str:
                        _branches = ' '.join(
                            f"WHEN row_id = {rid} THEN {_sql_str_1b(vals[col])}"
                            for rid, vals in _overrides.items() if col in vals
                        )
                        if not _branches:
                            return col
                        return f"CASE {_branches} ELSE {col} END AS {col}"

                    impala_manager.execute_write(
                        f"DROP TABLE IF EXISTS pos_stage_1b_country_{ETL_SFX}", database=db
                    )
                    impala_manager.execute_write(
                        f"""
                        CREATE TABLE pos_stage_1b_country_{ETL_SFX}
                        STORED AS PARQUET AS
                        SELECT
                            row_id, portfolio, security_full_name, security_short_name,
                            isin, ticker, quantity, shares_outstanding, shares_issued,
                            pct_holding, market_price, average_cost, cost_fc,
                            market_value_fc, net_book_value_fc, unrealized_pnl_fc,
                            cost_lc, market_value_lc, net_book_value_lc,
                            unrealized_pnl_lc, provision_lc, provision_fc, product_type,
                            security_type, quoted_unquoted, industry, fin_nonfin_co,
                            issuer_type, reits_or_fund_y_n, `exchange`, country_code,
                            {_case_for('country_of_exchange')},
                            {_case_for('country_of_incorporation')},
                            {_case_for('country_of_risk')},
                            {_case_for('country_of_operation')},
                            security_currency, corp_code, branch_code, cost_centre,
                            cels, bwcif_sg, bwcif_ovs, mas_6d_code_sg, mas_6d_code_ovs,
                            position_basis, reporting_date, maturity_date, src_system,
                            sub_system, data_cat, data_frq, source_table, etl_insert_ts,
                            etl_batch_id, src_id, processing_date
                        FROM pos_stage_1_base_{ETL_SFX}
                        """,
                        database=db
                    )
                    impala_manager.execute_write(
                        f"DROP TABLE IF EXISTS pos_stage_1_base_{ETL_SFX}", database=db
                    )
                    impala_manager.execute_write(
                        f"CREATE TABLE pos_stage_1_base_{ETL_SFX} STORED AS PARQUET AS "
                        f"SELECT * FROM pos_stage_1b_country_{ETL_SFX}",
                        database=db
                    )
                    impala_manager.execute_write(
                        f"DROP TABLE IF EXISTS pos_stage_1b_country_{ETL_SFX}", database=db
                    )
                    logger.info(
                        f"[position_etl] Step 1B: resolved country full-name -> code "
                        f"for {len(_overrides)} row(s)"
                    )
            _t = _step_time("Step 1B (universal country resolution)", _t)

            # ------------------------------------------------------------------
            # Step 2: Portfolio validation — join on pf.name (exact match)
            # ------------------------------------------------------------------
            impala_manager.execute_write(
                f"DROP TABLE IF EXISTS pos_stage_2_portfolio_{ETL_SFX}", database=db
            )
            impala_manager.execute_write(
                f"""
                CREATE TABLE pos_stage_2_portfolio_{ETL_SFX}
                STORED AS PARQUET AS
                SELECT
                    b.row_id,
                    b.portfolio,
                    pf.name AS valid_portfolio,
                    pf.currency AS portfolio_currency,
                    CASE
                        WHEN pf.name IS NOT NULL THEN 'PASS'
                        ELSE 'FAIL: Portfolio not found in cis_portfolio'
                    END AS portfolio_status
                FROM pos_stage_1_base_{ETL_SFX} b
                LEFT JOIN {db}.cis_portfolio pf ON b.portfolio = pf.name
                """,
                database=db
            )
            _s2_bd = _breakdown(f'pos_stage_2_portfolio_{ETL_SFX}', 'portfolio_status')
            _s2_fail = _count(f'pos_stage_2_portfolio_{ETL_SFX}', "portfolio_status LIKE 'FAIL%'")
            logger.info(f"[position_etl] Step 2 complete — portfolio validation: {_s2_bd}")
            if _s2_fail > 0:
                try:
                    _pf_rows = impala_manager.execute_query(
                        f"SELECT DISTINCT portfolio FROM pos_stage_2_portfolio_{ETL_SFX} "
                        "WHERE portfolio_status LIKE 'FAIL%' LIMIT 10",
                        database=db
                    )
                    _bad_pf = [r.get('portfolio') for r in (_pf_rows or [])]
                    logger.warning(f"[position_etl] Step 2: {_s2_fail} row(s) failed portfolio — unknown portfolios: {_bad_pf}")
                except Exception:
                    pass
            _t = _step_time("Step 2 (portfolio validation)", _t)

            # ------------------------------------------------------------------
            # Step 3+4: Security matching — 10-tier cascade per SA requirement
            # (Venkata Narayana Adisetty, 30/07/2026 — "Change Position ETL
            # security matching logic"):
            #   1. Short Name           -> cis_security.security_name
            #   2. ISIN + Country of Exchange
            #   3. Ticker (trailing "EQUITY" stripped) + Country of Exchange
            #   4. Full Name            -> cis_security.security_description + Country of Exchange
            #   5. Normalized Full Name + Country of Exchange
            #   6. ISIN only
            #   7. Ticker only
            #   8. Full Name only       -> cis_security.security_description
            #   9. Normalized Full Name only
            #   10. Create Security
            #
            # Rules: a tier is only evaluated when its required fields are
            # populated; matching is case-insensitive and trimmed; per tier
            # 0 matches -> next tier, 1 match -> stop, >1 matches -> FAIL:
            # MULTIPLE_MATCH_<TIER> and stop (no security is created for that
            # row). security_match_method records which tier resolved the
            # match ('NONE' if a new security had to be created). CIS
            # security_name is treated as Short Name; security_description is
            # treated as Full Name, per the SA's explicit mapping.
            #
            # Tiers 1-4 and 6-8 are pure SQL (Stage A / Stage C). Tiers 5 and 9
            # ("Normalized Full Name") reuse abbreviate_security_name() — a
            # Python-side normalizer — so they run as Python passes (Stage B /
            # Stage D) between the SQL stages, each only touching rows still
            # 'PENDING' after the higher-priority tiers.
            # ------------------------------------------------------------------

            def _build_normalized_cache(force: bool = False) -> dict:
                """Build/return the cache of normalized(security_name or
                security_description) -> list of candidate security dicts,
                used by tiers 5 and 9. A list (not a single winner) so a
                collision can be reported as MULTIPLE_MATCH instead of
                silently keeping the first candidate found.
                """
                import time as _time_cache
                _now_ts = _time_cache.time()
                if (not force and UploadService._cis_abbrev_cache and
                        _now_ts - UploadService._cis_abbrev_cache_ts <= UploadService._CIS_ABBREV_CACHE_TTL):
                    return UploadService._cis_abbrev_cache
                _cis_rows = impala_manager.execute_query(
                    f"""
                    SELECT security_id, security_name, security_description,
                           isin, exchange_code, country_of_exchange, currency_code
                    FROM {db}.cis_security WHERE is_active = true
                    """,
                    database=db
                ) or []
                _cache: dict = {}
                for _cs in _cis_rows:
                    _cand = {
                        'security_id':         int(_cs.get('security_id')),
                        'security_name':       _cs.get('security_name'),
                        'isin':                _cs.get('isin'),
                        'exchange_code':       _cs.get('exchange_code'),
                        'country_of_exchange': _cs.get('country_of_exchange'),
                        'currency_code':       _cs.get('currency_code'),
                    }
                    for _raw in (_cs.get('security_name'), _cs.get('security_description')):
                        _key = abbreviate_security_name(_raw or '')
                        if not _key:
                            continue
                        _bucket = _cache.setdefault(_key, [])
                        if not any(c['security_id'] == _cand['security_id'] for c in _bucket):
                            _bucket.append(_cand)
                UploadService._cis_abbrev_cache = _cache
                UploadService._cis_abbrev_cache_ts = _now_ts
                logger.info(f"[position_etl] Rebuilt normalized-name match cache ({len(_cache)} keys)")
                return _cache

            def _apply_python_tier_result(matches: dict, multi_ids: set, tier_name: str, status_suffix: str = '_MATCH') -> None:
                """Recreate pos_stage_4_security_fallback_<suffix>, applying Python-computed
                tier results (matches / multi-match fails) to rows currently
                'PENDING'; every other row's existing result passes through
                unchanged. Impala Parquet tables are immutable — recreate in
                place, matching this ETL's established pattern.
                """
                _match_ids = ', '.join(str(rid) for rid in matches.keys()) or '-1'
                _multi_ids_sql = ', '.join(str(rid) for rid in multi_ids) or '-1'
                _match_when = ' '.join(
                    f"WHEN row_id = {rid} THEN {c['security_id']}"
                    for rid, c in matches.items()
                ) or "WHEN 1 = 0 THEN NULL"
                impala_manager.execute_write(
                    f"DROP TABLE IF EXISTS pos_stage_4_tier_update_{ETL_SFX}", database=db
                )
                impala_manager.execute_write(
                    f"""
                    CREATE TABLE pos_stage_4_tier_update_{ETL_SFX}
                    STORED AS PARQUET AS
                    SELECT
                        p.row_id, p.upload_isin, p.security_full_name, p.security_short_name,
                        p.desc_prefix, p.upload_exchange, p.portfolio_status, p.resolved_country,
                        p.clean_ticker,
                        p.final_security_id   AS prev_security_id,
                        p.final_security_name AS prev_security_name,
                        p.final_isin          AS prev_isin,
                        p.final_exchange      AS prev_exchange,
                        p.final_country       AS prev_country,
                        p.final_currency      AS prev_currency,
                        p.security_match_method AS prev_method,
                        p.security_status        AS prev_status,
                        CASE
                            WHEN p.row_id IN ({_match_ids}) THEN CASE {_match_when} ELSE NULL END
                            ELSE NULL
                        END AS matched_security_id,
                        CASE
                            WHEN p.row_id IN ({_match_ids}) THEN 'MATCHED'
                            WHEN p.row_id IN ({_multi_ids_sql}) THEN 'MULTI'
                            ELSE 'UNCHANGED'
                        END AS tier_outcome
                    FROM pos_stage_4_security_fallback_{ETL_SFX} p
                    """,
                    database=db
                )
                impala_manager.execute_write(
                    f"DROP TABLE IF EXISTS pos_stage_4_security_fallback_{ETL_SFX}", database=db
                )
                impala_manager.execute_write(
                    f"""
                    CREATE TABLE pos_stage_4_security_fallback_{ETL_SFX}
                    STORED AS PARQUET AS
                    SELECT
                        u.row_id, u.upload_isin, u.security_full_name, u.security_short_name,
                        u.desc_prefix, u.upload_exchange, u.portfolio_status, u.resolved_country,
                        u.clean_ticker,
                        CASE WHEN u.tier_outcome = 'MATCHED' THEN u.matched_security_id ELSE u.prev_security_id END AS final_security_id,
                        CASE WHEN u.tier_outcome = 'MATCHED' THEN sn.security_name ELSE u.prev_security_name END AS final_security_name,
                        CASE WHEN u.tier_outcome = 'MATCHED' THEN sn.isin ELSE u.prev_isin END AS final_isin,
                        CASE WHEN u.tier_outcome = 'MATCHED' THEN sn.exchange_code ELSE u.prev_exchange END AS final_exchange,
                        CASE WHEN u.tier_outcome = 'MATCHED' THEN sn.country_of_exchange ELSE u.prev_country END AS final_country,
                        CASE WHEN u.tier_outcome = 'MATCHED' THEN sn.currency_code ELSE u.prev_currency END AS final_currency,
                        CASE
                            WHEN u.tier_outcome = 'MATCHED' THEN '{tier_name}'
                            WHEN u.tier_outcome = 'MULTI'   THEN 'FAIL: MULTIPLE_MATCH_{tier_name}'
                            ELSE u.prev_method
                        END AS security_match_method,
                        CASE
                            WHEN u.tier_outcome = 'MATCHED' THEN '{tier_name}{status_suffix}'
                            WHEN u.tier_outcome = 'MULTI'   THEN 'FAIL: MULTIPLE_MATCH_{tier_name}'
                            ELSE u.prev_status
                        END AS security_status
                    FROM pos_stage_4_tier_update_{ETL_SFX} u
                    LEFT JOIN {db}.cis_security sn
                        ON u.tier_outcome = 'MATCHED' AND sn.security_id = u.matched_security_id
                    """,
                    database=db
                )
                impala_manager.execute_write(
                    f"DROP TABLE IF EXISTS pos_stage_4_tier_update_{ETL_SFX}", database=db
                )

            # ---- Stage A (SQL): Tier 1 Short Name, Tier 2 ISIN+Country,
            #      Tier 3 Ticker+Country, Tier 4 Full Name(description)+Country ----
            impala_manager.execute_write(
                f"DROP TABLE IF EXISTS pos_stage_3_security_{ETL_SFX}", database=db
            )
            impala_manager.execute_write(
                f"DROP TABLE IF EXISTS pos_stage_4_security_fallback_{ETL_SFX}", database=db
            )
            impala_manager.execute_write(
                f"""
                CREATE TABLE pos_stage_4_security_fallback_{ETL_SFX}
                STORED AS PARQUET AS
                WITH
                -- Resolve exchange_quoted → country at query time via LUT.
                -- Dedup to one row per exchange_name (prefer country that exists
                -- as exchange_code in cis_security, else MIN).
                lut_dedup AS (
                    SELECT
                        lut.exchange_name,
                        COALESCE(
                            MIN(CASE WHEN sec.exchange_code IS NOT NULL THEN lut.country_name END),
                            MIN(lut.country_name)
                        ) AS country_name
                    FROM (
                        SELECT UPPER(TRIM(exchange_name)) AS exchange_name, country_name
                        FROM {db}.cis_exchange_mapping_lut
                    ) lut
                    LEFT JOIN (
                        SELECT DISTINCT UPPER(TRIM(exchange_code)) AS exchange_code
                        FROM {db}.cis_security WHERE is_active = true
                    ) sec ON lut.country_name = sec.exchange_code
                    GROUP BY lut.exchange_name
                ),
                base AS (
                    SELECT
                        b.row_id,
                        b.isin                  AS upload_isin,
                        b.security_full_name,
                        b.security_short_name,
                        b.`exchange`            AS upload_exchange,
                        p2.portfolio_status,
                        COALESCE(b.country_of_exchange, lut.country_name) AS resolved_country,
                        NULLIF(regexp_replace(UPPER(TRIM(CAST(b.ticker AS STRING))), '\\\\s+EQUITY$', ''), '') AS clean_ticker,
                        TRIM(
                            CASE
                                WHEN UPPER(b.security_full_name) LIKE '%COMMON STOCK%'
                                  OR UPPER(b.security_full_name) LIKE '%COMMON STICK%'
                                THEN regexp_replace(
                                        b.security_full_name,
                                        '(?i)\\\\s*COMMON\\\\s+(STOCK|STICK).*$',
                                        ''
                                     )
                                ELSE b.security_full_name
                            END
                        ) AS desc_prefix
                    FROM pos_stage_1_base_{ETL_SFX} b
                    JOIN pos_stage_2_portfolio_{ETL_SFX} p2
                        ON b.row_id = p2.row_id AND p2.portfolio_status = 'PASS'
                    LEFT JOIN lut_dedup lut
                        ON UPPER(TRIM(b.`exchange`)) = lut.exchange_name
                ),
                -- Tier 1: Short Name -> cis_security.security_name (no country requirement)
                t1 AS (
                    SELECT base.row_id, sn.security_id, sn.security_name, sn.isin,
                           sn.exchange_code, sn.country_of_exchange, sn.currency_code,
                           ROW_NUMBER() OVER (PARTITION BY base.row_id ORDER BY sn.security_id) AS rn,
                           COUNT(*) OVER (PARTITION BY base.row_id) AS cnt
                    FROM base
                    JOIN {db}.cis_security sn
                        ON  sn.is_active = true
                        AND base.security_short_name IS NOT NULL AND TRIM(base.security_short_name) != ''
                        AND UPPER(TRIM(sn.security_name)) = UPPER(TRIM(base.security_short_name))
                ),
                -- Tier 2: ISIN + Country of Exchange
                t2 AS (
                    SELECT base.row_id, sn.security_id, sn.security_name, sn.isin,
                           sn.exchange_code, sn.country_of_exchange, sn.currency_code,
                           ROW_NUMBER() OVER (PARTITION BY base.row_id ORDER BY sn.security_id) AS rn,
                           COUNT(*) OVER (PARTITION BY base.row_id) AS cnt
                    FROM base
                    JOIN {db}.cis_security sn
                        ON  sn.is_active = true
                        AND base.upload_isin IS NOT NULL AND TRIM(base.upload_isin) != ''
                        AND UPPER(TRIM(base.upload_isin)) NOT IN ('NA', 'N/A', 'NIL', 'NONE', '-', 'N.A.', 'NAP')
                        AND UPPER(TRIM(CAST(base.upload_isin AS STRING))) = UPPER(TRIM(CAST(sn.isin AS STRING)))
                        AND base.resolved_country IS NOT NULL AND TRIM(base.resolved_country) != ''
                        AND (
                            UPPER(TRIM(base.resolved_country)) = UPPER(TRIM(COALESCE(sn.exchange_code, '')))
                            OR UPPER(TRIM(base.resolved_country)) = UPPER(TRIM(COALESCE(sn.country_of_exchange, '')))
                        )
                ),
                -- Tier 3: Ticker (trailing "EQUITY" stripped) + Country of Exchange
                t3 AS (
                    SELECT base.row_id, sn.security_id, sn.security_name, sn.isin,
                           sn.exchange_code, sn.country_of_exchange, sn.currency_code,
                           ROW_NUMBER() OVER (PARTITION BY base.row_id ORDER BY sn.security_id) AS rn,
                           COUNT(*) OVER (PARTITION BY base.row_id) AS cnt
                    FROM base
                    JOIN {db}.cis_security sn
                        ON  sn.is_active = true
                        AND base.clean_ticker IS NOT NULL AND TRIM(base.clean_ticker) != ''
                        AND regexp_replace(UPPER(TRIM(CAST(sn.ticker AS STRING))), '\\\\s+EQUITY$', '') = base.clean_ticker
                        AND base.resolved_country IS NOT NULL AND TRIM(base.resolved_country) != ''
                        AND (
                            UPPER(TRIM(base.resolved_country)) = UPPER(TRIM(COALESCE(sn.exchange_code, '')))
                            OR UPPER(TRIM(base.resolved_country)) = UPPER(TRIM(COALESCE(sn.country_of_exchange, '')))
                        )
                ),
                -- Tier 4: Full Name -> cis_security.security_description + Country of Exchange
                t4 AS (
                    SELECT base.row_id, sn.security_id, sn.security_name, sn.isin,
                           sn.exchange_code, sn.country_of_exchange, sn.currency_code,
                           ROW_NUMBER() OVER (PARTITION BY base.row_id ORDER BY sn.security_id) AS rn,
                           COUNT(*) OVER (PARTITION BY base.row_id) AS cnt
                    FROM base
                    JOIN {db}.cis_security sn
                        ON  sn.is_active = true
                        AND base.desc_prefix IS NOT NULL AND TRIM(base.desc_prefix) != ''
                        AND sn.security_description IS NOT NULL
                        AND UPPER(TRIM(sn.security_description)) = UPPER(TRIM(base.desc_prefix))
                        AND base.resolved_country IS NOT NULL AND TRIM(base.resolved_country) != ''
                        AND (
                            UPPER(TRIM(base.resolved_country)) = UPPER(TRIM(COALESCE(sn.exchange_code, '')))
                            OR UPPER(TRIM(base.resolved_country)) = UPPER(TRIM(COALESCE(sn.country_of_exchange, '')))
                        )
                )
                SELECT
                    base.row_id,
                    base.upload_isin,
                    base.security_full_name,
                    base.security_short_name,
                    base.desc_prefix,
                    base.upload_exchange,
                    base.portfolio_status,
                    base.resolved_country,
                    base.clean_ticker,
                    COALESCE(
                        CASE WHEN COALESCE(t1.cnt, 0) = 1 THEN t1.security_id END,
                        CASE WHEN COALESCE(t1.cnt, 0) = 0 AND COALESCE(t2.cnt, 0) = 1 THEN t2.security_id END,
                        CASE WHEN COALESCE(t1.cnt, 0) = 0 AND COALESCE(t2.cnt, 0) = 0 AND COALESCE(t3.cnt, 0) = 1 THEN t3.security_id END,
                        CASE WHEN COALESCE(t1.cnt, 0) = 0 AND COALESCE(t2.cnt, 0) = 0 AND COALESCE(t3.cnt, 0) = 0 AND COALESCE(t4.cnt, 0) = 1 THEN t4.security_id END
                    ) AS final_security_id,
                    COALESCE(
                        CASE WHEN COALESCE(t1.cnt, 0) = 1 THEN t1.security_name END,
                        CASE WHEN COALESCE(t1.cnt, 0) = 0 AND COALESCE(t2.cnt, 0) = 1 THEN t2.security_name END,
                        CASE WHEN COALESCE(t1.cnt, 0) = 0 AND COALESCE(t2.cnt, 0) = 0 AND COALESCE(t3.cnt, 0) = 1 THEN t3.security_name END,
                        CASE WHEN COALESCE(t1.cnt, 0) = 0 AND COALESCE(t2.cnt, 0) = 0 AND COALESCE(t3.cnt, 0) = 0 AND COALESCE(t4.cnt, 0) = 1 THEN t4.security_name END
                    ) AS final_security_name,
                    COALESCE(
                        CASE WHEN COALESCE(t1.cnt, 0) = 1 THEN t1.isin END,
                        CASE WHEN COALESCE(t1.cnt, 0) = 0 AND COALESCE(t2.cnt, 0) = 1 THEN t2.isin END,
                        CASE WHEN COALESCE(t1.cnt, 0) = 0 AND COALESCE(t2.cnt, 0) = 0 AND COALESCE(t3.cnt, 0) = 1 THEN t3.isin END,
                        CASE WHEN COALESCE(t1.cnt, 0) = 0 AND COALESCE(t2.cnt, 0) = 0 AND COALESCE(t3.cnt, 0) = 0 AND COALESCE(t4.cnt, 0) = 1 THEN t4.isin END
                    ) AS final_isin,
                    COALESCE(
                        CASE WHEN COALESCE(t1.cnt, 0) = 1 THEN t1.exchange_code END,
                        CASE WHEN COALESCE(t1.cnt, 0) = 0 AND COALESCE(t2.cnt, 0) = 1 THEN t2.exchange_code END,
                        CASE WHEN COALESCE(t1.cnt, 0) = 0 AND COALESCE(t2.cnt, 0) = 0 AND COALESCE(t3.cnt, 0) = 1 THEN t3.exchange_code END,
                        CASE WHEN COALESCE(t1.cnt, 0) = 0 AND COALESCE(t2.cnt, 0) = 0 AND COALESCE(t3.cnt, 0) = 0 AND COALESCE(t4.cnt, 0) = 1 THEN t4.exchange_code END
                    ) AS final_exchange,
                    COALESCE(
                        CASE WHEN COALESCE(t1.cnt, 0) = 1 THEN t1.country_of_exchange END,
                        CASE WHEN COALESCE(t1.cnt, 0) = 0 AND COALESCE(t2.cnt, 0) = 1 THEN t2.country_of_exchange END,
                        CASE WHEN COALESCE(t1.cnt, 0) = 0 AND COALESCE(t2.cnt, 0) = 0 AND COALESCE(t3.cnt, 0) = 1 THEN t3.country_of_exchange END,
                        CASE WHEN COALESCE(t1.cnt, 0) = 0 AND COALESCE(t2.cnt, 0) = 0 AND COALESCE(t3.cnt, 0) = 0 AND COALESCE(t4.cnt, 0) = 1 THEN t4.country_of_exchange END
                    ) AS final_country,
                    COALESCE(
                        CASE WHEN COALESCE(t1.cnt, 0) = 1 THEN t1.currency_code END,
                        CASE WHEN COALESCE(t1.cnt, 0) = 0 AND COALESCE(t2.cnt, 0) = 1 THEN t2.currency_code END,
                        CASE WHEN COALESCE(t1.cnt, 0) = 0 AND COALESCE(t2.cnt, 0) = 0 AND COALESCE(t3.cnt, 0) = 1 THEN t3.currency_code END,
                        CASE WHEN COALESCE(t1.cnt, 0) = 0 AND COALESCE(t2.cnt, 0) = 0 AND COALESCE(t3.cnt, 0) = 0 AND COALESCE(t4.cnt, 0) = 1 THEN t4.currency_code END
                    ) AS final_currency,
                    CASE
                        WHEN COALESCE(t1.cnt, 0) > 1 THEN 'FAIL: MULTIPLE_MATCH_SHORT_NAME'
                        WHEN COALESCE(t1.cnt, 0) = 1 THEN 'SHORT_NAME'
                        WHEN COALESCE(t2.cnt, 0) > 1 THEN 'FAIL: MULTIPLE_MATCH_ISIN'
                        WHEN COALESCE(t2.cnt, 0) = 1 THEN 'ISIN'
                        WHEN COALESCE(t3.cnt, 0) > 1 THEN 'FAIL: MULTIPLE_MATCH_TICKER'
                        WHEN COALESCE(t3.cnt, 0) = 1 THEN 'TICKER'
                        WHEN COALESCE(t4.cnt, 0) > 1 THEN 'FAIL: MULTIPLE_MATCH_FULL_NAME'
                        WHEN COALESCE(t4.cnt, 0) = 1 THEN 'FULL_NAME'
                        ELSE 'PENDING'
                    END AS security_match_method,
                    CASE
                        WHEN COALESCE(t1.cnt, 0) > 1 THEN 'FAIL: MULTIPLE_MATCH_SHORT_NAME'
                        WHEN COALESCE(t1.cnt, 0) = 1 THEN 'SHORT_NAME_MATCH'
                        WHEN COALESCE(t2.cnt, 0) > 1 THEN 'FAIL: MULTIPLE_MATCH_ISIN'
                        WHEN COALESCE(t2.cnt, 0) = 1 THEN 'ISIN_MATCH'
                        WHEN COALESCE(t3.cnt, 0) > 1 THEN 'FAIL: MULTIPLE_MATCH_TICKER'
                        WHEN COALESCE(t3.cnt, 0) = 1 THEN 'TICKER_MATCH'
                        WHEN COALESCE(t4.cnt, 0) > 1 THEN 'FAIL: MULTIPLE_MATCH_FULL_NAME'
                        WHEN COALESCE(t4.cnt, 0) = 1 THEN 'FULL_NAME_MATCH'
                        ELSE 'PENDING'
                    END AS security_status
                FROM base
                LEFT JOIN t1 ON base.row_id = t1.row_id AND t1.rn = 1
                LEFT JOIN t2 ON base.row_id = t2.row_id AND t2.rn = 1
                LEFT JOIN t3 ON base.row_id = t3.row_id AND t3.rn = 1
                LEFT JOIN t4 ON base.row_id = t4.row_id AND t4.rn = 1
                """,
                database=db
            )
            _s34a_bd = _breakdown(f'pos_stage_4_security_fallback_{ETL_SFX}', 'security_status')
            logger.info(f"[position_etl] Step 3 Stage A (tiers 1-4) complete: {_s34a_bd}")
            _t = _step_time("Step 3 (ISIN match)", _t)

            # ---- Stage B (Python): Tier 5 — Normalized Full Name + Country ----
            # Uses desc_prefix (COMMON STOCK/STICK suffix already stripped),
            # NOT the raw security_full_name -- otherwise a row whose upload
            # full name carries that suffix never matches
            # cis_security.security_description (which doesn't), while a row
            # without the suffix matches fine. Same fix as tiers 4/8/9.
            _pending_b = impala_manager.execute_query(
                "SELECT row_id, desc_prefix, resolved_country "
                f"FROM pos_stage_4_security_fallback_{ETL_SFX} WHERE security_status = 'PENDING'",
                database=db
            ) or []
            if _pending_b:
                _norm_cache = _build_normalized_cache()
                _t5_match, _t5_multi = {}, set()
                for _row in _pending_b:
                    _country = (_row.get('resolved_country') or '').strip()
                    if not _country:
                        continue  # required field not populated — tier 5 not evaluated
                    _key = abbreviate_security_name(_row.get('desc_prefix') or '')
                    if not _key:
                        continue
                    _country_matches = [
                        c for c in _norm_cache.get(_key, [])
                        if _country.upper() == (c.get('exchange_code') or '').strip().upper()
                        or _country.upper() == (c.get('country_of_exchange') or '').strip().upper()
                    ]
                    if len(_country_matches) == 1:
                        _t5_match[_row['row_id']] = _country_matches[0]
                    elif len(_country_matches) > 1:
                        _t5_multi.add(_row['row_id'])
                if _t5_match or _t5_multi:
                    _apply_python_tier_result(_t5_match, _t5_multi, 'NORMALIZED_FULL_NAME')
                logger.info(f"[position_etl] Stage B (Tier 5 normalized+country) — {len(_t5_match)} matched, {len(_t5_multi)} multi-match")
            else:
                logger.info("[position_etl] Stage B (Tier 5): no PENDING rows")

            # ---- Stage C (SQL): Tier 6 ISIN only, Tier 7 Ticker only, Tier 8 Full Name only
            #      (all three: country-blank fallback, see t6/t7/t8 comment below) ----
            impala_manager.execute_write(
                f"DROP TABLE IF EXISTS pos_stage_4_tier_update_{ETL_SFX}", database=db
            )
            impala_manager.execute_write(
                f"""
                CREATE TABLE pos_stage_4_tier_update_{ETL_SFX}
                STORED AS PARQUET AS
                WITH
                pending AS (
                    SELECT * FROM pos_stage_4_security_fallback_{ETL_SFX} WHERE security_status = 'PENDING'
                ),
                -- Tiers 6/7/8 are the ISIN-only / Ticker-only / Full-Name-only
                -- fallback for rows whose upload country was genuinely blank (SA
                -- spec: all of tiers 6-9 are the country-blank fallback group,
                -- mirroring tiers 2-5's country-required group). Gating only on
                -- "tiers 1-5 found nothing" was wrong -- it also fired when the
                -- upload DID supply a country but that country didn't match any
                -- cis_security row (e.g. same ISIN cross-listed under a different
                -- country), silently matching the wrong listing instead of
                -- respecting the country mismatch signal. Tier 9 (Python, Stage D)
                -- has the same gate applied there.
                t6 AS (
                    SELECT p.row_id, sn.security_id, sn.security_name, sn.isin,
                           sn.exchange_code, sn.country_of_exchange, sn.currency_code,
                           ROW_NUMBER() OVER (PARTITION BY p.row_id ORDER BY sn.security_id) AS rn,
                           COUNT(*) OVER (PARTITION BY p.row_id) AS cnt
                    FROM pending p
                    JOIN {db}.cis_security sn
                        ON  sn.is_active = true
                        AND (p.resolved_country IS NULL OR TRIM(p.resolved_country) = '')
                        AND p.upload_isin IS NOT NULL AND TRIM(p.upload_isin) != ''
                        AND UPPER(TRIM(p.upload_isin)) NOT IN ('NA', 'N/A', 'NIL', 'NONE', '-', 'N.A.', 'NAP')
                        AND UPPER(TRIM(CAST(p.upload_isin AS STRING))) = UPPER(TRIM(CAST(sn.isin AS STRING)))
                ),
                t7 AS (
                    SELECT p.row_id, sn.security_id, sn.security_name, sn.isin,
                           sn.exchange_code, sn.country_of_exchange, sn.currency_code,
                           ROW_NUMBER() OVER (PARTITION BY p.row_id ORDER BY sn.security_id) AS rn,
                           COUNT(*) OVER (PARTITION BY p.row_id) AS cnt
                    FROM pending p
                    JOIN {db}.cis_security sn
                        ON  sn.is_active = true
                        AND (p.resolved_country IS NULL OR TRIM(p.resolved_country) = '')
                        AND p.clean_ticker IS NOT NULL AND TRIM(p.clean_ticker) != ''
                        AND regexp_replace(UPPER(TRIM(CAST(sn.ticker AS STRING))), '\\\\s+EQUITY$', '') = p.clean_ticker
                ),
                t8 AS (
                    SELECT p.row_id, sn.security_id, sn.security_name, sn.isin,
                           sn.exchange_code, sn.country_of_exchange, sn.currency_code,
                           ROW_NUMBER() OVER (PARTITION BY p.row_id ORDER BY sn.security_id) AS rn,
                           COUNT(*) OVER (PARTITION BY p.row_id) AS cnt
                    FROM pending p
                    JOIN {db}.cis_security sn
                        ON  sn.is_active = true
                        AND (p.resolved_country IS NULL OR TRIM(p.resolved_country) = '')
                        AND p.desc_prefix IS NOT NULL AND TRIM(p.desc_prefix) != ''
                        AND sn.security_description IS NOT NULL
                        AND UPPER(TRIM(sn.security_description)) = UPPER(TRIM(p.desc_prefix))
                )
                SELECT
                    p.row_id, p.upload_isin, p.security_full_name, p.security_short_name,
                    p.desc_prefix, p.upload_exchange, p.portfolio_status, p.resolved_country, p.clean_ticker,
                    COALESCE(
                        CASE WHEN COALESCE(t6.cnt, 0) = 1 THEN t6.security_id END,
                        CASE WHEN COALESCE(t6.cnt, 0) = 0 AND COALESCE(t7.cnt, 0) = 1 THEN t7.security_id END,
                        CASE WHEN COALESCE(t6.cnt, 0) = 0 AND COALESCE(t7.cnt, 0) = 0 AND COALESCE(t8.cnt, 0) = 1 THEN t8.security_id END
                    ) AS final_security_id,
                    COALESCE(
                        CASE WHEN COALESCE(t6.cnt, 0) = 1 THEN t6.security_name END,
                        CASE WHEN COALESCE(t6.cnt, 0) = 0 AND COALESCE(t7.cnt, 0) = 1 THEN t7.security_name END,
                        CASE WHEN COALESCE(t6.cnt, 0) = 0 AND COALESCE(t7.cnt, 0) = 0 AND COALESCE(t8.cnt, 0) = 1 THEN t8.security_name END
                    ) AS final_security_name,
                    COALESCE(
                        CASE WHEN COALESCE(t6.cnt, 0) = 1 THEN t6.isin END,
                        CASE WHEN COALESCE(t6.cnt, 0) = 0 AND COALESCE(t7.cnt, 0) = 1 THEN t7.isin END,
                        CASE WHEN COALESCE(t6.cnt, 0) = 0 AND COALESCE(t7.cnt, 0) = 0 AND COALESCE(t8.cnt, 0) = 1 THEN t8.isin END
                    ) AS final_isin,
                    COALESCE(
                        CASE WHEN COALESCE(t6.cnt, 0) = 1 THEN t6.exchange_code END,
                        CASE WHEN COALESCE(t6.cnt, 0) = 0 AND COALESCE(t7.cnt, 0) = 1 THEN t7.exchange_code END,
                        CASE WHEN COALESCE(t6.cnt, 0) = 0 AND COALESCE(t7.cnt, 0) = 0 AND COALESCE(t8.cnt, 0) = 1 THEN t8.exchange_code END
                    ) AS final_exchange,
                    COALESCE(
                        CASE WHEN COALESCE(t6.cnt, 0) = 1 THEN t6.country_of_exchange END,
                        CASE WHEN COALESCE(t6.cnt, 0) = 0 AND COALESCE(t7.cnt, 0) = 1 THEN t7.country_of_exchange END,
                        CASE WHEN COALESCE(t6.cnt, 0) = 0 AND COALESCE(t7.cnt, 0) = 0 AND COALESCE(t8.cnt, 0) = 1 THEN t8.country_of_exchange END
                    ) AS final_country,
                    COALESCE(
                        CASE WHEN COALESCE(t6.cnt, 0) = 1 THEN t6.currency_code END,
                        CASE WHEN COALESCE(t6.cnt, 0) = 0 AND COALESCE(t7.cnt, 0) = 1 THEN t7.currency_code END,
                        CASE WHEN COALESCE(t6.cnt, 0) = 0 AND COALESCE(t7.cnt, 0) = 0 AND COALESCE(t8.cnt, 0) = 1 THEN t8.currency_code END
                    ) AS final_currency,
                    CASE
                        WHEN COALESCE(t6.cnt, 0) > 1 THEN 'FAIL: MULTIPLE_MATCH_ISIN_ONLY'
                        WHEN COALESCE(t6.cnt, 0) = 1 THEN 'ISIN_ONLY'
                        WHEN COALESCE(t7.cnt, 0) > 1 THEN 'FAIL: MULTIPLE_MATCH_TICKER_ONLY'
                        WHEN COALESCE(t7.cnt, 0) = 1 THEN 'TICKER_ONLY'
                        WHEN COALESCE(t8.cnt, 0) > 1 THEN 'FAIL: MULTIPLE_MATCH_FULL_NAME_ONLY'
                        WHEN COALESCE(t8.cnt, 0) = 1 THEN 'FULL_NAME_ONLY'
                        ELSE 'PENDING'
                    END AS security_match_method,
                    CASE
                        WHEN COALESCE(t6.cnt, 0) > 1 THEN 'FAIL: MULTIPLE_MATCH_ISIN_ONLY'
                        WHEN COALESCE(t6.cnt, 0) = 1 THEN 'ISIN_ONLY_MATCH'
                        WHEN COALESCE(t7.cnt, 0) > 1 THEN 'FAIL: MULTIPLE_MATCH_TICKER_ONLY'
                        WHEN COALESCE(t7.cnt, 0) = 1 THEN 'TICKER_ONLY_MATCH'
                        WHEN COALESCE(t8.cnt, 0) > 1 THEN 'FAIL: MULTIPLE_MATCH_FULL_NAME_ONLY'
                        WHEN COALESCE(t8.cnt, 0) = 1 THEN 'FULL_NAME_ONLY_MATCH'
                        ELSE 'PENDING'
                    END AS security_status
                FROM pending p
                LEFT JOIN t6 ON p.row_id = t6.row_id AND t6.rn = 1
                LEFT JOIN t7 ON p.row_id = t7.row_id AND t7.rn = 1
                LEFT JOIN t8 ON p.row_id = t8.row_id AND t8.rn = 1

                UNION ALL

                SELECT
                    row_id, upload_isin, security_full_name, security_short_name,
                    desc_prefix, upload_exchange, portfolio_status, resolved_country, clean_ticker,
                    final_security_id, final_security_name, final_isin, final_exchange,
                    final_country, final_currency, security_match_method, security_status
                FROM pos_stage_4_security_fallback_{ETL_SFX}
                WHERE security_status != 'PENDING'
                """,
                database=db
            )
            impala_manager.execute_write(
                f"DROP TABLE IF EXISTS pos_stage_4_security_fallback_{ETL_SFX}", database=db
            )
            impala_manager.execute_write(
                f"CREATE TABLE pos_stage_4_security_fallback_{ETL_SFX} STORED AS PARQUET AS "
                f"SELECT * FROM pos_stage_4_tier_update_{ETL_SFX}",
                database=db
            )
            impala_manager.execute_write(
                f"DROP TABLE IF EXISTS pos_stage_4_tier_update_{ETL_SFX}", database=db
            )
            _s34c_bd = _breakdown(f'pos_stage_4_security_fallback_{ETL_SFX}', 'security_status')
            logger.info(f"[position_etl] Step 4 Stage C (tiers 6-8) complete: {_s34c_bd}")
            _t = _step_time("Step 4 (security fallback)", _t)

            # ---- Stage D (Python): Tier 9 — Normalized Full Name only (country blank) ----
            # Uses desc_prefix -- see Stage B (Tier 5) comment above for why.
            _pending_d = impala_manager.execute_query(
                "SELECT row_id, desc_prefix, resolved_country "
                f"FROM pos_stage_4_security_fallback_{ETL_SFX} WHERE security_status = 'PENDING'",
                database=db
            ) or []
            if _pending_d:
                _norm_cache = _build_normalized_cache()
                _t9_match, _t9_multi = {}, set()
                for _row in _pending_d:
                    if (_row.get('resolved_country') or '').strip():
                        continue  # tier 9 is the country-blank fallback — a mismatch stays PENDING
                    _key = abbreviate_security_name(_row.get('desc_prefix') or '')
                    if not _key:
                        continue
                    _candidates = _norm_cache.get(_key, [])
                    if len(_candidates) == 1:
                        _t9_match[_row['row_id']] = _candidates[0]
                    elif len(_candidates) > 1:
                        _t9_multi.add(_row['row_id'])
                if _t9_match or _t9_multi:
                    _apply_python_tier_result(_t9_match, _t9_multi, 'NORMALIZED_FULL_NAME_ONLY')
                logger.info(f"[position_etl] Stage D (Tier 9 normalized only) — {len(_t9_match)} matched, {len(_t9_multi)} multi-match")
            else:
                logger.info("[position_etl] Stage D (Tier 9): no PENDING rows")

            # ---- Tier 10: anything still PENDING is a create-security candidate ----
            impala_manager.execute_write(
                f"DROP TABLE IF EXISTS pos_stage_4_tier_update_{ETL_SFX}", database=db
            )
            impala_manager.execute_write(
                f"""
                CREATE TABLE pos_stage_4_tier_update_{ETL_SFX}
                STORED AS PARQUET AS
                SELECT
                    row_id, upload_isin, security_full_name, security_short_name,
                    desc_prefix, upload_exchange, portfolio_status, resolved_country, clean_ticker,
                    final_security_id, final_security_name, final_isin, final_exchange,
                    final_country, final_currency,
                    CASE WHEN security_status = 'PENDING' THEN 'NONE' ELSE security_match_method END AS security_match_method,
                    CASE WHEN security_status = 'PENDING' THEN 'NOT_FOUND: Create new security' ELSE security_status END AS security_status
                FROM pos_stage_4_security_fallback_{ETL_SFX}
                """,
                database=db
            )
            impala_manager.execute_write(
                f"DROP TABLE IF EXISTS pos_stage_4_security_fallback_{ETL_SFX}", database=db
            )
            impala_manager.execute_write(
                f"CREATE TABLE pos_stage_4_security_fallback_{ETL_SFX} STORED AS PARQUET AS "
                f"SELECT * FROM pos_stage_4_tier_update_{ETL_SFX}",
                database=db
            )
            impala_manager.execute_write(
                f"DROP TABLE IF EXISTS pos_stage_4_tier_update_{ETL_SFX}", database=db
            )
            _s4_bd = _breakdown(f'pos_stage_4_security_fallback_{ETL_SFX}', 'security_status')
            logger.info(f"[position_etl] Step 4 (10-tier cascade) complete: {_s4_bd}")
            _s4_notfound = _count(f'pos_stage_4_security_fallback_{ETL_SFX}', "security_status = 'NOT_FOUND: Create new security'")
            _s4_fail = _count(f'pos_stage_4_security_fallback_{ETL_SFX}', "security_status LIKE 'FAIL%'")
            if _s4_notfound > 0 or _s4_fail > 0:
                _sample_fails(
                    f'pos_stage_4_security_fallback_{ETL_SFX}', 'security_status',
                    portfolio_expr='b.portfolio',
                    name_expr='p.security_full_name',
                    isin_expr='p.upload_isin',
                    from_clause=f'pos_stage_4_security_fallback_{ETL_SFX} p '
                                f'JOIN pos_stage_1_base_{ETL_SFX} b ON p.row_id = b.row_id'
                )
            _t = _step_time("Step 4B", _t)

            # ------------------------------------------------------------------
            # Step 5: Price lookup — latest price per ISIN from cis_equity_price.
            #         Skip records with FAIL security status.
            # ------------------------------------------------------------------
            impala_manager.execute_write(
                f"DROP TABLE IF EXISTS pos_stage_5_price_{ETL_SFX}", database=db
            )
            ok_5 = impala_manager.execute_write(
                f"""
                CREATE TABLE pos_stage_5_price_{ETL_SFX}
                STORED AS PARQUET AS
                SELECT
                    b.row_id,
                    b.isin,
                    b.reporting_date,
                    b.market_price AS upload_market_price,
                    ep.main_closing_price,
                    CAST(CASE
                        WHEN ep.main_closing_price IS NOT NULL AND ep.main_closing_price != 0
                            THEN CAST(ep.main_closing_price AS DECIMAL(30,8))
                        WHEN b.market_price IS NOT NULL AND b.market_price != 0
                            THEN CAST(b.market_price AS DECIMAL(30,8))
                        ELSE CAST(NULL AS DECIMAL(30,8))
                    END AS DECIMAL(30,8)) AS final_market_price,
                    CASE
                        WHEN ep.main_closing_price IS NOT NULL AND ep.main_closing_price != 0
                            THEN 'PASS: Using cis_equity_price'
                        WHEN b.market_price IS NOT NULL AND b.market_price != 0
                            THEN 'PASS: Using uploaded'
                        WHEN ep.main_closing_price = 0 OR b.market_price = 0
                            THEN 'WARN: Price is zero (omitted)'
                        ELSE 'WARN: No price'
                    END AS price_status
                FROM pos_stage_1_base_{ETL_SFX} b
                JOIN pos_stage_4_security_fallback_{ETL_SFX} p4 ON b.row_id = p4.row_id
                LEFT JOIN (
                    SELECT isin, price_date, main_closing_price,
                           ROW_NUMBER() OVER (PARTITION BY isin, price_date ORDER BY price_timestamp DESC) AS rn
                    FROM {db}.cis_equity_price
                    WHERE is_active = true
                      AND main_closing_price IS NOT NULL
                      AND main_closing_price != 0
                ) ep ON b.isin = ep.isin AND b.reporting_date = ep.price_date AND ep.rn = 1
                WHERE p4.security_status NOT LIKE 'FAIL%'
                """,
                database=db
            )
            if not ok_5:
                logger.error(f"[position_etl] Step 5 FAILED — pos_stage_5_price_{ETL_SFX} not created; aborting ETL")
                return False, f"Step 5 CREATE TABLE pos_stage_5_price_{ETL_SFX} failed", result
            _s5_bd = _breakdown(f'pos_stage_5_price_{ETL_SFX}', 'price_status')
            logger.info(f"[position_etl] Step 5 complete — price lookup: {_s5_bd}")
            _t = _step_time("Step 5 (price lookup)", _t)

            # ------------------------------------------------------------------
            # Step 5B: Create new securities for NOT_FOUND rows.
            #   5B-i  : build candidates temp table with raw security_name
            #   5B-ii : fetch candidates into Python, apply abbreviate_security_name()
            #   5B-iii: INSERT into cis_security with abbreviated names
            # ------------------------------------------------------------------
            impala_manager.execute_write(
                f"DROP TABLE IF EXISTS pos_stage_5b_candidates_{ETL_SFX}", database=db
            )
            impala_manager.execute_write(
                f"""
                CREATE TABLE pos_stage_5b_candidates_{ETL_SFX}
                STORED AS PARQUET AS
                SELECT
                    -- Raw name (pre-abbreviation) — Python will abbreviate this.
                    COALESCE(
                        p4.desc_prefix,
                        b.security_short_name,
                        TRIM(regexp_replace(
                            b.security_full_name,
                            '(?i)\\\\s*COMMON\\\\s+(STOCK|STICK).*$',
                            ''
                        )),
                        b.isin
                    ) AS raw_security_name,
                    NULLIF(NULLIF(NULLIF(NULLIF(NULLIF(NULLIF(NULLIF(
                        UPPER(TRIM(CAST(b.isin AS STRING))),
                        'NA'), 'N/A'), 'NIL'), 'NONE'), '-'), 'N.A.'), 'NAP') AS isin,
                    b.security_full_name AS security_description,
                    NULLIF(NULLIF(NULLIF(NULLIF(NULLIF(NULLIF(NULLIF(
                        UPPER(TRIM(CAST(b.ticker AS STRING))),
                        'NA'), 'N/A'), 'NIL'), 'NONE'), '-'), 'N.A.'), 'NAP') AS ticker,
                    b.industry,
                    b.security_type,
                    b.issuer_type,
                    b.quoted_unquoted,
                    b.country_of_incorporation,
                    b.country_of_exchange,
                    b.`exchange`,
                    b.security_currency AS currency_code,
                    b.shares_outstanding,
                    b.fin_nonfin_co,
                    b.row_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPPER(TRIM(COALESCE(
                            p4.desc_prefix,
                            b.security_short_name,
                            TRIM(regexp_replace(b.security_full_name, '(?i)\\\\s*COMMON\\\\s+(STOCK|STICK).*$', '')),
                            b.isin
                        )))
                        ORDER BY b.row_id
                    ) AS rn
                FROM pos_stage_1_base_{ETL_SFX} b
                JOIN pos_stage_4_security_fallback_{ETL_SFX} p4 ON b.row_id = p4.row_id
                JOIN pos_stage_2_portfolio_{ETL_SFX} p2
                    ON b.row_id = p2.row_id AND p2.portfolio_status = 'PASS'
                WHERE p4.security_status = 'NOT_FOUND: Create new security'
                  AND (b.quantity IS NOT NULL OR b.cost_fc IS NOT NULL)
                  AND {'TRUE' if auto_create_security else 'FALSE'}
                """,
                database=db
            )
            # NOTE: no SQL-side existing-security_name dedup here (removed —
            # it compared the pre-abbreviation raw name against
            # cis_security.security_name, which stores the POST-abbreviation
            # form for every security this ETL previously created, so the
            # comparison almost never matched and let real name collisions
            # through). The dedup now happens below in Python, against
            # abbreviate_security_name(raw_name) via the same cache tiers
            # 5/9 use — the actual value that would be written.

            # 5B-ii: fetch distinct candidates (rn=1), abbreviate security_name in Python
            _candidates = impala_manager.execute_query(
                f"SELECT * FROM pos_stage_5b_candidates_{ETL_SFX} WHERE rn = 1",
                database=db
            ) or []

            import time as _time

            def _sql_str(v):
                # Impala uses C-style \' escaping, not doubled quotes.
                if v in (None, ''):
                    return 'NULL'
                s = str(v).replace('\\', '\\\\').replace("'", "\\'")
                return "'" + s + "'"

            def _sql_bigint(v):
                try:
                    return str(int(float(v)))
                except (TypeError, ValueError):
                    return 'NULL'

            # Duplicate-name guard: a candidate's FINAL name (post-abbreviation)
            # may collide with a security that already exists under a
            # different exchange — the common case is the same issuer
            # cross-listed on more than one exchange. cis_security has no
            # unique constraint on security_name (only security_id, the PK),
            # so an unguarded INSERT would silently succeed and create a
            # second, ambiguous security with the same name.
            #
            # Resolution order per candidate:
            #   1. No collision at all       -> create with the plain abbreviated name.
            #   2. Collision on a DIFFERENT exchange (the cross-listing case)
            #                                 -> disambiguate by appending the
            #                                    exchange code, e.g. 'DBS' on
            #                                    a second exchange becomes
            #                                    'DBS HK'; create under that
            #                                    name instead.
            #   3. Collision on the SAME exchange, or the disambiguated name
            #      *still* collides         -> genuinely ambiguous, can't be
            #                                   resolved automatically — fail
            #                                   the row instead of creating or
            #                                   silently skipping it.
            _norm_cache_5b = _build_normalized_cache()
            _collision_rows: dict = {}   # row_id -> FAIL reason string
            _pending: dict = {}          # sec_name -> {'row_id':.., 'exchange':..} (this batch)

            def _exch_key(d: dict) -> str:
                return (d.get('country_of_exchange') or d.get('exchange_code')
                        or d.get('exchange') or '').strip().upper()

            def _lookup(name: str) -> list:
                hits = list(_norm_cache_5b.get(name) or [])
                _p = _pending.get(name)
                if _p:
                    hits.append({
                        'security_id': f"pending row_id={_p['row_id']}",
                        'exchange_code': _p['exchange'], 'country_of_exchange': _p['exchange'],
                        'isin': None,
                    })
                return hits

            if _candidates:
                _now = _time.strftime('%Y-%m-%d %H:%M:%S')
                _base_ts = int(_time.time()) * 1000
                _value_rows = []
                for _row in _candidates:
                    _raw_name = _row.get('raw_security_name') or ''
                    _row_id   = int(_row.get('row_id') or 0)
                    _cand_exch = _exch_key(_row)
                    _base_name = abbreviate_security_name(_raw_name)

                    _sec_name = None
                    _fail_reason = None
                    _hits = _lookup(_base_name)
                    if not _hits:
                        _sec_name = _base_name
                    else:
                        _same_exch = next((h for h in _hits if _exch_key(h) == _cand_exch), None)
                        if _same_exch:
                            _fail_reason = (
                                f"FAIL: DUPLICATE_NAME — security '{_base_name}' already exists on the "
                                f"same exchange (security_id={_same_exch.get('security_id')}, "
                                f"isin={_same_exch.get('isin') or 'N/A'}, exchange={_cand_exch or 'N/A'})"
                            )
                        else:
                            # Cross-listed on a different exchange — disambiguate.
                            _suffix = _cand_exch or 'UNK'
                            _max_base = max(10, 35 - len(_suffix) - 1)
                            _disamb_name = f"{abbreviate_security_name(_raw_name, max_len=_max_base)} {_suffix}"
                            _hits2 = _lookup(_disamb_name)
                            if _hits2:
                                _e = _hits2[0]
                                _fail_reason = (
                                    f"FAIL: DUPLICATE_NAME — security '{_base_name}' already exists under a "
                                    f"different exchange (security_id={_e.get('security_id')}, "
                                    f"exchange={_exch_key(_e) or 'N/A'}); disambiguated name '{_disamb_name}' "
                                    f"also collides — needs manual resolution"
                                )
                            else:
                                _sec_name = _disamb_name
                                logger.info(
                                    f"[position_etl] Step 5B: disambiguated '{_base_name}' -> '{_sec_name}' "
                                    f"(candidate exchange={_cand_exch or 'N/A'}, existing exchange="
                                    f"{_exch_key(_hits[0]) or 'N/A'})"
                                )

                    if _fail_reason:
                        _collision_rows[_row_id] = _fail_reason
                        continue
                    _pending[_sec_name] = {'row_id': _row_id, 'exchange': _cand_exch}

                    _value_rows.append(
                        f"({_base_ts + _row_id},"
                        f"{_sql_str(_sec_name)},"
                        f"{_sql_str(_row.get('isin'))},"
                        f"{_sql_str(_row.get('security_description'))},"
                        f"NULL,"
                        f"{_sql_str(_row.get('ticker'))},"
                        f"{_sql_str(_row.get('industry'))},"
                        f"{_sql_str(_row.get('security_type'))},"
                        f"NULL,"
                        f"{_sql_str(_row.get('issuer_type'))},"
                        f"{_sql_str(_row.get('quoted_unquoted'))},"
                        f"{_sql_str(_row.get('country_of_incorporation'))},"
                        f"{_sql_str(_row.get('country_of_exchange'))},"
                        f"{_sql_str(_row.get('exchange'))},"
                        f"{_sql_str(_row.get('currency_code'))},"
                        f"{_sql_bigint(_row.get('shares_outstanding'))},"
                        f"{_sql_str(_row.get('fin_nonfin_co'))},"
                        f"'CIS','ACTIVE',TRUE,"
                        f"'POSITION_UPLOAD','{_now}',"
                        f"'POSITION_UPLOAD','{_now}')"
                    )

                # Single batch INSERT — one round trip regardless of row count
                if _value_rows:
                    impala_manager.execute_write(
                        f"""
                        INSERT INTO {db}.cis_security (
                            security_id, security_name, isin, security_description,
                            issuer, ticker, industry, security_type, investment_type,
                            issuer_type, quoted_unquoted, country_of_incorporation,
                            country_of_exchange, exchange_code, currency_code,
                            shares_outstanding, fin_nonfin_ind, src_system, status,
                            is_active, created_by, created_at, updated_by, updated_at
                        ) VALUES {', '.join(_value_rows)}
                        """,
                        database=db
                    )

            if _collision_rows:
                _coll_when = ' '.join(
                    f"WHEN row_id = {rid} THEN {_sql_str(reason)}"
                    for rid, reason in _collision_rows.items()
                )
                impala_manager.execute_write(
                    f"DROP TABLE IF EXISTS pos_stage_4_collision_update_{ETL_SFX}", database=db
                )
                impala_manager.execute_write(
                    f"""
                    CREATE TABLE pos_stage_4_collision_update_{ETL_SFX}
                    STORED AS PARQUET AS
                    SELECT
                        row_id, upload_isin, security_full_name, security_short_name,
                        desc_prefix, upload_exchange, portfolio_status, resolved_country,
                        clean_ticker, final_security_id, final_security_name, final_isin,
                        final_exchange, final_country, final_currency, security_match_method,
                        CASE {_coll_when} ELSE security_status END AS security_status
                    FROM pos_stage_4_security_fallback_{ETL_SFX}
                    """,
                    database=db
                )
                impala_manager.execute_write(
                    f"DROP TABLE IF EXISTS pos_stage_4_security_fallback_{ETL_SFX}", database=db
                )
                impala_manager.execute_write(
                    f"CREATE TABLE pos_stage_4_security_fallback_{ETL_SFX} STORED AS PARQUET AS "
                    f"SELECT * FROM pos_stage_4_collision_update_{ETL_SFX}",
                    database=db
                )
                impala_manager.execute_write(
                    f"DROP TABLE IF EXISTS pos_stage_4_collision_update_{ETL_SFX}", database=db
                )
                logger.warning(
                    f"[position_etl] Step 5B: {len(_collision_rows)} row(s) blocked — "
                    f"security name collides with an existing/pending security under a "
                    f"different exchange or ISIN; marked FAIL instead of creating a duplicate"
                )

            if _candidates:
                # Invalidate abbrev cache so newly created securities are visible
                # on the next ETL run without waiting for TTL expiry
                UploadService._cis_abbrev_cache_ts = 0.0

            logger.info(f"[position_etl] Step 5B complete — {len(_candidates) - len(_collision_rows)} new securities created, {len(_collision_rows)} blocked as duplicates")
            if _candidates:
                for _c in _candidates[:5]:
                    logger.info(f"[position_etl] Step 5B: created security label='{_c.get('security_label')}' isin='{_c.get('isin')}' currency='{_c.get('currency_code')}'")
            _t = _step_time("Step 5B (new security creation)", _t)

            # ------------------------------------------------------------------
            # Step 5C: Party (issuer) matching & auto-creation.
            #
            # Guarded by:
            #   1. GMP exclusion: skipped when src_id starts with 'gmp' (GMP
            #      daily positions are managed through their own sync path).
            #   2. auto_create_security flag: follows the same opt-in gate as
            #      Step 5B; when False, party creation is also disabled.
            #
            # For each upload row that has a resolvable issuer name we:
            #   a. Collect issuer names from newly created securities (Step 5B
            #      _candidates minus collisions) AND from existing securities
            #      (already matched in Step 4) — covering the case where the
            #      security exists but no party record does.
            #   b. Abbreviate the raw name using the same abbreviate_security_name()
            #      function, making the party_short_name (PK) consistent with
            #      cis_security.security_name.
            #   c. Collapse multiple securities that share the same issuer name
            #      into a single party candidate (DISTINCT on party_short_name).
            #   d. Query cis_party once to find already-existing parties; skip
            #      those — reuse rather than duplicate.
            #   e. Batch UPSERT the remaining new parties in a single round-trip.
            # ------------------------------------------------------------------
            _is_gmp_src = src_id.lower().startswith('gmp')
            if auto_create_security and not _is_gmp_src:
                # ---- Collect raw issuer names from two sources ----
                #
                # Source A: securities created in Step 5B (use the raw_security_name
                # field from _candidates, minus the collision rows).
                #
                # Source B: existing-security rows matched in Step 4 — join
                # pos_stage_4_security_fallback (has final_security_id) back to
                # cis_security to get the issuer field.  We fetch these from
                # Impala in one query so we don't have to read the full
                # pos_stage_4 table in Python.

                # A: names from newly created securities
                _created_row_ids = {
                    int(_row.get('row_id') or 0): _row.get('raw_security_name') or ''
                    for _row in _candidates
                    if int(_row.get('row_id') or 0) not in _collision_rows
                }

                # B: names from existing matched securities (issuer stored in cis_security)
                _existing_issuer_rows = impala_manager.execute_query(
                    f"""
                    SELECT DISTINCT s.issuer AS raw_issuer_name,
                           b.industry, b.issuer_type,
                           b.country_of_incorporation, b.cels
                    FROM pos_stage_4_security_fallback_{ETL_SFX} p4
                    JOIN pos_stage_1_base_{ETL_SFX} b ON b.row_id = p4.row_id
                    JOIN {db}.cis_security s ON s.security_id = p4.final_security_id
                    WHERE p4.final_security_id IS NOT NULL
                      AND s.issuer IS NOT NULL
                      AND TRIM(s.issuer) != ''
                    """,
                    database=db
                ) or []

                # ---- Build unified candidate dict keyed by party_short_name ----
                # Multiple securities → one party: last-write-wins for metadata
                # (all rows for the same issuer carry essentially the same fields).
                _party_candidates: dict = {}  # party_short_name -> metadata dict

                # Add Source B first (existing securities) so Source A (new
                # securities, which have richer metadata from the upload) can
                # override if the same issuer appears in both.
                for _er in _existing_issuer_rows:
                    _raw = (_er.get('raw_issuer_name') or '').strip()
                    if not _raw:
                        continue
                    _pname = abbreviate_security_name(_raw)
                    if not _pname:
                        continue
                    _party_candidates[_pname] = {
                        'party_full_name':         _raw,
                        'industry':                _er.get('industry'),
                        'issuer_type':             _er.get('issuer_type'),
                        'country_of_incorporation': _er.get('country_of_incorporation'),
                        'cels':                    _er.get('cels'),
                    }

                # Source A: newly created securities
                for _row in _candidates:
                    if int(_row.get('row_id') or 0) in _collision_rows:
                        continue
                    _raw = (_row.get('raw_security_name') or '').strip()
                    if not _raw:
                        continue
                    _pname = abbreviate_security_name(_raw)
                    if not _pname:
                        continue
                    _party_candidates[_pname] = {
                        'party_full_name':         _raw,
                        'industry':                _row.get('industry'),
                        'issuer_type':             _row.get('issuer_type'),
                        'country_of_incorporation': _row.get('country_of_incorporation'),
                        'cels':                    _row.get('cels'),
                    }

                if _party_candidates:
                    # ---- Deduplicate against existing cis_party rows ----
                    _existing_names_in_list = ', '.join(
                        _sql_str(n) for n in _party_candidates
                    )
                    _existing_party_rows = impala_manager.execute_query(
                        f"""
                        SELECT party_short_name
                        FROM {db}.cis_party
                        WHERE party_short_name IN ({_existing_names_in_list})
                        """,
                        database=db
                    ) or []
                    _existing_party_names = {
                        r.get('party_short_name') for r in _existing_party_rows
                    }

                    _new_parties = {
                        name: meta
                        for name, meta in _party_candidates.items()
                        if name not in _existing_party_names
                    }

                    _reused_count = len(_party_candidates) - len(_new_parties)
                    logger.info(
                        f"[position_etl] Step 5C: {len(_party_candidates)} issuer(s) found — "
                        f"{_reused_count} already in cis_party (reused), "
                        f"{len(_new_parties)} to create"
                    )

                    # ---- Batch UPSERT new parties (one round-trip) ----
                    if _new_parties:
                        _party_now = _time.strftime('%Y-%m-%d %H:%M:%S')
                        _party_value_rows = []
                        for _pname, _meta in _new_parties.items():
                            _country = _meta.get('country_of_incorporation')
                            _party_value_rows.append(
                                f"({_sql_str(_pname)},"
                                f"{_sql_str(_meta.get('party_full_name'))},"
                                f"{_sql_str(_meta.get('issuer_type'))},"
                                f"{_sql_str(_meta.get('industry'))},"
                                f"{_sql_str(_country)},"
                                f"{_sql_str(_country)},"
                                f"{_sql_str(_meta.get('cels'))},"
                                f"FALSE,FALSE,TRUE,FALSE,FALSE,FALSE,"
                                f"'POSITION_UPLOAD',"
                                f"'INITIAL',"
                                f"TRUE,FALSE,"
                                f"{_sql_str(updated_by)},'{_party_now}',"
                                f"{_sql_str(updated_by)},'{_party_now}')"
                            )
                        impala_manager.execute_write(
                            f"""
                            UPSERT INTO {db}.cis_party (
                                party_short_name, party_full_name, record_type,
                                industry, country_of_incorporation, country,
                                cels_code,
                                is_broker, is_custodian, is_issuer, is_bank,
                                is_subsidiary, is_corporate,
                                src_system, status,
                                is_active, is_deleted,
                                created_by, created_at, updated_by, updated_at
                            ) VALUES {', '.join(_party_value_rows)}
                            """,
                            database=db
                        )
                        for _pn in list(_new_parties)[:5]:
                            logger.info(f"[position_etl] Step 5C: created party party_short_name='{_pn}'")
                else:
                    logger.info("[position_etl] Step 5C: no issuer candidates — nothing to create")

            else:
                if _is_gmp_src:
                    logger.info("[position_etl] Step 5C: skipped (GMP source — party sync handled separately)")
                else:
                    logger.info("[position_etl] Step 5C: skipped (auto_create_security=False)")
            _t = _step_time("Step 5C (party matching & creation)", _t)

            # ------------------------------------------------------------------
            # Step 5D: when auto_create_security is False, pos_stage_5b_candidates
            # was forced empty above so nothing got created — fail every row
            # still sitting at 'NOT_FOUND: Create new security' instead of
            # letting Step 6 report it as valid. Uses a broad status-only
            # rewrite (not a per-row CASE) since every NOT_FOUND row gets the
            # same reason here, unlike the per-row collision reasons above.
            # ------------------------------------------------------------------
            if not auto_create_security:
                impala_manager.execute_write(
                    f"DROP TABLE IF EXISTS pos_stage_4_tier_update_{ETL_SFX}", database=db
                )
                impala_manager.execute_write(
                    f"""
                    CREATE TABLE pos_stage_4_tier_update_{ETL_SFX}
                    STORED AS PARQUET AS
                    SELECT
                        row_id, upload_isin, security_full_name, security_short_name,
                        desc_prefix, upload_exchange, portfolio_status, resolved_country,
                        clean_ticker, final_security_id, final_security_name, final_isin,
                        final_exchange, final_country, final_currency, security_match_method,
                        CASE
                            WHEN security_status = 'NOT_FOUND: Create new security'
                                THEN 'FAIL: Security not found — auto-create disabled for this upload'
                            ELSE security_status
                        END AS security_status
                    FROM pos_stage_4_security_fallback_{ETL_SFX}
                    """,
                    database=db
                )
                impala_manager.execute_write(
                    f"DROP TABLE IF EXISTS pos_stage_4_security_fallback_{ETL_SFX}", database=db
                )
                impala_manager.execute_write(
                    f"CREATE TABLE pos_stage_4_security_fallback_{ETL_SFX} STORED AS PARQUET AS "
                    f"SELECT * FROM pos_stage_4_tier_update_{ETL_SFX}",
                    database=db
                )
                impala_manager.execute_write(
                    f"DROP TABLE IF EXISTS pos_stage_4_tier_update_{ETL_SFX}", database=db
                )
                _notfound_failed = _count(
                    f'pos_stage_4_security_fallback_{ETL_SFX}',
                    "security_status LIKE 'FAIL: Security not found%'"
                )
                logger.info(
                    f"[position_etl] Step 5D: auto_create_security=False — "
                    f"{_notfound_failed} row(s) failed instead of creating a new security"
                )
                _t = _step_time("Step 5D (auto-create disabled)", _t)

            # ------------------------------------------------------------------
            # Step 6: Final staging — INNER JOIN on portfolio PASS; compute
            #         final_quantity, final_market_value_fc, overall_status.
            #         Also create position_upload_failed for reporting.
            # ------------------------------------------------------------------
            impala_manager.execute_write(
                f"DROP TABLE IF EXISTS position_upload_staging_{ETL_SFX}", database=db
            )
            impala_manager.execute_write(
                f"""
                CREATE TABLE position_upload_staging_{ETL_SFX}
                STORED AS PARQUET AS
                SELECT
                    b.*,
                    p2.valid_portfolio,
                    p2.portfolio_currency,
                    p2.portfolio_status,
                    p4.final_security_id,
                    p4.final_security_name AS matched_security_name,
                    p4.final_isin,
                    p4.final_country AS country_resolved,
                    p4.final_currency AS security_currency_resolved,
                    p4.security_status,
                    p4.security_match_method,
                    p5.final_market_price,
                    p5.price_status,
                    CASE
                        WHEN b.quantity IS NOT NULL THEN CAST(b.quantity AS DECIMAL(30,8))
                        WHEN b.cost_fc IS NOT NULL  THEN CAST(b.cost_fc  AS DECIMAL(30,8))
                        ELSE CAST(NULL AS DECIMAL(30,8))
                    END AS final_quantity,
                    CASE
                        WHEN b.quantity IS NOT NULL THEN 'PASS'
                        WHEN b.cost_fc IS NOT NULL  THEN 'PASS: Using cost_fc'
                        ELSE 'FAIL: Both quantity and cost_fc null'
                    END AS quantity_status,
                    CASE
                        WHEN b.shares_issued IS NOT NULL THEN CAST(b.shares_issued AS DECIMAL(30,8))
                        WHEN b.pct_holding IS NOT NULL AND b.quantity IS NOT NULL AND b.pct_holding > 0
                            THEN CAST(CAST(b.quantity AS DECIMAL(30,8)) / CAST(b.pct_holding AS DECIMAL(30,8)) AS DECIMAL(30,8))
                        ELSE CAST(NULL AS DECIMAL(30,8))
                    END AS final_shares_issued,
                    CASE
                        WHEN b.`exchange` IS NULL OR TRIM(b.`exchange`) = ''
                            THEN 'WARN: Exchange is null'
                        ELSE 'PASS'
                    END AS exchange_status,
                    CASE
                        WHEN b.market_value_fc IS NOT NULL AND b.market_value_fc != 0
                            THEN CAST(b.market_value_fc AS DECIMAL(30,8))
                        WHEN b.quantity IS NOT NULL AND p5.final_market_price IS NOT NULL
                            THEN CAST(CAST(b.quantity AS DECIMAL(30,8)) * CAST(p5.final_market_price AS DECIMAL(30,8)) AS DECIMAL(30,8))
                        ELSE CAST(NULL AS DECIMAL(30,8))
                    END AS final_market_value_fc,
                    CASE
                        WHEN b.net_book_value_fc IS NOT NULL THEN CAST(b.net_book_value_fc AS DECIMAL(30,8))
                        WHEN b.cost_fc IS NOT NULL
                            THEN CAST(CAST(b.cost_fc AS DECIMAL(30,8)) - CAST(COALESCE(b.provision_fc, CAST(0 AS DECIMAL(30,8))) AS DECIMAL(30,8)) AS DECIMAL(30,8))
                        ELSE CAST(NULL AS DECIMAL(30,8))
                    END AS final_net_book_value_fc,
                    CASE
                        WHEN p4.security_status LIKE 'FAIL: No identifier%'
                            THEN 'INVALID: No security identifier'
                        WHEN b.quantity IS NULL AND b.cost_fc IS NULL
                            THEN 'INVALID: No quantity'
                        WHEN p4.security_status = 'NOT_FOUND: Create new security'
                            THEN 'VALID: New security created'
                        WHEN p4.security_status = 'ISIN_MATCH'
                            THEN 'VALID'
                        WHEN p4.security_status LIKE 'FAIL%'
                            THEN CONCAT('INVALID: ', p4.security_status)
                        ELSE 'VALID'
                    END AS overall_status
                FROM pos_stage_1_base_{ETL_SFX} b
                JOIN pos_stage_2_portfolio_{ETL_SFX} p2
                    ON b.row_id = p2.row_id AND p2.portfolio_status = 'PASS'
                JOIN pos_stage_4_security_fallback_{ETL_SFX} p4 ON b.row_id = p4.row_id
                LEFT JOIN pos_stage_5_price_{ETL_SFX} p5 ON b.row_id = p5.row_id

                UNION ALL

                -- Portfolio-failed rows never reach pos_stage_3/4/5 (those steps
                -- INNER JOIN on portfolio_status='PASS' by design — matching a
                -- security/price for a row whose portfolio doesn't exist is
                -- meaningless work). Without this branch these rows silently
                -- vanished from position_upload_staging and therefore from the
                -- Step 7B report entirely — every ingested row must appear in
                -- the report with a PASS/FAIL status and reason, regardless of
                -- whether it made it into cis_position.
                SELECT
                    b.*,
                    p2.valid_portfolio,
                    p2.portfolio_currency,
                    p2.portfolio_status,
                    CAST(NULL AS BIGINT)          AS final_security_id,
                    CAST(NULL AS STRING)          AS matched_security_name,
                    CAST(NULL AS STRING)          AS final_isin,
                    CAST(NULL AS STRING)          AS country_resolved,
                    CAST(NULL AS STRING)          AS security_currency_resolved,
                    CAST(NULL AS STRING)          AS security_status,
                    CAST(NULL AS STRING)          AS security_match_method,
                    CAST(NULL AS DECIMAL(30,8))   AS final_market_price,
                    CAST(NULL AS STRING)          AS price_status,
                    CAST(NULL AS DECIMAL(30,8))   AS final_quantity,
                    CAST(NULL AS STRING)          AS quantity_status,
                    CAST(NULL AS DECIMAL(30,8))   AS final_shares_issued,
                    CAST(NULL AS STRING)          AS exchange_status,
                    CAST(NULL AS DECIMAL(30,8))   AS final_market_value_fc,
                    CAST(NULL AS DECIMAL(30,8))   AS final_net_book_value_fc,
                    CONCAT(
                        'INVALID: ',
                        regexp_replace(p2.portfolio_status, '^FAIL: ', '')
                    ) AS overall_status
                FROM pos_stage_1_base_{ETL_SFX} b
                JOIN pos_stage_2_portfolio_{ETL_SFX} p2
                    ON b.row_id = p2.row_id AND p2.portfolio_status != 'PASS'
                """,
                database=db
            )
            _s6_bd = _breakdown(f'position_upload_staging_{ETL_SFX}', 'overall_status')
            _s6_total = _count(f'position_upload_staging_{ETL_SFX}')
            logger.info(f"[position_etl] Step 6 complete — {_s6_total} rows in staging: {_s6_bd}")
            _s6_invalid = _count(f'position_upload_staging_{ETL_SFX}', "overall_status LIKE 'INVALID%'")
            _s6_valid   = _count(f'position_upload_staging_{ETL_SFX}', "overall_status LIKE 'VALID%'")
            if _s6_invalid > 0:
                _sample_fails(f'position_upload_staging_{ETL_SFX}', 'overall_status')
            # Set pass/fail counts here from staging — this is the authoritative source.
            # position_upload_staging is fresh and correct at this point.
            # _s7b_pass from the report may be 0 if concurrent runs wipe the staging tables
            # before Step 7B executes, so we lock these counts in now.
            result.update({
                'total':  max(_s6_total, 0),
                'passed': max(_s6_valid, 0),
                'failed': max(_s6_total - _s6_valid, 0),
            })
            _t = _step_time("Step 6 (final staging)", _t)

            # Failed records table (for reporting — records that never made it to staging)
            impala_manager.execute_write(
                f"DROP TABLE IF EXISTS position_upload_failed_{ETL_SFX}", database=db
            )
            impala_manager.execute_write(
                f"""
                CREATE TABLE position_upload_failed_{ETL_SFX}
                STORED AS PARQUET AS
                SELECT
                    b.*,
                    CASE
                        WHEN p2.portfolio_status LIKE 'FAIL%' THEN p2.portfolio_status
                        ELSE NULL
                    END AS portfolio_fail_reason,
                    CASE
                        WHEN (b.isin IS NULL OR TRIM(b.isin) = '')
                             AND (b.security_full_name IS NULL OR TRIM(b.security_full_name) = '')
                             AND (b.security_short_name IS NULL OR TRIM(b.security_short_name) = '')
                            THEN 'FAIL: No security identifier'
                        ELSE NULL
                    END AS security_fail_reason,
                    CASE
                        WHEN b.`exchange` IS NULL OR TRIM(b.`exchange`) = ''
                            THEN 'WARN: Exchange is null'
                        ELSE NULL
                    END AS exchange_fail_reason,
                    CASE
                        WHEN b.quantity IS NULL AND b.cost_fc IS NULL
                            THEN 'FAIL: No quantity'
                        ELSE NULL
                    END AS quantity_fail_reason,
                    CASE
                        WHEN p2.portfolio_status LIKE 'FAIL%' THEN 'PORTFOLIO_NOT_FOUND'
                        WHEN (b.isin IS NULL OR TRIM(b.isin) = '')
                             AND (b.security_full_name IS NULL OR TRIM(b.security_full_name) = '')
                             AND (b.security_short_name IS NULL OR TRIM(b.security_short_name) = '')
                            THEN 'NO_SECURITY_IDENTIFIER'
                        WHEN b.`exchange` IS NULL OR TRIM(b.`exchange`) = ''
                            THEN 'EXCHANGE_NULL'
                        WHEN b.quantity IS NULL AND b.cost_fc IS NULL
                            THEN 'NO_QUANTITY'
                        ELSE 'OTHER'
                    END AS fail_category
                FROM pos_stage_1_base_{ETL_SFX} b
                LEFT JOIN pos_stage_2_portfolio_{ETL_SFX} p2 ON b.row_id = p2.row_id
                WHERE p2.portfolio_status LIKE 'FAIL%'
                   OR ((b.isin IS NULL OR TRIM(b.isin) = '')
                       AND (b.security_full_name IS NULL OR TRIM(b.security_full_name) = '')
                       AND (b.security_short_name IS NULL OR TRIM(b.security_short_name) = ''))
                   OR (b.quantity IS NULL AND b.cost_fc IS NULL)
                """,
                database=db
            )
            _s6b_rows = _count(f'position_upload_failed_{ETL_SFX}')
            logger.info(f"[position_etl] Step 6B complete — {_s6b_rows} failed rows (portfolio/security reject)")
            _t = _step_time("Step 6B (failed table)", _t)

            # ------------------------------------------------------------------
            # Step 6C: Reject rows with reporting_date > contextual_today.
            #          Future-dated positions are not allowed.
            # ------------------------------------------------------------------
            from core.services.system_date_service import system_date_service as _sds_check
            _today_iso_check = _sds_check.get_system_date().isoformat()
            future_rows = impala_manager.execute_query(
                f"""
                SELECT COUNT(*) AS cnt
                FROM position_upload_staging_{ETL_SFX}
                WHERE overall_status LIKE 'VALID%'
                  AND CAST(reporting_date AS STRING) > '{_today_iso_check}'
                """,
                database=db
            )
            _future_count = int((future_rows or [{}])[0].get('cnt', 0))
            if _future_count > 0:
                logger.warning(
                    f"[position_etl] Step 6C: {_future_count} row(s) have future reporting_date "
                    f"(> contextual_today={_today_iso_check}). These rows will not be upserted."
                )
                result['future_date_rows_blocked'] = _future_count
            _t = _step_time("Step 6C", _t)

            # ------------------------------------------------------------------
            # Step 7A: UPSERT valid records into cis_position (Kudu).
            #
            # position_id is a deterministic hash of the natural key:
            #   (portfolio, security_label, position_basis, position_date, src_system)
            # Using FNV-style: abs(hash(concat(...))) cast to BIGINT.
            # This guarantees that re-running ETL for the same batch replaces
            # existing rows rather than inserting duplicates.
            #
            # position_type is always 'INT' for uploads:
            #   == today  → 'INT'  (intraday / live)
            #   <  today  → 'INT'  (backdated upload — still INT, not CORR)
            #   >  today  → blocked in pre-validation (Step 6C rejects these rows)
            # ------------------------------------------------------------------
            from core.services.system_date_service import system_date_service as _sds
            _contextual_today_iso = _sds.get_system_date().isoformat()  # 'YYYY-MM-DD'

            ok = impala_manager.execute_write(
                f"""
                UPSERT INTO {db}.cis_position (
                    position_id,
                    version_id,
                    portfolio,
                    security_label,
                    position_basis,
                    position_date,
                    src_system,
                    processing_date,
                    quantity,
                    average_cost_fc,
                    cost_fc,
                    market_value_fc,
                    net_book_value_fc,
                    unrealized_pnl_fc,
                    cost_lc,
                    market_value_lc,
                    net_book_value_lc,
                    unrealized_pnl_lc,
                    provision_lc,
                    provision_fc,
                    dividend_fc,
                    dividend_lc,
                    realized_pnl_fc,
                    realized_pnl_lc,
                    isin,
                    average_cost_lc,
                    source_table,
                    processing_timestamp,
                    uncall_fc,
                    uncall_lc,
                    pipeline_fc,
                    pipeline_lc,
                    position_type,
                    is_latest
                )
                SELECT
                    ABS(CAST(fnv_hash(CONCAT_WS('|',
                        COALESCE(s.portfolio, ''),
                        COALESCE(COALESCE(s.matched_security_name, s.security_full_name, s.security_short_name), ''),
                        COALESCE(s.position_basis, ''),
                        COALESCE(CAST(s.reporting_date AS STRING), ''),
                        COALESCE(s.src_system, '')
                    )) AS BIGINT))                          AS position_id,
                    CAST(UNIX_TIMESTAMP() * 1000 AS BIGINT) AS version_id,
                    s.portfolio,
                    COALESCE(s.matched_security_name, s.security_full_name, s.security_short_name) AS security_label,
                    s.position_basis,
                    s.reporting_date AS position_date,
                    s.src_system,
                    s.processing_date,
                    CAST(s.final_quantity          AS DECIMAL(30,8)) AS quantity,
                    {build_average_cost_sql(
                        "s.final_quantity",
                        "s.cost_fc",
                        "s.average_cost",
                    )}                                           AS average_cost_fc,
                    CAST(s.cost_fc                 AS DECIMAL(30,8)) AS cost_fc,
                    CAST(s.final_market_value_fc   AS DECIMAL(30,8)) AS market_value_fc,
                    CAST(s.final_net_book_value_fc AS DECIMAL(30,8)) AS net_book_value_fc,
                    CAST(s.unrealized_pnl_fc       AS DECIMAL(30,8)) AS unrealized_pnl_fc,
                    CAST(s.cost_lc                 AS DECIMAL(30,8)) AS cost_lc,
                    CAST(s.market_value_lc         AS DECIMAL(30,8)) AS market_value_lc,
                    CAST(s.net_book_value_lc       AS DECIMAL(30,8)) AS net_book_value_lc,
                    CAST(s.unrealized_pnl_lc       AS DECIMAL(30,8)) AS unrealized_pnl_lc,
                    CAST(s.provision_lc            AS DECIMAL(30,8)) AS provision_lc,
                    CAST(s.provision_fc            AS DECIMAL(30,8)) AS provision_fc,
                    -- CA/CF fields: carry from existing USER_UPLOAD position (never zeroed on re-upload)
                    COALESCE(ep.dividend_fc,     CAST(0 AS DECIMAL(30,8))) AS dividend_fc,
                    COALESCE(ep.dividend_lc,     CAST(0 AS DECIMAL(30,8))) AS dividend_lc,
                    COALESCE(ep.realized_pnl_fc, CAST(0 AS DECIMAL(30,8))) AS realized_pnl_fc,
                    COALESCE(ep.realized_pnl_lc, CAST(0 AS DECIMAL(30,8))) AS realized_pnl_lc,
                    COALESCE(s.final_isin, s.isin)                   AS isin,
                    {build_average_cost_sql(
                        "s.final_quantity",
                        "s.cost_lc",
                        "ep.average_cost_lc",
                    )}                                           AS average_cost_lc,
                    s.source_table                                   AS source_table,
                    from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS processing_timestamp,
                    COALESCE(ep.uncall_fc,   CAST(0 AS DECIMAL(30,8))) AS uncall_fc,
                    COALESCE(ep.uncall_lc,   CAST(0 AS DECIMAL(30,8))) AS uncall_lc,
                    COALESCE(ep.pipeline_fc, CAST(0 AS DECIMAL(30,8))) AS pipeline_fc,
                    COALESCE(ep.pipeline_lc, CAST(0 AS DECIMAL(30,8))) AS pipeline_lc,
                    'INT'                                            AS position_type,
                    true                                             AS is_latest
                FROM position_upload_staging_{ETL_SFX} s
                LEFT JOIN (
                    SELECT
                        portfolio,
                        security_label,
                        position_basis,
                        dividend_fc, dividend_lc,
                        realized_pnl_fc, realized_pnl_lc,
                        average_cost_lc,
                        uncall_fc, uncall_lc,
                        pipeline_fc, pipeline_lc
                    FROM {db}.cis_position
                    WHERE src_system = 'USER_UPLOAD'
                      AND (is_latest = true OR is_latest IS NULL)
                ) ep
                    ON ep.portfolio      = s.portfolio
                   AND ep.security_label = COALESCE(s.matched_security_name, s.security_full_name, s.security_short_name)
                   AND ep.position_basis = s.position_basis
                WHERE s.overall_status LIKE 'VALID%'
                  AND CAST(s.reporting_date AS STRING) <= '{_contextual_today_iso}'
                """,
                database=db
            )
            if not ok:
                return False, "Step 7A UPSERT INTO cis_position failed — check Impala logs for column/type mismatch", result
            # Count valid rows from staging — this is exactly the number of rows
            # written to cis_position by this upload (before carry-forward adds more dates).
            _s7a_upserted = _count(
                f'position_upload_staging_{ETL_SFX}',
                "overall_status LIKE 'VALID%'"
            )
            result['cis_position_rows'] = max(_s7a_upserted, 0)
            logger.info(f"[position_etl] Step 7A complete — {_s7a_upserted} is_latest=true rows in cis_position for this run")
            _t = _step_time("Step 7A (cis_position upsert)", _t)

            result['closed_positions'] = 0
            result['closed_positions_carried_forward'] = 0
            if reconciliation_mode == 'FULL':
                _missing_rows = self._find_authoritative_missing_positions(
                    db=db,
                    staging_table=f'position_upload_staging_{ETL_SFX}',
                )
                if _missing_rows:
                    _closed_now = self._upsert_authoritative_close_rows(
                        db=db,
                        rows=_missing_rows,
                        processing_date=processing_date,
                    )
                    result['closed_positions'] = _closed_now
                    try:
                        _hive_refresh_table('gmp_cis_sta_dly_alldatesinfo', "Authoritative carry-forward")
                    except Exception:
                        pass
                    result['closed_positions_carried_forward'] = self._carry_forward_authoritative_close_rows(
                        db=db,
                        rows=_missing_rows,
                        processing_date=processing_date,
                    )
                    logger.info(
                        f"[position_etl] Authoritative reconciliation: closed {_closed_now} missing "
                        f"position(s), carried {result['closed_positions_carried_forward']} forward"
                    )
                else:
                    logger.info("[position_etl] Authoritative reconciliation: no missing positions to close")
            else:
                logger.info("[position_etl] Partial upload mode: authoritative missing-position closure skipped")

            # ------------------------------------------------------------------
            # Step 7A2: For backdated uploads (reporting_date < today), carry the
            # uploaded position forward through all valid business dates until today,
            # stopping at each portfolio/security/basis when an existing INT
            # USER_UPLOAD record is already present for that date.
            #
            # SA rules (e.g. upload for 26-Feb, today = 2-Mar):
            #   e.g.1: No INT exists for 27-Feb or 2-Mar  → write 26, 27, 2-Mar
            #   e.g.2: INT exists for 2-Mar               → write 26, 27 only
            #   e.g.3: INT exists for 27-Feb              → write 26 only, stop
            # ------------------------------------------------------------------
            # Use calendar today (not GMP system date) so carry-forward reaches
            # all dates from upload_date through the actual current date.
            from datetime import date as _cal_date_cls
            _calendar_today_iso = _cal_date_cls.today().isoformat()  # e.g. '2026-07-04'

            _backdated_rows = impala_manager.execute_query(
                f"""
                SELECT
                    s.portfolio,
                    COALESCE(s.matched_security_name, s.security_full_name, s.security_short_name) AS security_label,
                    s.position_basis,
                    MIN(CAST(s.reporting_date AS STRING)) AS upload_date
                FROM {db}.position_upload_staging_{ETL_SFX} s
                WHERE s.overall_status LIKE 'VALID%'
                  AND CAST(s.reporting_date AS STRING) < '{_calendar_today_iso}'
                GROUP BY 1, 2, 3
                """,
                database=db
            )
            _has_backdated = bool(_backdated_rows)

            if _has_backdated:
                logger.info(
                    f"[position_etl] Step 7A2: {len(_backdated_rows)} backdated combo(s) "
                    f"(upload_date < today={_calendar_today_iso}) — carry-forward starting"
                )
                for _dbr in _backdated_rows:
                    logger.info(
                        f"[position_etl] Step 7A2: backdated combo — "
                        f"portfolio={_dbr.get('portfolio')} "
                        f"security={_dbr.get('security_label')} "
                        f"basis={_dbr.get('position_basis')} "
                        f"upload_date={_dbr.get('upload_date')}"
                    )

                _min_upload_date = min(
                    (r.get('upload_date', _calendar_today_iso) or _calendar_today_iso)
                    for r in _backdated_rows
                )

                _min_upload_date_nodash = _min_upload_date[:10].replace('-', '')
                _calendar_today_nodash  = _calendar_today_iso.replace('-', '')
                logger.info(
                    f"[position_etl] Step 7A2: querying alldatesinfo for biz dates "
                    f"{_min_upload_date_nodash} < date <= {_calendar_today_nodash}"
                )
                # Guard: REFRESH gmp_cis_sta_dly_alldatesinfo before reading —
                # it's a Hive external table; stale metadata causes NoSuchFileException.
                _hive_refresh_table('gmp_cis_sta_dly_alldatesinfo', "Step 7A2")
                _biz_dates_rows = impala_manager.execute_query(
                    f"""
                    SELECT contextual_today AS biz_date
                    FROM {db}.gmp_cis_sta_dly_alldatesinfo
                    WHERE src_system = 'gmp'
                      AND sub_system  = 'cis'
                      AND data_frq    = 'dly'
                      AND record_type = 'D'
                      AND CAST(contextual_today AS BIGINT) > {_min_upload_date_nodash}
                      AND CAST(contextual_today AS BIGINT) <= {_calendar_today_nodash}
                    ORDER BY contextual_today ASC
                    """,
                    database=db
                )
                # contextual_today may come back as an integer (e.g. 20260302) or a
                # date/string.  Normalise to ISO 'YYYY-MM-DD' in all cases.
                def _to_iso(val):
                    s = str(val).strip()[:10].replace('-', '').replace('/', '')
                    if len(s) == 8 and s.isdigit():
                        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
                    return str(val)[:10]  # already has dashes

                _biz_dates = [_to_iso(r['biz_date']) for r in (_biz_dates_rows or []) if r.get('biz_date')]
                logger.info(f"[position_etl] Step 7A2: {len(_biz_dates)} business date(s) to carry forward: {_biz_dates}")

                _carried = 0
                for row in _backdated_rows:
                    _ptf   = row.get('portfolio', '')
                    _sec   = row.get('security_label', '')
                    _basis = row.get('position_basis', 'TRADED')
                    _upload_date = (row.get('upload_date') or '')[:10]
                    # Impala uses C-style \' escaping, not doubled quotes — security
                    # names with an apostrophe (e.g. "CD INT'L ENT") produced a
                    # ParseException when escaped with '' below.
                    _ptf_esc = _ptf.replace('\\', '\\\\').replace("'", "\\'")
                    _sec_esc = _sec.replace('\\', '\\\\').replace("'", "\\'")
                    logger.info(
                        f"[position_etl] Step 7A2: carry-forward start — "
                        f"portfolio={_ptf} security={_sec} basis={_basis} from={_upload_date}"
                    )
                    if not _ptf or not _sec:
                        continue

                    # Walk forward through each business date after the upload date
                    # Stop as soon as an existing INT USER_UPLOAD record is found
                    for _biz_date in _biz_dates:
                        if _biz_date <= _upload_date:
                            continue  # skip dates on or before the upload date itself

                        _exists_rows = impala_manager.execute_query(
                            f"""
                            SELECT COUNT(*) AS cnt
                            FROM {db}.cis_position
                            WHERE portfolio       = '{_ptf_esc}'
                              AND security_label  = '{_sec_esc}'
                              AND position_basis  = '{_basis}'
                              AND src_system      = 'USER_UPLOAD'
                              AND CAST(position_date AS STRING) = '{_biz_date}'
                              AND (is_latest = true OR is_latest IS NULL)
                            """,
                            database=db
                        )
                        _date_exists = int((_exists_rows or [{}])[0].get('cnt', 0)) > 0

                        if _date_exists:
                            logger.info(
                                f"[position_etl] Step 7A2: existing INT found for "
                                f"{_ptf}/{_sec}/{_basis} on {_biz_date} — stopping carry-forward"
                            )
                            break  # stop walking forward for this portfolio/security/basis

                        # No existing row — carry the most recent position forward to this date
                        _carry_ok = impala_manager.execute_write(
                            f"""
                            UPSERT INTO {db}.cis_position (
                                position_id,
                                version_id,
                                portfolio,
                                security_label,
                                position_basis,
                                position_date,
                                src_system,
                                processing_date,
                                quantity,
                                average_cost_fc,
                                cost_fc,
                                market_value_fc,
                                net_book_value_fc,
                                unrealized_pnl_fc,
                                cost_lc,
                                market_value_lc,
                                net_book_value_lc,
                                unrealized_pnl_lc,
                                provision_lc,
                                provision_fc,
                                dividend_fc,
                                dividend_lc,
                                realized_pnl_fc,
                                realized_pnl_lc,
                                isin,
                                average_cost_lc,
                                source_table,
                                processing_timestamp,
                                uncall_fc,
                                uncall_lc,
                                pipeline_fc,
                                pipeline_lc,
                                position_type,
                                is_latest
                            )
                            SELECT
                                ABS(CAST(fnv_hash(CONCAT_WS('|',
                                    COALESCE(portfolio, ''),
                                    COALESCE(security_label, ''),
                                    COALESCE(position_basis, ''),
                                    '{_biz_date}',
                                    COALESCE(src_system, '')
                                )) AS BIGINT))                          AS position_id,
                                CAST(UNIX_TIMESTAMP() * 1000 AS BIGINT) AS version_id,
                                portfolio,
                                security_label,
                                position_basis,
                                '{_biz_date}'                           AS position_date,
                                src_system,
                                '{processing_date}'                     AS processing_date,
                                quantity,
                                average_cost_fc,
                                cost_fc,
                                market_value_fc,
                                net_book_value_fc,
                                unrealized_pnl_fc,
                                cost_lc,
                                market_value_lc,
                                net_book_value_lc,
                                unrealized_pnl_lc,
                                provision_lc,
                                provision_fc,
                                dividend_fc,
                                dividend_lc,
                                realized_pnl_fc,
                                realized_pnl_lc,
                                isin,
                                average_cost_lc,
                                source_table,
                                from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS processing_timestamp,
                                uncall_fc,
                                uncall_lc,
                                pipeline_fc,
                                pipeline_lc,
                                'INT'                                   AS position_type,
                                true                                    AS is_latest
                            FROM {db}.cis_position
                            WHERE portfolio       = '{_ptf_esc}'
                              AND security_label  = '{_sec_esc}'
                              AND position_basis  = '{_basis}'
                              AND src_system      = 'USER_UPLOAD'
                              AND CAST(position_date AS STRING) < '{_biz_date}'
                              AND (is_latest = true OR is_latest IS NULL)
                            ORDER BY position_date DESC
                            LIMIT 1
                            """,
                            database=db
                        )
                        if _carry_ok:
                            _carried += 1
                            logger.info(
                                f"[position_etl] Step 7A2: carried {_ptf}/{_sec}/{_basis} "
                                f"forward to {_biz_date}"
                            )
                        else:
                            logger.warning(
                                f"[position_etl] Step 7A2: carry-forward failed for "
                                f"{_ptf}/{_sec}/{_basis} on {_biz_date}"
                            )

                logger.info(
                    f"[position_etl] Step 7A2 complete — {_carried} date(s) carried forward "
                    f"across {len(_backdated_rows)} combo(s)"
                )
                result['today_positions_carried_forward'] = _carried
            else:
                logger.info("[position_etl] Step 7A2: no backdated rows — carry-forward skipped")
            _t = _step_time("Step 7A2 (carry-forward)", _t)

            # ------------------------------------------------------------------
            # Step 7B: INSERT OVERWRITE into the existing external partitioned
            #          table gmp_cis.position_upload_report.
            #          Columns match the DDL in 25_position_upload_standardized.sql.
            #          Only the (processing_date, src_id) partition is overwritten;
            #          all other partitions (other runs) are preserved.
            # ------------------------------------------------------------------
            # Step 7B reads from position_upload_staging (which already contains all
            # columns from pos_stage_1_base via b.* plus all status columns from the
            # joins in Step 6). This avoids re-joining pos_stage_1_base here and
            # ensures the report always has exactly the rows that reached final staging.
            # Portfolio-fail / security-fail rows that were excluded from staging are
            # captured in position_upload_failed and reported via fail_reason below.
            impala_manager.execute_write(
                f"""
                INSERT OVERWRITE {db}.position_upload_report
                (
                    portfolio,
                    security_full_name,
                    security_short_name,
                    row_status,
                    fail_reason,
                    portfolio_status,
                    security_status,
                    security_match_method,
                    price_status,
                    quantity_status,
                    exchange_status,
                    matched_security_id,
                    matched_security_name,
                    isin,
                    ticker,
                    quantity,
                    shares_outstanding,
                    shares_issued,
                    pct_holding,
                    market_price,
                    average_cost,
                    cost_fc,
                    market_value_fc,
                    net_book_value_fc,
                    unrealized_pnl_fc,
                    provision_fc,
                    cost_lc,
                    market_value_lc,
                    net_book_value_lc,
                    unrealized_pnl_lc,
                    provision_lc,
                    product_type,
                    security_type,
                    quoted_unquoted,
                    industry,
                    fin_nonfin_co,
                    issuer_type,
                    reits_or_fund_y_n,
                    `exchange`,
                    country_of_exchange,
                    country_of_incorporation,
                    country_of_risk,
                    country_of_operation,
                    security_currency,
                    corp_code,
                    branch_code,
                    cost_centre,
                    cels,
                    bwcif_sg,
                    bwcif_ovs,
                    mas_6d_code_sg,
                    mas_6d_code_ovs,
                    position_basis,
                    reporting_date,
                    maturity_date,
                    src_system,
                    source_table
                )
                PARTITION (processing_date='{processing_date}', src_id='{src_id}')
                -- Explicit column list above maps each SELECT expression by name
                -- to its target column, immune to the SELECT's isin position
                -- (after matched_security_name, not after security_short_name)
                -- not matching the live table's physical column order (isin
                -- right after security_short_name, per DDL 81). Without this
                -- list, INSERT OVERWRITE mapped positionally against the table
                -- schema and silently shifted every column from isin through
                -- ticker by one slot — e.g. fail_reason's value (NULL for a
                -- PASS row) landed in the row_status column.
                SELECT
                    s.portfolio,
                    COALESCE(s.security_full_name, s.security_short_name, s.isin) AS security_full_name,
                    s.security_short_name,
                    CASE
                        WHEN s.overall_status LIKE 'INVALID%' THEN 'FAIL'
                        WHEN s.overall_status LIKE 'VALID%'   THEN 'PASS'
                        ELSE 'FAIL'
                    END AS row_status,
                    CASE
                        WHEN s.overall_status LIKE 'INVALID%' THEN s.overall_status
                        ELSE NULL
                    END AS fail_reason,
                    s.portfolio_status,
                    s.security_status,
                    s.security_match_method,
                    s.price_status,
                    s.quantity_status,
                    s.exchange_status,
                    CAST(s.final_security_id AS STRING) AS matched_security_id,
                    s.matched_security_name,
                    s.isin,
                    s.ticker,
                    s.quantity,
                    s.shares_outstanding,
                    s.shares_issued,
                    s.pct_holding,
                    s.market_price,
                    s.average_cost,
                    s.cost_fc,
                    s.market_value_fc,
                    s.net_book_value_fc,
                    s.unrealized_pnl_fc,
                    s.provision_fc,
                    s.cost_lc,
                    s.market_value_lc,
                    s.net_book_value_lc,
                    s.unrealized_pnl_lc,
                    s.provision_lc,
                    s.product_type,
                    s.security_type,
                    s.quoted_unquoted,
                    s.industry,
                    s.fin_nonfin_co,
                    s.issuer_type,
                    s.reits_or_fund_y_n,
                    s.`exchange`                AS `exchange`,
                    s.country_of_exchange,
                    s.country_of_incorporation,
                    s.country_of_risk,
                    s.country_of_operation,
                    s.security_currency,
                    s.corp_code,
                    s.branch_code,
                    s.cost_centre,
                    s.cels,
                    s.bwcif_sg,
                    s.bwcif_ovs,
                    s.mas_6d_code_sg,
                    s.mas_6d_code_ovs,
                    s.position_basis,
                    s.reporting_date,
                    s.maturity_date,
                    s.src_system,
                    s.source_table
                FROM position_upload_staging_{ETL_SFX} s
                """,
                database=db
            )
            impala_manager.execute_write(
                f"REFRESH {db}.position_upload_report PARTITION (processing_date='{processing_date}', src_id='{src_id}')",
                database=db
            )
            _s7b_total = _count(f'{db}.position_upload_report', f"processing_date='{processing_date}' AND src_id='{src_id}'")
            _s7b_pass  = _count(f'{db}.position_upload_report', f"processing_date='{processing_date}' AND src_id='{src_id}' AND row_status='PASS'")
            _s7b_fail  = _s7b_total - _s7b_pass
            logger.info(
                f"[position_etl] Step 7B complete — report rows: {_s7b_total} total / "
                f"{_s7b_pass} PASS / {_s7b_fail} FAIL"
            )
            if _s7b_fail > 0:
                try:
                    _fail_reasons = impala_manager.execute_query(
                        f"""
                        SELECT fail_reason, COUNT(*) AS n
                        FROM {db}.position_upload_report
                        WHERE processing_date='{processing_date}' AND src_id='{src_id}'
                          AND row_status='FAIL'
                        GROUP BY fail_reason ORDER BY n DESC LIMIT 10
                        """,
                        database=db
                    )
                    for r in (_fail_reasons or []):
                        logger.warning(
                            f"[position_etl] Step 7B fail_reason: '{r.get('fail_reason')}' × {r.get('n')} row(s)"
                        )
                except Exception:
                    pass
            _t = _step_time("Step 7B (report write)", _t)

            # ------------------------------------------------------------------
            # Count totals — Step 6 already locked in the authoritative
            # total/passed/failed from position_upload_staging (see comment
            # above that assignment). Do NOT let Step 7B's re-count overwrite
            # it: position_upload_report is a shared partition that a
            # concurrent second ETL run for the same src_id/processing_date
            # can rewrite between Step 6 and here, which previously caused
            # this run's correct Step 6 counts to be silently replaced with
            # the other run's numbers (or zeros), producing a description
            # ("750/751 → cis_position") that disagreed with the live upload
            # detail page's stat panel (re-queries position_upload_report at
            # view time and would show whichever run's data landed last).
            # Only use Step 7B's numbers as a fallback if Step 6 never ran.
            # ------------------------------------------------------------------
            if result.get('total') is not None:
                pass  # Step 6 already set total/passed/failed — keep it authoritative
            elif _s7b_total >= 0:
                result.update({
                    'total':  _s7b_total,
                    'passed': _s7b_pass,
                    'failed': _s7b_fail,
                })
            else:
                rows = impala_manager.execute_query(
                    f"""
                    SELECT
                        COUNT(*) AS total,
                        SUM(CASE WHEN row_status = 'PASS' THEN 1 ELSE 0 END) AS passed,
                        SUM(CASE WHEN row_status = 'FAIL' THEN 1 ELSE 0 END) AS failed
                    FROM {db}.position_upload_report
                    WHERE src_id = '{src_id}'
                      AND processing_date = '{processing_date}'
                    """,
                    database=db
                )
                if rows:
                    result.update({
                        'total':  rows[0].get('total', 0),
                        'passed': rows[0].get('passed', 0),
                        'failed': rows[0].get('failed', 0),
                    })
                else:
                    result.update({'total': 0, 'passed': 0, 'failed': 0})

            # Clean up intermediate staging tables (keep report + failed for UI)
            _cleanup_staging_tables()

            _total_elapsed = _etl_time.time() - _etl_t0
            msg = (
                f"Position ETL complete for {src_id} / {processing_date}: "
                f"{result.get('total', 0)} rows — "
                f"{result.get('passed', 0)} PASS, {result.get('failed', 0)} FAIL "
                f"(total {_total_elapsed:.1f}s)"
            )
            logger.info(f"[position_etl] {msg}")
            notify_user(updated_by, EVT_UPLOAD_COMPLETED, {
                **_notif_base,
                'total':  result.get('total', 0),
                'passed': result.get('passed', 0),
                'failed': result.get('failed', 0),
                'message': msg,
            })
            return True, msg, result

        except EtlCancelled as e:
            logger.info(f"[position_etl] Cancelled by user: {e}")
            try:
                _cleanup_staging_tables()
            except Exception as _cleanup_ex:
                logger.warning(f"[position_etl] Cleanup after cancel failed: {_cleanup_ex}")
            result['cancelled'] = True
            msg = f"Position ETL cancelled by user: {e}"
            notify_user(updated_by, EVT_UPLOAD_FAILED, {
                **_notif_base,
                'message': f'Position ETL cancelled for {src_id}',
            })
            return False, msg, result

        except Exception as e:
            logger.error(f"[position_etl] Error: {e}", exc_info=True)
            err_msg = f"Position ETL error: {e}"
            notify_user(updated_by, EVT_UPLOAD_FAILED, {
                **_notif_base,
                'error':   str(e)[:500],
                'message': f'Position ETL failed for {src_id}: {str(e)[:200]}',
            })
            notify_admins(EVT_UPLOAD_FAILED, {
                **_notif_base,
                'triggered_by': updated_by,
                'error':        str(e)[:500],
                'message':      f'Upload ETL failure: src_id={src_id} user={updated_by} — {str(e)[:200]}',
            })
            return False, err_msg, result

        finally:
            unregister_etl_run(upload_id)

    def get_position_report(
        self,
        src_id: str,
        processing_date: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch all rows from position_upload_report for a given partition.

        Args:
            src_id: Source partition value
            processing_date: YYYYMMDD partition value

        Returns:
            List of row dicts
        """
        from core.repositories.impala_connection import impala_manager

        try:
            rows = impala_manager.execute_query(
                f"""
                SELECT
                    portfolio                   AS portfolio_name,
                    security_full_name,
                    security_short_name,
                    isin,
                    row_status,
                    fail_reason,
                    portfolio_status,
                    security_status,
                    security_match_method,
                    price_status,
                    quantity_status,
                    exchange_status,
                    matched_security_id,
                    matched_security_name,
                    ticker,
                    quantity,
                    shares_outstanding,
                    shares_issued,
                    pct_holding,
                    market_price,
                    average_cost,
                    cost_fc,
                    market_value_fc,
                    net_book_value_fc,
                    unrealized_pnl_fc,
                    provision_fc,
                    cost_lc,
                    market_value_lc,
                    net_book_value_lc,
                    unrealized_pnl_lc,
                    provision_lc,
                    product_type,
                    security_type,
                    quoted_unquoted,
                    industry,
                    fin_nonfin_co,
                    issuer_type,
                    reits_or_fund_y_n,
                    `exchange`,
                    country_of_exchange,
                    country_of_incorporation,
                    country_of_risk,
                    country_of_operation,
                    security_currency,
                    corp_code,
                    branch_code,
                    cost_centre,
                    cels,
                    bwcif_sg,
                    bwcif_ovs,
                    mas_6d_code_sg,
                    mas_6d_code_ovs,
                    position_basis,
                    reporting_date,
                    maturity_date,
                    src_system,
                    source_table
                FROM gmp_cis.position_upload_report
                WHERE src_id = '{src_id}'
                  AND processing_date = '{processing_date}'
                ORDER BY row_status, portfolio, security_full_name
                """,
            )
            return rows or []
        except Exception as e:
            logger.error(f"[position_report] Query error: {e}")
            return []

    def build_position_report_csv(
        self,
        src_id: str,
        processing_date: str
    ) -> Tuple[bool, str, str]:
        """
        Build CSV content from position_upload_report for a given partition.

        Returns:
            Tuple of (success, message, csv_string)
        """
        rows = self.get_position_report(src_id, processing_date)
        if not rows:
            return False, "No report data found for this upload partition", ""

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
        return True, f"{len(rows)} rows", output.getvalue()
