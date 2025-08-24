#!/usr/bin/env python3
"""
CLI ingestion script for Daily Hill CSV files.

Key principles:
- Keep ingestion fast and pure (vectorized via Polars)
- Centralize categorization rules for auditability
- Maintain DB schema compatibility and deduplication

Usage: python ingest.py /path/to/dailyhill.csv
"""

import sys
import polars as pl
import duckdb
from pathlib import Path
from datetime import datetime
import re
import time


def detect_header_row(file_path: str) -> int:
    """Detect the row containing the actual headers (Date (YYYY/MM/DD))."""
    with open(file_path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if 'Date (YYYY/MM/DD)' in line:
                return i
    return 0


def detect_delimiter(file_path: str) -> str:
    """Crude delimiter detection among tab, comma, semicolon, pipe.

    Looks at the first few non-empty lines and picks the delimiter with the
    most occurrences. Defaults to comma if tie/none.
    """
    candidates = ['\t', ',', ';', '|']
    counts = {c: 0 for c in candidates}
    lines_checked = 0
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            if not line.strip():
                continue
            for c in candidates:
                counts[c] += line.count(c)
            lines_checked += 1
            if lines_checked >= 5:
                break
    # Pick the delimiter with highest count
    delim = max(counts, key=lambda k: counts[k])
    # If nothing stands out, default to comma
    if counts[delim] == 0:
        return ','
    return delim


def normalize_column_names(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize column names and drop unnamed columns.

    Makes the ingest resilient to small header changes by:
    - Dropping 'Unnamed' columns
    - Trimming whitespace and normalizing case for matching
    - Mapping common header variants to canonical names
    - Falling back to regex heuristics (e.g., any column starting with 'date')
    """
    # Drop unnamed columns
    cols_to_keep = [col for col in df.columns if not col.strip().startswith('Unnamed')]
    df = df.select(cols_to_keep)

    # Build a lookup using normalized keys
    def norm(s: str) -> str:
        return (
            s.strip()
             .replace("\u200b", "")  # zero-width
             .replace("\ufeff", "")  # BOM
             .lower()
        )

    normalized = {norm(c): c for c in df.columns}

    def exists(key_variants: list[str]) -> str | None:
        for k in key_variants:
            if k in normalized:
                return normalized[k]
        return None

    # Map known variants
    mapping: dict[str, str] = {}

    # Date columns
    date_col = exists([
        'date (yyyy/mm/dd)', 'date (yyyy-mm-dd)', 'date', 'date (dd/mm/yyyy)', 'date (mm/dd/yyyy)'
    ])
    if not date_col:
        # Heuristic: any column that starts with 'date'
        for k, orig in normalized.items():
            if k.startswith('date'):
                date_col = orig
                break
    if date_col:
        mapping[date_col] = 'date_raw'

    # Simple helpers
    def map_if(found: str | None, to: str):
        if found:
            mapping[found] = to

    map_if(exists(['shift name', 'shiftname']), 'shift_name')
    map_if(exists(['shift type', 'shifttype']), 'shift_type')
    map_if(exists(['shift start (hh:mm)', 'shift start', 'shift start (hhmm)', 'shiftstart']), 'shift_start')
    map_if(exists(['shift end (hh:mm)', 'shift end', 'shift end (hhmm)', 'shiftend']), 'shift_end')
    map_if(exists(['staff first name', 'first name', 'firstname']), 'first_name')
    map_if(exists(['staff last name', 'last name', 'lastname']), 'last_name')
    map_if(exists(['staff id', 'staffid', 'staff_id']), 'staff_id')
    map_if(exists(['payroll id', 'payrollid', 'payroll_id']), 'payroll_id')
    map_if(exists(['priority ranking', 'priority', 'priorityranking']), 'priority_ranking')
    map_if(exists(['task name', 'taskname']), 'task_name')
    map_if(exists(['task type', 'tasktype']), 'task_type')
    map_if(exists(['task start (hh:mm)', 'task start', 'task start (hhmm)', 'taskstart']), 'task_start')
    map_if(exists(['task end (hh:mm)', 'task end', 'task end (hhmm)', 'taskend']), 'task_end')
    map_if(exists(['task duration', 'taskduration']), 'task_duration')
    map_if(exists(['comments', 'comment']), 'comments')
    map_if(exists(['private guest name', 'private guest', 'guest name']), 'private_guest_name')
    map_if(exists(['is request private', 'request private', 'is private']), 'is_request_private')
    map_if(exists(['private guest note', 'guest note', 'private note']), 'private_guest_note')

    if mapping:
        df = df.rename(mapping)

    return df


class TaskCategorizer:
    """Centralized categorization rules.

    Exposes rule constants that are used by vectorized expressions
    in `derive_fields()` to keep logic consistent across ingest and backfill.
    """

    # Levels we consider as Lessons for high-level category aggregation
    LESSON_LEVELS = {
        '1st Time', 'Novice', 'Beginner', 'Intermediate', 'Advanced',
        'Freestyle', 'Big Carpet', 'Little Carpet', 'Private'
    }

    # Keywords that indicate Kids age band (besides explicit Program)
    KIDS_TOKENS = [' KD ', ' KD', '- KD', 'Kids', 'Youth', 'Lowriders', 'Skiwees']


def derive_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Add derived fields according to business rules.

    Notes on performance:
    - We compute a lowercase version of `task_name_clean` once (tn_lower)
      to avoid repeating `.str.to_lowercase()` for every condition.
    - We keep everything as vectorized Polars expressions for speed on 200k+ rows.
    """
    
    # Build a cleaned task name that falls back to task_type when task_name is invalid (e.g., 'a' or empty)
    df = df.with_columns([
        pl.when(
            pl.col('task_name').is_null() |
            (pl.col('task_name').cast(pl.Utf8).str.len_chars() <= 1) |
            (pl.col('task_name').cast(pl.Utf8).str.to_lowercase() == 'a')
        ).then(pl.col('task_type').cast(pl.Utf8))
        .otherwise(pl.col('task_name').cast(pl.Utf8))
        .alias('task_name_clean')
    ])

    # Create lowercase helper once for many contains checks
    df = df.with_columns([
        pl.col('task_name_clean').str.to_lowercase().alias('tn_lower')
    ])

    df = df.with_columns([
        # Basic derived fields
        (pl.col('first_name').fill_null('').cast(pl.Utf8) + ' ' + pl.col('last_name').fill_null('').cast(pl.Utf8)).alias('instructor'),
        (pl.col('task_type') != 'Non Teaching').alias('is_teaching'),
        
        # Date parsing - defensive with multiple formats
        pl.coalesce([
            pl.col('date_raw').cast(pl.Utf8).str.strptime(pl.Date, format='%d/%m/%Y', strict=False),
            pl.col('date_raw').cast(pl.Utf8).str.strptime(pl.Date, format='%Y-%m-%d', strict=False),
            pl.col('date_raw').cast(pl.Utf8).str.strptime(pl.Date, format='%Y/%m/%d', strict=False),
            pl.col('date_raw').cast(pl.Utf8).str.strptime(pl.Date, format='%m/%d/%Y', strict=False),
        ]).alias('date'),
        
        # Time parsing - defensive and flexible
        # Normalize whitespace and '.' to ':' before parsing; try multiple formats
        pl.coalesce([
            pl.col('task_start').cast(pl.Utf8).str.strip_chars().str.replace_all(r"\.", ":").str.strptime(pl.Time, format='%H:%M', strict=False),
            pl.col('task_start').cast(pl.Utf8).str.strip_chars().str.strptime(pl.Time, format='%H.%M', strict=False),
            pl.col('task_start').cast(pl.Utf8).str.strip_chars().str.strptime(pl.Datetime, format='%Y-%m-%d %H:%M', strict=False).dt.time(),
        ]).alias('start_time'),
        pl.coalesce([
            pl.col('task_end').cast(pl.Utf8).str.strip_chars().str.replace_all(r"\.", ":").str.strptime(pl.Time, format='%H:%M', strict=False),
            pl.col('task_end').cast(pl.Utf8).str.strip_chars().str.strptime(pl.Time, format='%H.%M', strict=False),
            pl.col('task_end').cast(pl.Utf8).str.strip_chars().str.strptime(pl.Datetime, format='%Y-%m-%d %H:%M', strict=False).dt.time(),
        ]).alias('end_time'),
        
        # Combine relevant free-text fields for inference (lowercased)
        (
            (pl.col('task_name_clean').fill_null('').cast(pl.Utf8) + ' ' +
             pl.col('comments').fill_null('').cast(pl.Utf8) + ' ' +
             pl.col('private_guest_note').fill_null('').cast(pl.Utf8) + ' ' +
             pl.col('private_guest_name').fill_null('').cast(pl.Utf8)
            ).str.to_lowercase()
        ).alias('notes_lower'),
        
        # Age band logic (Program defaults to Kids)
        pl.when(
            pl.any_horizontal(*[
                pl.col('task_name_clean').str.contains(tok) for tok in TaskCategorizer.KIDS_TOKENS
            ]) | pl.col('task_type').cast(pl.Utf8).str.contains('Program')
        ).then(pl.lit('Kids')).otherwise(pl.lit('Adults')).alias('age_band'),
        
        # Level categorization (order matters!) — prioritize specific labels first
        # Meet & Greet (including Level Lead and variants)
        pl.when(
            pl.col('tn_lower').str.contains('meet and greet') |
            pl.col('tn_lower').str.contains('meet & greet') |
            pl.col('tn_lower').str.contains('m&g') |
            pl.col('tn_lower').str.contains('m & g') |
            pl.col('tn_lower').str.contains('level lead')
        ).then(pl.lit('Meet & Greet'))
        # Training
        .when(pl.col('tn_lower').str.contains('training')).then(pl.lit('Training'))
        # Operational tasks like Base Area Set Up//Down, setup, pack up/down
        .when(
            pl.col('tn_lower').str.contains('base area set up') |
            pl.col('tn_lower').str.contains('base area setup') |
            pl.col('tn_lower').str.contains('base area set down') |
            pl.col('tn_lower').str.contains('set up//down') |
            pl.col('tn_lower').str.contains('packup') |
            pl.col('tn_lower').str.contains('pack down') |
            pl.col('tn_lower').str.contains('packdown') |
            pl.col('tn_lower').str.contains('pack up') |
            pl.col('tn_lower').str.contains('setup') |
            pl.col('tn_lower').str.contains('set up')
        ).then(pl.lit('Fencing/Setup'))
        # Showed Up / Available to teach
        .when(
            pl.col('tn_lower').str.contains('available') |
            pl.col('tn_lower').str.contains('showed up')
        ).then(pl.lit('Showed Up'))
        # 1st time learner
        .when(
            pl.col('tn_lower').str.contains('1st time') |
            pl.col('tn_lower').str.contains('first time')
        ).then(pl.lit('1st Time'))
        # Specific carpets categories
        .when(pl.col('tn_lower').str.contains('big carpet'))
        .then(pl.lit('Big Carpet'))
        .when(pl.col('tn_lower').str.contains('little carpet'))
        .then(pl.lit('Little Carpet'))
        .when(pl.col('tn_lower').str.contains('novice'))
        .then(pl.lit('Novice'))
        .when(pl.col('tn_lower').str.contains('intermediate'))
        .then(pl.lit('Intermediate'))
        .when(pl.col('tn_lower').str.contains('advanced'))
        .then(pl.lit('Advanced'))
        .when(pl.col('tn_lower').str.contains('beginner'))
        .then(pl.lit('Beginner'))
        .when(pl.col('tn_lower').str.contains('freestyle'))
        .then(pl.lit('Freestyle'))
        # Broad fallbacks
        .when(pl.col('task_type').cast(pl.Utf8).str.contains('Non Teaching')).then(pl.lit('Non Teaching'))
        .when(pl.col('task_type').cast(pl.Utf8).str.contains('Private')).then(pl.lit('Private'))
        .otherwise(pl.lit('Other'))
        .alias('level')
    ])

    # Infer numeric age from notes for Private lessons
    df = df.with_columns([
        # Try patterns like '12yo', '12 yo', '12 y/o', '12 yrs', '12 years'
        pl.coalesce([
            pl.col('notes_lower').str.extract(r"\b(\d{1,2})\s*(?:y/?o|yo|yrs?|years?|yr)\b", 1),
            pl.col('notes_lower').str.extract(r"\b(?:age|aged)\s*(\d{1,2})\b", 1)
        ]).cast(pl.Int64).alias('age_inferred')
    ])

    # Override age_band for Private when age is inferred
    df = df.with_columns([
        pl.when(
            pl.col('task_type').cast(pl.Utf8).str.contains('Private') & pl.col('age_inferred').is_not_null()
        ).then(
            pl.when(pl.col('age_inferred') < 16).then(pl.lit('Kids')).otherwise(pl.lit('Adults'))
        ).otherwise(pl.col('age_band')).alias('age_band')
    ])

    # Ability hint from notes (kept separate so level can remain 'Private')
    df = df.with_columns([
        pl.when(pl.col('notes_lower').str.contains('1st time') | pl.col('notes_lower').str.contains('first time')).then(pl.lit('1st Time'))
        .when(pl.col('notes_lower').str.contains('novice')).then(pl.lit('Novice'))
        .when(pl.col('notes_lower').str.contains('beginner')).then(pl.lit('Beginner'))
        .when(pl.col('notes_lower').str.contains('intermediate')).then(pl.lit('Intermediate'))
        .when(pl.col('notes_lower').str.contains('advanced')).then(pl.lit('Advanced'))
        .when(pl.col('notes_lower').str.contains('freestyle')).then(pl.lit('Freestyle'))
        .otherwise(None).alias('ability_hint')
    ])

    # Derive high-level task_category for filtering (Lesson vs explicit non-lesson buckets)
    df = df.with_columns([
        pl.when(pl.col('level').is_in(list(TaskCategorizer.LESSON_LEVELS))).then(pl.lit('Lesson'))
        .when(pl.col('level') == 'Fencing/Setup').then(pl.lit('Fencing/Setup'))
        .when(pl.col('level') == 'Showed Up').then(pl.lit('Showed Up'))
        .when(pl.col('level') == 'Meet & Greet').then(pl.lit('Meet & Greet'))
        .when(pl.col('level') == 'Training').then(pl.lit('Training'))
        .when(pl.col('task_type').cast(pl.Utf8).str.contains('Non Teaching')).then(pl.lit('Non Teaching'))
        .otherwise(pl.lit('Other')).alias('task_category')
    ])

    # Add week number and booking_id
    df = df.with_columns([
        pl.col('date').dt.week().alias('week'),
        (
            pl.col('date').dt.strftime('%Y-%m-%d') + '|' +
            pl.col('staff_id').cast(pl.Utf8).fill_null('') + '|' +
            pl.col('task_start').cast(pl.Utf8).fill_null('') + '-' +
            pl.col('task_end').cast(pl.Utf8).fill_null('') + '|' +
            pl.col('task_name_clean').fill_null('')
        ).alias('booking_id')
    ])
    
    return df


def setup_database(db_path: str = 'flaik.duckdb', retries: int = 5, delay_seconds: float = 0.5):
    """Initialize DuckDB database and create tables with simple retry on Windows file lock."""
    last_err = None
    for attempt in range(retries):
        try:
            conn = duckdb.connect(db_path)
            break
        except duckdb.IOException as e:
            msg = str(e).lower()
            # Windows can hold a hard lock if another Python process has the file open
            if ('file is already open' in msg) or ('being used by another process' in msg):
                last_err = e
                if attempt < retries - 1:
                    time.sleep(delay_seconds * (attempt + 1))  # simple backoff
                    continue
            # Non-lock errors or exhausted retries
            raise
    else:
        # Exhausted retries
        raise last_err if last_err else duckdb.IOException("Failed to open DuckDB after retries")

    # Create bookings table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bookings (
            date_raw VARCHAR,
            shift_name VARCHAR,
            shift_type VARCHAR,
            shift_start VARCHAR,
            shift_end VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            staff_id VARCHAR,
            payroll_id VARCHAR,
            priority_ranking VARCHAR,
            task_name VARCHAR,
            task_type VARCHAR,
            task_start VARCHAR,
            task_end VARCHAR,
            task_duration VARCHAR,
            comments VARCHAR,
            private_guest_name VARCHAR,
            is_request_private VARCHAR,
            private_guest_note VARCHAR,
            instructor VARCHAR,
            is_teaching BOOLEAN,
            date DATE,
            start_time TIME,
            end_time TIME,
            age_band VARCHAR,
            level VARCHAR,
            task_category VARCHAR,
            week INTEGER,
            booking_id VARCHAR UNIQUE,
            age_inferred INTEGER,
            ability_hint VARCHAR
        )
    """)
    # Ensure task_category exists for older schemas
    try:
        conn.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS task_category VARCHAR")
        conn.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS age_inferred INTEGER")
        conn.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS ability_hint VARCHAR")
    except Exception:
        pass
    
    # Create unique index on booking_id for deduplication
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_booking_id ON bookings(booking_id)")
    
    return conn


def ingest_csv(file_path: str, db_path: str = 'flaik.duckdb', conn: duckdb.DuckDBPyConnection | None = None) -> int:
    """Ingest a CSV file into the database.
    If `conn` is provided, reuse it; otherwise, open a new connection.
    """
    print(f"Processing {file_path}...")
    
    # Detect header row and delimiter
    header_row = detect_header_row(file_path)
    print(f"Found headers at row {header_row + 1}")
    delimiter = detect_delimiter(file_path)
    human_delim = {"\t": "TAB", ",": ",", ";": ";", "|": "|"}.get(delimiter, delimiter)
    print(f"Detected delimiter: {human_delim}")
    
    # Read CSV with Polars - handle mixed data types safely
    try:
        df = pl.read_csv(
            file_path,
            skip_rows=header_row,
            try_parse_dates=False,
            ignore_errors=False,
            separator=delimiter,
            infer_schema_length=0,  # scan whole file for robust types
            quote_char='"',
            null_values=["", "NULL", "NaN"],
            truncate_ragged_lines=True,
            has_header=True,
            rechunk=True,
        )
    except Exception as e:
        print(f"Primary CSV read failed: {e}. Falling back to pandas -> polars.")
        # Fallback: read with pandas then convert
        import pandas as pd
        try:
            df_pandas = pd.read_csv(
                file_path,
                skiprows=header_row,
                dtype=str,
                sep=delimiter if delimiter != "\t" else "\t",
                engine='python',
                quoting=0,
                on_bad_lines='skip'
            )
        except Exception as e2:
            print(f"Pandas read also failed: {e2}")
            raise
        df = pl.from_pandas(df_pandas)
    print(f"Read {len(df)} rows and {len(df.columns)} columns")
    
    # Normalize and derive fields
    df = normalize_column_names(df)
    df = derive_fields(df)
    
    # Setup database (reuse connection if provided)
    external_conn = conn is not None
    if not external_conn:
        conn = setup_database(db_path)
    
    # Insert data with deduplication
    try:
        # Convert to pandas for DuckDB insert
        df_pandas = df.to_pandas()

        # Ensure columns align with table schema across uploads
        table_columns = [
            'date_raw','shift_name','shift_type','shift_start','shift_end',
            'first_name','last_name','staff_id','payroll_id','priority_ranking',
            'task_name','task_type','task_start','task_end','task_duration',
            'comments','private_guest_name','is_request_private','private_guest_note',
            'instructor','is_teaching','date','start_time','end_time','age_band',
            'level','task_category','week','booking_id',
            'age_inferred','ability_hint'
        ]

        # Add any missing columns as None and order consistently
        for col in table_columns:
            if col not in df_pandas.columns:
                df_pandas[col] = None
        df_pandas = df_pandas[table_columns]
        
        # Get count before insert
        before_count = conn.execute("SELECT COUNT(*) FROM bookings").fetchone()[0]
        
        # Insert matching columns by name and ignore conflicts on unique booking_id
        conn.execute("INSERT INTO bookings BY NAME SELECT * FROM df_pandas ON CONFLICT DO NOTHING")
        
        # Get count after insert
        after_count = conn.execute("SELECT COUNT(*) FROM bookings").fetchone()[0]
        
        rows_inserted = after_count - before_count
        print(f"Inserted {rows_inserted} new rows (total: {after_count})")
        
        if not external_conn:
            conn.close()
        return rows_inserted
        
    except Exception as e:
        print(f"Error inserting data: {e}")
        if not external_conn and conn is not None:
            conn.close()
        return 0


def main():
    if len(sys.argv) != 2:
        print("Usage: python ingest.py /path/to/dailyhill.csv")
        sys.exit(1)
    
    file_path = sys.argv[1]
    if not Path(file_path).exists():
        print(f"File not found: {file_path}")
        sys.exit(1)
    
    rows_inserted = ingest_csv(file_path)
    print(f"Ingestion complete. {rows_inserted} rows added.")


if __name__ == '__main__':
    main()
