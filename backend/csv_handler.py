import duckdb
import uuid
import os
import re

SESSIONS = {}  # session_id -> DuckDB connection


def load_csv(file_bytes: bytes, filename: str) -> dict:
    session_id = str(uuid.uuid4())
    con = duckdb.connect()

    tmp = f'/tmp/{session_id}.csv'
    with open(tmp, 'wb') as f:
        f.write(file_bytes)

    # Load as all varchar first to avoid encoding/type issues
    con.execute(
        f"CREATE TABLE data AS SELECT * FROM read_csv_auto('{tmp}', ignore_errors=true, all_varchar=true)")

    # Now auto-detect and cast date/numeric columns
    _auto_cast_columns(con)

    schema = con.execute('DESCRIBE data').fetchdf().to_dict(orient='records')
    sample = con.execute(
        'SELECT * FROM data LIMIT 3').fetchdf().to_dict(orient='records')
    row_count = con.execute('SELECT COUNT(*) FROM data').fetchone()[0]

    # Convert sample to plain strings so JSON serialization never fails
    clean_sample = []
    for row in sample:
        clean_row = {}
        for k, v in row.items():
            clean_row[k] = str(v) if v is not None else None
        clean_sample.append(clean_row)

    SESSIONS[session_id] = con
    os.remove(tmp)

    return {
        'session_id': session_id,
        'schema':     schema,
        'sample':     clean_sample,
        'row_count':  row_count,
    }


def _auto_cast_columns(con):
    """
    Try to cast VARCHAR columns to proper types (TIMESTAMP, DATE, DOUBLE, BIGINT).
    This makes date functions and numeric aggregations work on any dataset.
    """
    schema = con.execute('DESCRIBE data').fetchdf()
    columns = schema['column_name'].tolist()
    types = schema['column_type'].tolist()

    for col, dtype in zip(columns, types):
        if dtype != 'VARCHAR':
            continue

        safe_col = col.replace('"', '""')

        # Try TIMESTAMP first
        try:
            result = con.execute(f"""
                SELECT COUNT(*) FROM data
                WHERE "{safe_col}" IS NOT NULL
                AND TRY_CAST("{safe_col}" AS TIMESTAMP) IS NULL
                AND "{safe_col}" != ''
            """).fetchone()[0]
            total = con.execute(f"""
                SELECT COUNT(*) FROM data
                WHERE "{safe_col}" IS NOT NULL AND "{safe_col}" != ''
            """).fetchone()[0]
            if total > 0 and result / total < 0.05:  # >95% parse as timestamp
                con.execute(
                    f'ALTER TABLE data ALTER "{safe_col}" TYPE TIMESTAMP USING TRY_CAST("{safe_col}" AS TIMESTAMP)')
                continue
        except Exception:
            pass

        # Try DATE
        try:
            result = con.execute(f"""
                SELECT COUNT(*) FROM data
                WHERE "{safe_col}" IS NOT NULL
                AND TRY_CAST("{safe_col}" AS DATE) IS NULL
                AND "{safe_col}" != ''
            """).fetchone()[0]
            total = con.execute(f"""
                SELECT COUNT(*) FROM data
                WHERE "{safe_col}" IS NOT NULL AND "{safe_col}" != ''
            """).fetchone()[0]
            if total > 0 and result / total < 0.05:
                con.execute(
                    f'ALTER TABLE data ALTER "{safe_col}" TYPE DATE USING TRY_CAST("{safe_col}" AS DATE)')
                continue
        except Exception:
            pass

        # Try DOUBLE
        try:
            result = con.execute(f"""
                SELECT COUNT(*) FROM data
                WHERE "{safe_col}" IS NOT NULL
                AND TRY_CAST("{safe_col}" AS DOUBLE) IS NULL
                AND "{safe_col}" != ''
            """).fetchone()[0]
            total = con.execute(f"""
                SELECT COUNT(*) FROM data
                WHERE "{safe_col}" IS NOT NULL AND "{safe_col}" != ''
            """).fetchone()[0]
            if total > 0 and result / total < 0.05:
                con.execute(
                    f'ALTER TABLE data ALTER "{safe_col}" TYPE DOUBLE USING TRY_CAST("{safe_col}" AS DOUBLE)')
                continue
        except Exception:
            pass

        # Try BIGINT
        try:
            result = con.execute(f"""
                SELECT COUNT(*) FROM data
                WHERE "{safe_col}" IS NOT NULL
                AND TRY_CAST("{safe_col}" AS BIGINT) IS NULL
                AND "{safe_col}" != ''
            """).fetchone()[0]
            total = con.execute(f"""
                SELECT COUNT(*) FROM data
                WHERE "{safe_col}" IS NOT NULL AND "{safe_col}" != ''
            """).fetchone()[0]
            if total > 0 and result / total < 0.05:
                con.execute(
                    f'ALTER TABLE data ALTER "{safe_col}" TYPE BIGINT USING TRY_CAST("{safe_col}" AS BIGINT)')
        except Exception:
            pass


def _fix_sql(sql: str) -> str:
    """
    Clean up common AI-generated SQL issues:
    - Replace backticks with double quotes
    - Fix common DuckDB syntax issues
    """
    # Replace backticks with double quotes
    sql = sql.replace('`', '"')

    # Fix double-double-quotes that sometimes appear
    sql = re.sub(r'""([^"]+)""', r'"\1"', sql)

    return sql


def run_query(session_id: str, sql: str):
    con = SESSIONS.get(session_id)
    if not con:
        raise ValueError('Session expired. Please re-upload your file.')

    sql = _fix_sql(sql)

    try:
        df = con.execute(sql).fetchdf()
    except Exception as e:
        error_msg = str(e)

        # If date function failed, try wrapping suspected date columns in CAST
        if 'date_part' in error_msg or 'EXTRACT' in error_msg or 'strftime' in error_msg:
            # Find all column names used with date functions and cast them
            sql_fixed = re.sub(
                r'(date_part\s*\([^,]+,\s*)(\w+)(\s*\))',
                r'\1CAST(\2 AS TIMESTAMP)\3',
                sql
            )
            sql_fixed = re.sub(
                r'(EXTRACT\s*\(\s*\w+\s+FROM\s+)(\w+)(\s*\))',
                r'\1CAST(\2 AS TIMESTAMP)\3',
                sql_fixed
            )
            sql_fixed = re.sub(
                r"(strftime\s*\('[^']+',\s*)(\w+)(\s*\))",
                r'\1CAST(\2 AS TIMESTAMP)\3',
                sql_fixed
            )
            try:
                df = con.execute(sql_fixed).fetchdf()
            except Exception:
                raise ValueError(f"Query failed: {error_msg}")

        # If numeric function failed on VARCHAR, try casting to DOUBLE
        elif 'No function matches' in error_msg or 'Binder Error' in error_msg:
            raise ValueError(f"Query failed: {error_msg}")

        else:
            raise ValueError(f"Query failed: {error_msg}")

    # Convert all values to JSON-safe types
    return df.astype(str).where(df.notna(), None).to_dict(orient='records')
