import duckdb
import uuid
import os
import re
import math
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
    Handles common date formats including M/D/YYYY used in US datasets.
    """
    schema = con.execute('DESCRIBE data').fetchdf()
    columns = schema['column_name'].tolist()
    types = schema['column_type'].tolist()

    for col, dtype in zip(columns, types):
        if dtype != 'VARCHAR':
            continue

        safe_col = col.replace('"', '""')

        # Try M/D/YYYY format (e.g. Superstore, US government datasets)
        try:
            result = con.execute(f"""
                SELECT COUNT(*) FROM data
                WHERE "{safe_col}" IS NOT NULL
                AND TRY_STRPTIME("{safe_col}", '%m/%d/%Y') IS NULL
                AND "{safe_col}" != ''
            """).fetchone()[0]
            total = con.execute(f"""
                SELECT COUNT(*) FROM data
                WHERE "{safe_col}" IS NOT NULL AND "{safe_col}" != ''
            """).fetchone()[0]
            if total > 0 and result / total < 0.05:
                con.execute(f"""
                    ALTER TABLE data ALTER "{safe_col}" TYPE TIMESTAMP
                    USING TRY_STRPTIME("{safe_col}", '%m/%d/%Y')
                """)
                continue
        except Exception:
            pass

        # Try YYYY-MM-DD format (ISO standard, most common)
        try:
            result = con.execute(f"""
                SELECT COUNT(*) FROM data
                WHERE "{safe_col}" IS NOT NULL
                AND TRY_STRPTIME("{safe_col}", '%Y-%m-%d') IS NULL
                AND "{safe_col}" != ''
            """).fetchone()[0]
            total = con.execute(f"""
                SELECT COUNT(*) FROM data
                WHERE "{safe_col}" IS NOT NULL AND "{safe_col}" != ''
            """).fetchone()[0]
            if total > 0 and result / total < 0.05:
                con.execute(f"""
                    ALTER TABLE data ALTER "{safe_col}" TYPE TIMESTAMP
                    USING TRY_STRPTIME("{safe_col}", '%Y-%m-%d')
                """)
                continue
        except Exception:
            pass

        # Try DD/MM/YYYY format (European datasets)
        try:
            result = con.execute(f"""
                SELECT COUNT(*) FROM data
                WHERE "{safe_col}" IS NOT NULL
                AND TRY_STRPTIME("{safe_col}", '%d/%m/%Y') IS NULL
                AND "{safe_col}" != ''
            """).fetchone()[0]
            total = con.execute(f"""
                SELECT COUNT(*) FROM data
                WHERE "{safe_col}" IS NOT NULL AND "{safe_col}" != ''
            """).fetchone()[0]
            if total > 0 and result / total < 0.05:
                con.execute(f"""
                    ALTER TABLE data ALTER "{safe_col}" TYPE TIMESTAMP
                    USING TRY_STRPTIME("{safe_col}", '%d/%m/%Y')
                """)
                continue
        except Exception:
            pass

        # Try MM-DD-YYYY format
        try:
            result = con.execute(f"""
                SELECT COUNT(*) FROM data
                WHERE "{safe_col}" IS NOT NULL
                AND TRY_STRPTIME("{safe_col}", '%m-%d-%Y') IS NULL
                AND "{safe_col}" != ''
            """).fetchone()[0]
            total = con.execute(f"""
                SELECT COUNT(*) FROM data
                WHERE "{safe_col}" IS NOT NULL AND "{safe_col}" != ''
            """).fetchone()[0]
            if total > 0 and result / total < 0.05:
                con.execute(f"""
                    ALTER TABLE data ALTER "{safe_col}" TYPE TIMESTAMP
                    USING TRY_STRPTIME("{safe_col}", '%m-%d-%Y')
                """)
                continue
        except Exception:
            pass

        # Try YYYY-MM-DD HH:MM:SS format (datetime with time)
        try:
            result = con.execute(f"""
                SELECT COUNT(*) FROM data
                WHERE "{safe_col}" IS NOT NULL
                AND TRY_STRPTIME("{safe_col}", '%Y-%m-%d %H:%M:%S') IS NULL
                AND "{safe_col}" != ''
            """).fetchone()[0]
            total = con.execute(f"""
                SELECT COUNT(*) FROM data
                WHERE "{safe_col}" IS NOT NULL AND "{safe_col}" != ''
            """).fetchone()[0]
            if total > 0 and result / total < 0.05:
                con.execute(f"""
                    ALTER TABLE data ALTER "{safe_col}" TYPE TIMESTAMP
                    USING TRY_STRPTIME("{safe_col}", '%Y-%m-%d %H:%M:%S')
                """)
                continue
        except Exception:
            pass

        # Try generic TIMESTAMP cast
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
            if total > 0 and result / total < 0.05:
                con.execute(
                    f'ALTER TABLE data ALTER "{safe_col}" TYPE TIMESTAMP USING TRY_CAST("{safe_col}" AS TIMESTAMP)')
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
    Clean up common AI-generated SQL issues for DuckDB compatibility.
    """
    # Replace backticks with double quotes
    sql = sql.replace('`', '"')

    # Replace TO_DATE(col, format) -> TRY_STRPTIME(col, format)
    sql = re.sub(
        r'TO_DATE\s*\(\s*([^,]+),\s*([^)]+)\)',
        lambda m: f"TRY_STRPTIME({m.group(1).strip()}, {m.group(2).strip()})",
        sql, flags=re.IGNORECASE
    )

    # Replace TO_TIMESTAMP(col, format) -> TRY_STRPTIME(col, format)
    sql = re.sub(
        r'TO_TIMESTAMP\s*\(\s*([^,]+),\s*([^)]+)\)',
        lambda m: f"TRY_STRPTIME({m.group(1).strip()}, {m.group(2).strip()})",
        sql, flags=re.IGNORECASE
    )

    # Replace STR_TO_DATE(col, format) -> TRY_STRPTIME(col, format)
    sql = re.sub(
        r'STR_TO_DATE\s*\(\s*([^,]+),\s*([^)]+)\)',
        lambda m: f"TRY_STRPTIME({m.group(1).strip()}, {m.group(2).strip()})",
        sql, flags=re.IGNORECASE
    )

    # Fix MONTHNAME(col) / MONTH(col) / YEAR(col) / DAY(col) on VARCHAR columns
    sql = re.sub(
        r'(MONTHNAME|MONTH|YEAR|DAY|DAYNAME|QUARTER|WEEK)\s*\(\s*("[\w\s]+"|\w+)\s*\)',
        lambda m: f"{m.group(1)}(TRY_CAST({m.group(2)} AS TIMESTAMP))",
        sql, flags=re.IGNORECASE
    )

    # Fix EXTRACT(... FROM "column") -> EXTRACT(... FROM TRY_CAST("column" AS TIMESTAMP))
    sql = re.sub(
        r'EXTRACT\s*\(\s*(\w+)\s+FROM\s+("?\w[\w\s]*"?)\s*\)',
        lambda m: f'EXTRACT({m.group(1)} FROM TRY_CAST({m.group(2)} AS TIMESTAMP))',
        sql
    )

    # Fix date_part('...', "column") -> date_part('...', TRY_CAST("column" AS TIMESTAMP))
    sql = re.sub(
        r'date_part\s*\(\s*(\'[^\']+\')\s*,\s*("?\w[\w\s]*"?)\s*\)',
        lambda m: f'date_part({m.group(1)}, TRY_CAST({m.group(2)} AS TIMESTAMP))',
        sql
    )

    # Fix strftime("column", '...')
    sql = re.sub(
        r'strftime\s*\(\s*("?\w[\w\s]*"?)\s*,\s*(\'[^\']+\')\s*\)',
        lambda m: f'strftime(TRY_CAST({m.group(1)} AS TIMESTAMP), {m.group(2)})',
        sql
    )
    sql = re.sub(r'\bCAST\s*\(', 'TRY_CAST(', sql, flags=re.IGNORECASE)
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

        # If date function failed, try wrapping date columns in TRY_CAST
        if 'date_part' in error_msg or 'EXTRACT' in error_msg or 'strftime' in error_msg:
            sql_fixed = re.sub(
                r'(date_part\s*\([^,]+,\s*)(\w+)(\s*\))',
                lambda m: m.group(0).replace(
                    m.group(2), f"TRY_CAST({m.group(2)} AS TIMESTAMP)"),
                sql
            )
            sql_fixed = re.sub(
                r'(EXTRACT\s*\(\s*\w+\s+FROM\s+)(\w+)(\s*\))',
                lambda m: m.group(0).replace(
                    m.group(2), f"TRY_CAST({m.group(2)} AS TIMESTAMP)"),
                sql_fixed
            )
            sql_fixed = re.sub(
                r"(strftime\s*\('[^']+',\s*)(\w+)(\s*\))",
                lambda m: m.group(0).replace(
                    m.group(2), f"TRY_CAST({m.group(2)} AS TIMESTAMP)"),
                sql_fixed
            )
            try:
                df = con.execute(sql_fixed).fetchdf()
            except Exception:
                raise ValueError(f"Query failed: {error_msg}")

        elif 'No function matches' in error_msg or 'Binder Error' in error_msg:
            raise ValueError(f"Query failed: {error_msg}")

        else:
            raise ValueError(f"Query failed: {error_msg}")

    # Convert all values to JSON-safe types, handling NaN and Infinity
    result = []
    for record in df.to_dict(orient='records'):
        clean = {}
        for k, v in record.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                clean[k] = None
            else:
                clean[k] = str(v) if v is not None else None
        result.append(clean)
    return result
