import duckdb
import uuid
import os

SESSIONS = {}


def load_csv(file_bytes: bytes, filename: str) -> dict:
    session_id = str(uuid.uuid4())
    con = duckdb.connect()

    tmp = f'/tmp/{session_id}.csv'
    with open(tmp, 'wb') as f:
        f.write(file_bytes)

    # all_varchar=true prevents date/timestamp parsing issues
    con.execute(
        f"CREATE TABLE data AS SELECT * FROM read_csv_auto('{tmp}', ignore_errors=true, all_varchar=true)")

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


def run_query(session_id: str, sql: str):
    con = SESSIONS.get(session_id)
    if not con:
        raise ValueError('Session expired. Please re-upload your file.')
    # DuckDB uses double quotes, not backticks — fix AI-generated SQL
    sql = sql.replace('`', '"')
    df = con.execute(sql).fetchdf()
    return df.astype(str).where(df.notna(), None).to_dict(orient='records')
