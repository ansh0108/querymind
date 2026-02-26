import sqlite3
from datetime import datetime

DB_PATH = 'history.db'


def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute('''
        CREATE TABLE IF NOT EXISTS history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id  TEXT,
            question    TEXT,
            sql         TEXT,
            summary     TEXT,
            row_count   INTEGER,
            chart_type  TEXT,
            timestamp   TEXT
        )
    ''')
    con.commit()


def save_query(session_id, question, sql, summary, row_count, chart_type):
    con = sqlite3.connect(DB_PATH)
    con.execute(
        'INSERT INTO history VALUES (NULL,?,?,?,?,?,?,?)',
        (session_id, question, sql, summary,
         row_count, chart_type, datetime.now().isoformat())
    )
    con.commit()


def get_history(session_id, limit=20):
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        'SELECT * FROM history WHERE session_id=? ORDER BY timestamp DESC LIMIT ?',
        (session_id, limit)
    ).fetchall()
    keys = ['id', 'session_id', 'question', 'sql',
            'summary', 'row_count', 'chart_type', 'timestamp']
    return [dict(zip(keys, r)) for r in rows]
