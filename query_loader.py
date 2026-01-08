from pathlib import Path

SQL_DIR = Path(__file__).parent / 'sql'

def load_query(filename):
    sql_path = SQL_DIR / filename
    with open(sql_path, 'r', encoding='utf-8') as f:
        return f.read().strip()

CREATE_AND_TRUNCATE = load_query('create_and_truncate.sql')