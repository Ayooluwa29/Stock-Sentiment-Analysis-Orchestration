import csv
import logging
from pathlib import Path
from airflow.providers.postgres.hooks.postgres import PostgresHook
from query_loader import CREATE_AND_TRUNCATE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_and_truncate_table(postgres_conn_id):
    """Create table if not exists and truncate it."""
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    pg_hook.run(CREATE_AND_TRUNCATE)
    logger.info("Table created and truncated")


def load_csv_to_db(csv_path, data_date, data_hour, postgres_conn_id):
    """Load company data from CSV to PostgreSQL."""
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append((
                row['company'],
                int(row['views']),
                data_date,
                data_hour
            ))
    
    pg_hook.insert_rows(
        table='company_pageviews',
        rows=rows,
        target_fields=['company_name', 'view_count', 'data_date', 'data_hour']
    )
    
    logger.info(f"Loaded {len(rows)} companies. Top company: {rows[0][0]} with {rows[0][1]:,} views")
    return len(rows)