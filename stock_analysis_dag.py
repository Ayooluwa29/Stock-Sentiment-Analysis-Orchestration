from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pendulum
from pathlib import Path
from downloader import build_url, download_file
from extract import extract_and_identify
from db_loader import create_and_truncate_table, load_csv_to_db

# Get Airflow Variables
BASE_URL = Variable.get("wiki_url", default_var="https://dumps.wikimedia.org/other/pageviews/")
YEAR = Variable.get("year", default_var="2024")
MONTH = Variable.get("month", default_var="12")
DAY = Variable.get("day", default_var="15")
HOUR = Variable.get("hour", default_var="100000")
DATA_DATE = Variable.get("data_date", default_var="2024-12-15")
DATA_HOUR = Variable.get("data_hour", default_var="10:00:00")
POSTGRES_CONN = Variable.get("postgres_conn", default_var="postgres_default")

# Paths
DATA_DIR = Path("data")
DOWNLOAD_PATH = DATA_DIR / f"pageviews-{YEAR}{MONTH}{DAY}-{HOUR}.gz"
COMPANIES_PATH = DATA_DIR / f"companies-{YEAR}{MONTH}{DAY}-{HOUR}.csv"

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2024, 12, 31, tz='UTC'),
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

with DAG(
    'wikipedia_pageviews_analysis',
    default_args=default_args,
    description='Download, extract, identify companies, and load to database',
    schedule='None',
    catchup=False,
    tags=['wikipedia', 'pageviews'],
) as dag:
    
    def download_task():
        """Download Wikipedia pageviews data"""
        url = build_url(BASE_URL, YEAR, MONTH, DAY, HOUR)
        download_file(url, DOWNLOAD_PATH)
    
    def extract_and_identify_task():
        """Extract and identify companies in one step"""
        extract_and_identify(DOWNLOAD_PATH, COMPANIES_PATH)
    
    def create_truncate_task():
        """Create table if not exists and truncate"""
        create_and_truncate_table(POSTGRES_CONN)
    
    def load_task():
        """Load companies to database"""
        load_csv_to_db(COMPANIES_PATH, DATA_DATE, DATA_HOUR, POSTGRES_CONN)
    
    # Define tasks
    download = PythonOperator(
        task_id='download',
        python_callable=download_task
    )
    
    extract_identify = PythonOperator(
        task_id='extract_and_identify',
        python_callable=extract_and_identify_task
    )
    
    create_truncate = PythonOperator(
        task_id='create_and_truncate',
        python_callable=create_truncate_task
    )
    
    load_db = PythonOperator(
        task_id='load_to_database',
        python_callable=load_task
    )
    
    # Define workflow
    download >> extract_identify >> create_truncate >> load_db