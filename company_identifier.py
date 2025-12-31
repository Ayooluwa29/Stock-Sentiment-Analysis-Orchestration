import csv
import logging
from airflow.sdk import Variable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_company_list():
    """Fetch company list from Airflow Variable."""
    # Get the list from Airflow Variable (automatically deserializes JSON)
    companies = Variable.get("company_list", deserialize_json=True)
    logger.info(f"Loaded {len(companies)} companies from Airflow Variable")
    return set(companies)


def identify_companies(csv_path):
    """Filter pageviews for known companies."""
    known_companies = get_company_list()
    companies = []
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            page = row['page_title']
            if page in known_companies:
                companies.append({
                    'company': page,
                    'views': int(row['view_count'])
                })
    
    logger.info(f"Found {len(companies)} companies in pageviews")
    return companies