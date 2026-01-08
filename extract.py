import gzip
import csv
import logging
from pathlib import Path
from airflow.sdk import Variable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_company_list():
    """Get company list from Airflow Variable."""
    try:
        companies = Variable.get("company_list", deserialize_json=True)
        return set(companies)
    except:
        # Fallback list
        return {
            'Apple_Inc.', 'Microsoft', 'Google', 'Amazon.com', 'Facebook'
        }


def extract_and_identify(input_path, output_path, domain_filter='en'):
    """Extract gzip, filter for companies, and save to CSV."""
    known_companies = get_company_list()
    companies = {}
    
    logger.info(f"Extracting and identifying companies from {input_path}...")
    
    # Read and filter in one pass
    with gzip.open(input_path, 'rt', encoding='utf-8') as gz_file:
        for line in gz_file:
            parts = line.strip().split()
            if len(parts) < 3:
                continue
            
            domain = parts[0]
            page_title = parts[1]
            view_count = int(parts[2])
            
            # Filter for English Wikipedia and known companies
            if domain.startswith(domain_filter) and page_title in known_companies:
                companies[page_title] = view_count
    
    # Sort by views and save to CSV
    sorted_companies = sorted(companies.items(), key=lambda x: x[1], reverse=True)
    
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['company', 'views'])
        writer.writerows(sorted_companies)
    
    logger.info(f"Found {len(sorted_companies)} companies. Top: {sorted_companies[0][0]} with {sorted_companies[0][1]:,} views")
    return output_path