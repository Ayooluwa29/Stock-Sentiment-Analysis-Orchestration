import gzip
import csv
import logging
from pathlib import Path
from config import ensure_directory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_line(line):
    """Parse a single line from pageviews file."""
    parts = line.strip().split()
    if len(parts) < 3:
        return None
    
    domain = parts[0]
    page_title = parts[1]
    view_count = parts[2]
    
    return {
        'domain': domain,
        'page_title': page_title,
        'view_count': int(view_count)
    }


def extract_and_parse(input_path, output_path, domain_filter='en', force_extract=False):
    """Extract gzip and parse relevant data."""
    input_path = Path(input_path)
    output_path = Path(output_path)
    
    # Ensure directory exists
    ensure_directory(output_path)
    
    if output_path.exists() and not force_extract:
        logger.info(f"Extracted file already exists: {output_path}, skipping extraction")
        return output_path
    
    logger.info(f"Extracting and parsing {input_path}...")
    
    with gzip.open(input_path, 'rt', encoding='utf-8') as gz_file:
        with open(output_path, 'w', newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['page_title', 'view_count'])
            
            count = 0
            for line in gz_file:
                data = parse_line(line)
                
                if data and data['domain'].startswith(domain_filter):
                    writer.writerow([data['page_title'], data['view_count']])
                    count += 1
                    
                    if count % 100000 == 0:
                        logger.info(f"Processed {count} lines...")
            
            logger.info(f"Extraction complete. Total lines: {count}")
    
    return output_path