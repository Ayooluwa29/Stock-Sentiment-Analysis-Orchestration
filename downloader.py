import requests
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_url(base_url, year, month, day, hour):
    """Construct the Wikipedia pageviews URL."""
    filename = f"pageviews-{year}{month}{day}-{hour}.gz"
    return f"{base_url}{year}/{year}-{month}/{filename}"


def download_file(url, save_path):
    """Download file from URL."""
    save_path = Path(save_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    
    if save_path.exists():
        logger.info(f"File already exists: {save_path}")
        return save_path
    
    logger.info(f"Downloading from {url}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    with open(save_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    logger.info(f"Downloaded to {save_path}")
    return save_path