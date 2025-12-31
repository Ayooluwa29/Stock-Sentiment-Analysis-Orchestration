import requests
import logging
from pathlib import Path
from config import BASE_URL, YEAR, MONTH, DAY, HOUR, ensure_directory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_url():
    """Construct the Wikipedia pageviews URL."""
    filename = f"pageviews-{YEAR}{MONTH}{DAY}-{HOUR}.gz"
    return f"{BASE_URL}{YEAR}/{YEAR}-{MONTH}/{filename}"


def download_file(url, save_path, force_download=False):
    """Download file from URL to local path."""
    save_path = Path(save_path)
    
    # Ensure directory exists
    ensure_directory(save_path)
    
    if save_path.exists() and not force_download:
        logger.info(f"File already exists: {save_path}, skipping download")
        return save_path
    
    logger.info(f"Downloading from {url}")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    with open(save_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    logger.info(f"Downloaded to {save_path}")
    return save_path