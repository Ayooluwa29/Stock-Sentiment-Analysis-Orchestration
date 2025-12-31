from pathlib import Path
import logging

BASE_URL = "https://dumps.wikimedia.org/other/pageviews/"
YEAR = "2025"
MONTH = "12"
DAY = "15"
HOUR = "100000"  # Format: HHMMSS (10:00:00)

# Define paths using pathlib
DATA_DIR = Path("data")
DOWNLOAD_PATH = DATA_DIR / "pageviews.gz"
EXTRACTED_PATH = DATA_DIR / "pageviews.csv"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ensure_directory(file_path):
    """Create directory for file path if it doesn't exist."""
    directory = Path(file_path).parent
    if not directory.exists():
        directory.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory: {directory}")
    else:
        logger.info(f"Directory already exists: {directory}")