import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import os
import logging
import pandas as pd
import gspread

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from utils import clean_column_names, add_metadata

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format="{asctime} - {levelname} - {message}",
                    style="{",
                    datefmt="%Y-%m-%d %H:%M",
                    filename="process_etl.log",
                    encoding='utf-8',
                    filemode="a")
logger = logging.getLogger(__name__)


def extract_agents():
    """
        Extract agents data from a Google Sheet and return DataFrame.
    """
    logger.info(".................... Extracting Agents from Google Sheets ......................")

    service_account = os.getenv("SERVICE_ACCOUNT")
    sheet_id = os.getenv("SHEET_ID")
    sheet_name = os.getenv("SHEET_NAME")

    try:
        creds = Credentials.from_service_account_file(service_account, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
        client = gspread.authorize(creds)
        sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        df = clean_column_names(df)
        df = add_metadata(df, "agents")

        logger.info(f"......................... Loaded {len(df)} agents.....................")
        return df
    except Exception as e:
        logger.warning(f"******************* Failed to extract agents: {e} *********************")
        raise

if __name__ == "__main__":
    extract_agents()
