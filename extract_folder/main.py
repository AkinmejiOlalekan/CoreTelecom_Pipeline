import logging

from dotenv import load_dotenv
from utils import EXECUTION_DATE, write_to_s3_parquet, SOURCE_BUCKET, DEST_BUCKET
from s3_extractor import extract_customers, extract_call_logs, extract_social_media
from gsheet_extractor import extract_agents
from pg_extractor import extract_web_forms

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    filename="process_etl.log",
    encoding="utf-8",
    filemode="a",
)
logger = logging.getLogger(__name__)

TRACKER_FILE = f"s3://{DEST_BUCKET}/metadata/processed_source_files.json"


def run_full_pipeline(execution_date=None):
    exec_date = execution_date or EXECUTION_DATE
    logger.info("=" * 97)
    logger.info(
        "- ------------------------ CORETELECOMS UNIFIED DATA PLATFORM -------------------------"
    )
    logger.info("=" * 80)
    logger.info(f"Execution Date: {exec_date}")
    logger.info(f"Source: s3://{SOURCE_BUCKET}/")
    logger.info(f"Destination: s3://{DEST_BUCKET}/raw/")
    logger.info(f"Tracker: {TRACKER_FILE}")
    logger.info("=" * 80)

    # Static data
    customers_records = extract_customers()
    logger.info(f"------------------------ Customer data: {customers_records} rows loaded -----------------------------")

    df_agents = extract_agents()
    if not df_agents.empty:
        write_to_s3_parquet(df_agents, "agents", mode='overwrite')

    # Daily data
    df_call_logs = extract_call_logs()
    if not df_call_logs.empty:
        write_to_s3_parquet(df_call_logs, "call_logs", EXECUTION_DATE)

    df_social_media = extract_social_media()
    if not df_social_media.empty:
        write_to_s3_parquet(df_social_media, "social_media", EXECUTION_DATE)

    rows_written = extract_web_forms(table_name_path="web_forms", exec_date=2025_11_20)
    logger.info(f"Web forms streaming wrote {rows_written} rows")

    logger.info("\n" + "=" * 80)
    logger.info(
        "------------------------------ PIPELINE COMPLETED SUCCESSFULLY ------------------------------"
    )
    logger.info("=" * 80)


if __name__ == "__main__":
    run_full_pipeline()
