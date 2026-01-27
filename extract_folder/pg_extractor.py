import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gc
import logging
import pandas as pd
import psycopg2
import boto3
import awswrangler as wr

from datetime import datetime
from utils import clean_column_names, add_metadata, EXECUTION_DATE, DEST_BUCKET
from utils import ssm_client_2
from dotenv import load_dotenv

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

session_dest = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION", "eu-north-1"),
)

session_source = boto3.Session(profile_name="source", region_name="eu-north-1")


def get_db_credentials_from_ssm():
    """
    Retrieve Postgres credentials from AWS SSM Parameter Store.
    """
    try:
        params = {
            "host": ssm_client_2.get_parameter(
                Name="/coretelecomms/database/db_host", WithDecryption=True
            )["Parameter"]["Value"],
            "port": ssm_client_2.get_parameter(
                Name="/coretelecomms/database/db_port", WithDecryption=True
            )["Parameter"]["Value"],
            "database": ssm_client_2.get_parameter(
                Name="/coretelecomms/database/db_name", WithDecryption=True
            )["Parameter"]["Value"],
            "user": ssm_client_2.get_parameter(
                Name="/coretelecomms/database/db_username", WithDecryption=True
            )["Parameter"]["Value"],
            "password": ssm_client_2.get_parameter(
                Name="/coretelecomms/database/db_password", WithDecryption=True
            )["Parameter"]["Value"],
        }

        logger.info("-" * 80)
        logger.info("Retrieved DB credentials from SSM")
        logger.info("-" * 80)
        return params
    except Exception as e:
        logger.info(f"Failed to get SSM parameters: {e}............................")
        raise


def extract_web_forms(
    table_name_path="web_forms", exec_date="2025-11-23", chunk_size=50_000
):
    """
    Query Postgres for web form table for the given execution date.
    """

    if exec_date is None:
        exec_date = EXECUTION_DATE
    elif isinstance(exec_date, int):
        date_str = str(exec_date)
        exec_date = datetime.strptime(date_str, "%Y%m%d").date()
    elif isinstance(exec_date, str):
        if "-" in exec_date:
            exec_date = datetime.strptime(exec_date, "%Y-%m-%d").date()
        else:
            exec_date = datetime.strptime(exec_date, "%Y%m%d").date()

    table_name = f"web_form_request_{exec_date.strftime('%Y_%m_%d')}"

    logger.info(
        f"...................... Extracting Web Forms from Postgres table: {table_name}......................"
    )

    query = f"SELECT * FROM customer_complaints.{table_name}"

    total_rows = 0
    conn = None
    try:
        db_config = get_db_credentials_from_ssm()
        conn = psycopg2.connect(**db_config)

        chunk_iter = pd.read_sql(query, conn, chunksize=chunk_size)

        for chunk_df in chunk_iter:
            chunk_df = clean_column_names(chunk_df)
            chunk_df = add_metadata(chunk_df, "web_forms")

            total_rows += len(chunk_df)
            logger.info(
                f"Wrote chunk with {len(chunk_df)} rows so far: {total_rows} ......................."
            )

            if exec_date:
                chunk_df = chunk_df.copy()
                chunk_df["ingestion_date"] = exec_date

            path = f"s3://{DEST_BUCKET}/staging/{table_name_path}/"
            wr.s3.to_parquet(
                df=chunk_df,
                path=path,
                boto3_session=session_dest,
                dataset=True,
                mode="append",
                partition_cols=["ingestion_date"],
                compression="snappy",
            )
            logger.info(
                f"Successfully wrote {len(chunk_df)} rows to {path}..................."
            )

            del chunk_df
            gc.collect()

        logger.info(
            f"Loaded {total_rows} web form records from {table_name} in our Data Lake (s3)"
        )
        return total_rows
    except Exception as e:
        logger.exception(
            f"********************* Failed to extract web forms for {exec_date}: {e} ******************"
        )
        raise
    finally:
        if conn:
            conn.close()
