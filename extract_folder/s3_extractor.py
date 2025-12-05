import os
import gc
import json
import logging
import boto3
from datetime import datetime
import pandas as pd
import awswrangler as wr

from utils import SOURCE_BUCKET, EXECUTION_DATE, DEST_BUCKET
from utils import (
    clean_column_names,
    add_metadata,
    get_new_source_files,
    mark_source_files_as_processed,
    write_to_s3_parquet,
)
from dotenv import load_dotenv

load_dotenv()

session_dest = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION", "eu-north-1"),
)

session_source = boto3.Session(
    aws_access_key_id=os.getenv("SOURCE_AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("SOURCE_AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION", "eu-north-1"),
)


s3_client = session_source.client("s3")
ssm_client = session_source.client("ssm")

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


def _list_files(prefix, suffixes=None):
    """
    Return list of keys in SOURCE_BUCKET under prefix that match suffixes.
    """
    response = s3_client.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=prefix)
    contents = response.get("Contents", []) or []
    if suffixes:
        return [
            obj["Key"]
            for obj in contents
            if any(obj["Key"].endswith(s) for s in suffixes)
        ]
    else:
        return [obj["Key"] for obj in contents]


def _read_csv_from_s3(key, chunk_size=50_000):
    try:
        obj = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=key)

        for chunk in pd.read_csv(obj["Body"], chunksize=chunk_size, low_memory=False):
            yield chunk

    except Exception as e:
        logger.error(
            f"---------------------- Error reading {key}: {e} ------------------------"
        )
        raise


def extract_customers(chunk_size=200_000):
    """
    Extract customer CSVs from S3 and return a cleaned DataFrame.
    """
    logger.info(
        "[1/3]: ....................... Extracting Customers from S3 ......................"
    )
    prefix = "customers/"
    path = f"s3://{DEST_BUCKET}/staging/customers/"

    new_files = get_new_source_files(prefix, ".csv")
    if not new_files:
        logger.warning(
            "************ No new customer files found in the bucket to process ***********"
        )
        return pd.DataFrame()

    total_rows = 0

    for file_info in new_files:
        logger.info(f"----------------------- Processing new file: {file_info['key']}")

        for chunk_num, chunk in enumerate(
            _read_csv_from_s3(file_info["key"], chunk_size=chunk_size), 1
        ):
            chunk = clean_column_names(chunk)
            chunk = add_metadata(chunk, "customers")

            wr.s3.to_parquet(
                df=chunk,
                path=path,
                dataset=True,
                mode="append",
                partition_cols=["ingestion_date"],
                compression="snappy",
                boto3_session=session_dest,
            )

            row_count = len(chunk)
            total_rows += row_count

            logger.info(
                f"------------------------- Streamed Chunk {chunk_num}: {row_count} rows"
            )

            del chunk
            gc.collect()

    mark_source_files_as_processed(new_files, EXECUTION_DATE)

    logger.info(
        f"Loaded {total_rows} records of customers data from {len(new_files)} new zfiles into our Data Lake (s3)......................"
    )

    return pd.DataFrame({"total_rows": [total_rows]})


def extract_call_logs():
    """
    Extract call logs CSVs from S3.
    """
    logger.info(
        "[2/3]: ....................... Extracting Call Logs from S3 ......................"
    )

    prefix = "call logs/"

    new_files = get_new_source_files(prefix, ".csv")

    # files = _list_files(prefix, suffixes=[".csv"])
    if not new_files:
        logger.warning(
            "*************** No new call logs files to process ****************"
        )
        return pd.DataFrame()

    dfs = []
    for file_info in new_files:
        logger.info(
            f"---------------------- Processing new file: {file_info['key']} ---------------------"
        )
        obj = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=file_info["key"])
        df = pd.read_csv(obj["Body"])
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)
    df = clean_column_names(df)
    df = add_metadata(df, "call_logs")

    mark_source_files_as_processed(new_files, EXECUTION_DATE)
    logger.info(
        f"Loaded {len(df)} call logs from {len(new_files)} new source files)......................"
    )
    return df


def extract_social_media():
    """
    Extract social media json from S3 and load to destination S3.
    """
    logger.info(
        "[3/3]: ..................... Extracting Social Media data from S3 ....................."
    )

    prefix = "social_medias/"
    new_files = get_new_source_files(prefix, ".json")

    if not new_files:
        logger.warning(
            "****************** No new social media files to process ********************"
        )
        return 0

    total_rows = 0

    for idx, file_info in enumerate(new_files):
        file_key = file_info["key"]
        logger.info(f"[{idx+1}/{len(new_files)}] Processing: {file_key}")

        try:
            # Extract date from filename
            filename = file_key.split("/")[-1]
            date_str = filename.split("_")[-1].replace(".json", "")
            partition_date = datetime.strptime(date_str, "%Y-%m-%d").date()

            # Read and process file
            obj = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=file_key)
            data = json.loads(obj["Body"].read())
            df = (
                pd.json_normalize(data)
                if isinstance(data, list)
                else pd.json_normalize([data])
            )

            # Clean and add metadata
            df = clean_column_names(df)
            df = add_metadata(df, "social_medias")

            # Check if partition already exists in destination S3
            partition_path = (
                f"s3://{DEST_BUCKET}/social_medias/media_complaint_day_{partition_date}"
            )

            try:
                # Try to read existing partition to check if it exists
                existing_files = wr.s3.list_objects(partition_path)
                partition_exists = len(existing_files) > 0
            except:
                partition_exists = False

            if partition_exists:
                mode = "append"
                logger.info(
                    f"-------------------------------- Partition {partition_date} exists: APPENDING"
                )
            else:
                mode = "overwrite_partitions"
                logger.info(
                    f"---------------------------- New partition {partition_date}: OVERWRITING"
                )

            write_to_s3_parquet(
                df=df,
                table_name="social_medias",
                mode=mode,
                partition_date=partition_date,
            )

            rows_processed = len(df)
            total_rows += rows_processed
            logger.info(
                f"----------------------------------- Wrote {rows_processed} rows. Total: {total_rows}"
            )

            del df, data, obj
            gc.collect()

        except Exception as e:
            logger.error(f"✗ Failed to process {file_key}: {e}")
            continue

    # Mark files as processed only after successful completion
    if total_rows > 0:
        mark_source_files_as_processed(new_files, EXECUTION_DATE)
        logger.info(
            f"SUCCESS: Loaded {total_rows} records from {len(new_files)} files ✓"
        )
    else:
        logger.warning("No rows were processed")

    return total_rows
