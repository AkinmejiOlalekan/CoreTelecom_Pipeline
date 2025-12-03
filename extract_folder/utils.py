import os
import json
import boto3
import logging
import awswrangler as wr

from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

session_dest = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION", "eu-north-1")
)

session_source = boto3.Session(
    profile_name="source",
    region_name="eu-north-1"
)

s3_client_1 = session_dest.client("s3")
ssm_client_1 = session_dest.client("ssm")

s3_client_2 = session_source.client("s3")
ssm_client_2 = session_source.client("ssm")


logging.basicConfig(level=logging.INFO,
                    format="{asctime} - {levelname} - {message}",
                    style="{",
                    datefmt="%Y-%m-%d %H:%M",
                    filename="process_etl.log",
                    encoding='utf-8',
                    filemode="a")
logger = logging.getLogger(__name__)


# s3 Constants
SOURCE_BUCKET = os.getenv("SOURCE_BUCKET")
DEST_BUCKET = os.getenv("DEST_BUCKET")
EXECUTION_DATE = datetime.now().date()


# ==================== SOURCE FILE TRACKING ====================
def load_processed_files_tracker():
    """
        Load the list of source files we've already processed.
    """
    try:
        obj = s3_client_1.get_object(Bucket=DEST_BUCKET, Key='metadata/processed_source_files.json')
        data = json.loads(obj['Body'].read())
        logger.info(f"------------------------------ Loaded tracker: {len(data)} source files already processed ------------------------")
        return data
    except s3_client_1.exceptions.NoSuchKey:
        logger.info(f"-------------------------- No tracker file found in the destination folder: Starting fresh --------------------------")
        return {}
    except Exception as e:
        logger.exception(f"------------------------ Error loading tracker due to {e} --------------------------")
        raise


def save_processed_files_tracker(tracker_data):
    """Save the updated tracker to S3."""
    s3_client_1.put_object(
        Bucket=DEST_BUCKET,
        Key='metadata/processed_source_files.json',
        Body=json.dumps(tracker_data, indent=2),
        ContentType='application/json'
    )
    logger.info(f"---------------------------- Saved tracker: {len(tracker_data)} source files ------------------------")


def get_new_source_files(prefix, file_extension = None):
    """
        Get only new source files that haven't been processed yet.
    """
    response = s3_client_2.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=prefix)
    all_source_files = []

    for obj in response.get('Contents', []):
        if obj['Key'].endswith(file_extension):
            all_source_files.append({
                'key': obj['Key'],
                'last_modified': obj['LastModified'].isoformat(),
                'size': obj['Size']
            })
    
    logger.info(f"---------------------- Total source files in {prefix}: {len(all_source_files)} ------------------------")
    

    tracker = load_processed_files_tracker()
    

    new_files = []
    for file_info in all_source_files:
        if file_info['key'] not in tracker:
            new_files.append(file_info)
            logger.info(f"-------------------------- NEW: {file_info['key']} processed -----------------------")
        else:
            logger.info(f"SKIP: {file_info['key']} already processed on {tracker[file_info['key']]['processed_date']})")
    
    logger.info(f"----------------------- Result: {len(new_files)} new files to process ---------------------")
    return new_files


def mark_source_files_as_processed(files, execution_date):
    """
        Mark source files as processed in the tracker.
    """
    tracker = load_processed_files_tracker()
    
    for file_info in files:
        tracker[file_info['key']] = {
            'processed_date': execution_date.isoformat(),
            'processed_timestamp': datetime.now().isoformat(),
            'file_size': file_info.get('size', 0),
            'source_last_modified': file_info.get('last_modified')
        }
    
    save_processed_files_tracker(tracker)
    logger.info(f"------------------------- Marked {len(files)} files as processed -------------------------")

# Data columns standardization
def clean_column_names(df):
    """
        Standardize column names: lowercase, replace spaces with underscores.
    """
    df.columns = df.columns.str.lower().str.replace(" ", "_").str.strip()
    return df


def add_metadata(df, source_name):
    """
        Add ingestion metadata to DataFrame.
    """
    df = df.copy()
    df["source_system"] = source_name
    df["ingestion_timestamp"] = datetime.now()
    df["ingestion_date"] =  EXECUTION_DATE
    return df


def write_to_s3_parquet(df, table_name, mode = None, partition_date = None):
    """
        Write DataFrame to S3 as Parquet. Overwrites partitions for idempotency.
    """

    mode = "overwrite_partitions" or mode
    if df is None or df.empty:
        logger.error(f"Empty DataFrame for {table_name}, skipping...................")
        return

    if partition_date:
        df = df.copy()
        df["ingestion_date"] = partition_date

    path = f"s3://{DEST_BUCKET}/staging/{table_name}/"
    wr.s3.to_parquet(
        df=df,
        path=path,
        boto3_session=session_dest,
        dataset=True,
        mode=mode,
        partition_cols=["ingestion_date"],
        compression="snappy",
    )
    logger.info(f"Successfully wrote {len(df)} rows to {path}...................")
