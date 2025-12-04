import snowflake.connector
import logging


logging.basicConfig(level=logging.INFO,
                    format="{asctime} - {levelname} - {message}",
                    style="{",
                    datefmt="%Y-%m-%d %H:%M",
                    filename="process_etl.log",
                    encoding='utf-8',
                    filemode="a")
logger = logging.getLogger(__name__)

from config import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    )

SNOWFLAKE_STAGE = "TELECOM_SNOWFLAKE_STAGE" 


def get_connection():
    """Create and return a new Snowflake connection."""
    conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    )
    return conn

def load_s3_parquet_to_snowflake(table_name, unique_keys, column_mapping=None):
    """
    Load parquet files from stage into Snowflake:
    - Find stage path variant
    - Use INFER_SCHEMA table function directly in TEMPLATE to create a temp table
    - COPY INTO temp table (FORCE=FALSE for idempotency)
    - MERGE into main table
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        possible_paths = [
            f"@{SNOWFLAKE_STAGE}/{table_name}/",
            f"@{SNOWFLAKE_STAGE}/{table_name}",
            f"@{SNOWFLAKE_STAGE}/{table_name.upper()}/",
            f"@{SNOWFLAKE_STAGE}/{table_name.lower()}/",
        ]

        s3_path = None
        files = []
        logger.info(f"----------------------------- Searching for {table_name} files in stage --------------------------")
        for path in possible_paths:
            try:
                logger.info(f" --------------------------------------- Trying: {path}")
                cursor.execute(f"LIST {path}")
                temp_files = cursor.fetchall()
                if temp_files:
                    s3_path = path
                    files = temp_files
                    logger.info(f" ----------------------------- Found {len(files)} file(s) at {path} -----------------------------")
                    break
            except Exception as e:
                logger.debug(f"  Path {path} not accessible: {str(e)}")

        if not s3_path or not files:
            logger.error(f"********************** No files found for table '{table_name}' in any path variants **********************")
            try:
                cursor.execute(f"LIST @{SNOWFLAKE_STAGE}/")
                root_items = cursor.fetchall()
                available = [item[0] for item in root_items if '/' in item[0]]
                logger.error(f"************************* Available paths at stage root (sample): {available[:10]}")
            except Exception:
                pass
            raise ValueError(
                f"No files found at any variation of @{SNOWFLAKE_STAGE}/{table_name}/. "
                f"Tried: {', '.join(possible_paths)}."
            )

        logger.info(f"Found {len(files)} file(s) at {s3_path}")

        # -------------------------
        # Use INFER_SCHEMA table function with ARRAY_AGG
        # -------------------------
        logger.info(f"................. Creating TEMP table {table_name}_TEMP using INFER_SCHEMA template .................")
        create_temp_sql = f"""
        CREATE OR REPLACE TEMPORARY TABLE {table_name}_TEMP
        USING TEMPLATE (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
            FROM TABLE(
                INFER_SCHEMA(
                    LOCATION => '{s3_path}',
                    FILE_FORMAT => 'MY_PARQUET_FORMAT'
                )
            )
        )
        """
        cursor.execute(create_temp_sql)
        logger.info(f"---------------------------- Temporary table {table_name}_TEMP created (template from infer schema)")

        # Column mapping
        if column_mapping:
            logger.info(f"Applying column mapping: {column_mapping}")
            for old_name, new_name in column_mapping.items():
                try:
                    cursor.execute(f'ALTER TABLE {table_name}_TEMP RENAME COLUMN "{old_name}" TO "{new_name}"')
                    logger.info(f"---------------------------- Renamed column {old_name} >> {new_name}")
                except Exception as e:
                    logger.warning(f"Could not rename {old_name} to {new_name}: {e}")

        # Ensuring main table exists with same structure
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} LIKE {table_name}_TEMP")
        logger.info(f"Main table {table_name} verified/created (LIKE {table_name}_TEMP)")
        
        # Empty temp table
        cursor.execute(f"TRUNCATE TABLE {table_name}_TEMP")

        # COPY INTO temp table
        logger.info(f"..................Copying data from {s3_path} to {table_name}_TEMP (FORCE=FALSE)..................")
        copy_sql = f"""
        COPY INTO {table_name}_TEMP
        FROM '{s3_path}'
        FILE_FORMAT = 'MY_PARQUET_FORMAT'
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        FORCE = FALSE
        """
        cursor.execute(copy_sql)
        try:
            copy_result = cursor.fetchone()
            logger.info(f"COPY INTO result: {copy_result}")
        except Exception:
            logger.debug("                   No fetchable COPY result (connector/version behaviour)                        ")

        # Get columns for MERGE
        cursor.execute(f"DESCRIBE TABLE {table_name}_TEMP")
        cols = [row[0] for row in cursor.fetchall()]
        logger.info(f"----------------------------- Columns in temp table: {cols}")

        # Use actual temp table columns for unique keys
        actual_unique_keys = [c for c in cols if c.upper() in [k.upper() for k in unique_keys]]
        missing = set([k.upper() for k in unique_keys]) - set([c.upper() for c in cols])
        if missing:
            raise ValueError(f"Unique keys not found in temp table: {missing}")

        # Deduplicate temp table, keeping only the latest row per unique key
        logger.info(f"............................... Deduplicating {table_name}_TEMP on keys: {actual_unique_keys} ...............................")
        dedup_table = f"{table_name}_TEMP_DEDUP"

        # Create deduplicated table using row_number to keep latest row
        partition_by = ", ".join([f'"{k}"' for k in actual_unique_keys])
        order_by = '"ingestion_timestamp" DESC' if 'ingestion_timestamp' in cols else '1'

        dedup_sql = f"""
        CREATE OR REPLACE TEMPORARY TABLE {dedup_table} AS
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY {order_by}) as rn
            FROM {table_name}_TEMP
        )
        WHERE rn = 1
        """
        cursor.execute(dedup_sql)

        # Drop the row_number column
        cursor.execute(f'ALTER TABLE {dedup_table} DROP COLUMN rn')

        # Get row counts for logging
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}_TEMP")
        original_count = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM {dedup_table}")
        dedup_count = cursor.fetchone()[0]

        if original_count != dedup_count:
            logger.warning(f"--------------------------------- Removed {original_count - dedup_count} duplicate rows from {table_name}_TEMP")
        else:
            logger.info(f"------------------------------- No duplicates found in {table_name}_TEMP")

        # Use deduplicated table for merge
        temp_source = dedup_table

        update_cols = [c for c in cols if c.upper() not in [k.upper() for k in unique_keys]]

        # Build merge statement
        if not update_cols:
            merge_sql = f"""
            MERGE INTO {table_name} t
            USING {temp_source} s
            ON {" AND ".join([f't."{col}"=s."{col}"' for col in actual_unique_keys])}
            WHEN NOT MATCHED THEN INSERT ({",".join([f'"{c}"' for c in cols])})
            VALUES ({",".join([f's."{c}"' for c in cols])})
            """
        else:
            update_sql = ", ".join([f't."{c}"=s."{c}"' for c in update_cols])
            join_cond = " AND ".join([f't."{col}"=s."{col}"' for col in actual_unique_keys])
            merge_sql = f"""
            MERGE INTO {table_name} t
            USING {temp_source} s
            ON {join_cond}
            WHEN MATCHED THEN UPDATE SET {update_sql}
            WHEN NOT MATCHED THEN INSERT ({",".join([f'"{c}"' for c in cols])})
            VALUES ({",".join([f's."{c}"' for c in cols])})
            """

        logger.info(f"........................... Executing MERGE for {table_name}...............................")
        cursor.execute(merge_sql)
        rows_affected = cursor.rowcount if cursor.rowcount is not None else 0
        logger.info(f"[{table_name}] Successfully merged ~{rows_affected} rows")

        return rows_affected

    except Exception as e:
        logger.exception(f"********************* Error loading {table_name}: {str(e)} ************************")
        raise

    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
