import sys

sys.path.insert(0, "/home/olalekan/telecom")
sys.path.insert(0, "/home/olalekan/telecom/snowflakes")


from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


from extract_folder.gsheet_extractor import extract_agents
from extract_folder.pg_extractor import extract_web_forms
from extract_folder.utils import write_to_s3_parquet
from extract_folder.s3_extractor import (
    extract_customers,
    extract_call_logs,
    extract_social_media,
)
from snowflakes.snowflake_load import load_s3_parquet_to_snowflake


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["akinmejiolalekan7@gmail.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


# ==================== S3 LOAD FUNCTIONS ====================


def extract_and_load_customers(**context):
    """Extract customers and load to S3."""
    df = extract_customers()
    if not df.empty:
        customers_count = int(df["total_rows"].iloc[0])
    else:
        customers_count = 0
        context["ti"].xcom_push(key="customers_count", value=customers_count)


def extract_and_load_agents(**context):
    """Extract agents from Google Sheets and load to S3."""
    df = extract_agents()
    if not df.empty:
        write_to_s3_parquet(df, "agents")
        context["ti"].xcom_push(key="agents_count", value=len(df))


def extract_and_load_call_logs(**context):
    """Extract call logs for execution date and load to S3."""
    execution_date = context["ds"]
    exec_date = datetime.strptime(execution_date, "%Y-%m-%d").date()

    df = extract_call_logs()
    if not df.empty:
        write_to_s3_parquet(df, "call_logs", exec_date)
        context["ti"].xcom_push(key="call_logs_count", value=len(df))


def extract_and_load_social_media(**context):
    """Extract social media data and load to S3. Memory-efficient and idempotent."""
    total_rows = extract_social_media()

    if total_rows > 0:
        context["ti"].xcom_push(key="social_media_count", value=total_rows)
    else:
        context["ti"].xcom_push(key="social_media_count", value=0)

    return total_rows


def extract_and_load_web_forms(**context):
    """Extract web forms from Postgres for execution date and load to S3."""
    rows_written = extract_web_forms(table_name_path="web_forms", chunk_size=50_000)

    context["ti"].xcom_push(key="web_forms_count", value=rows_written)


# ======================= SNOWFLAKES LOAD FUNTIONS ===========================


def load_customers_to_snowflake():
    load_s3_parquet_to_snowflake("customers", unique_keys=["CUSTOMER_ID"])


def load_agents_to_snowflake():
    load_s3_parquet_to_snowflake("agents", unique_keys=["ID"])


def load_call_logs_to_snowflake():
    load_s3_parquet_to_snowflake("call_logs", unique_keys=["CALL_ID"])


def load_media_data_to_snowflake():
    load_s3_parquet_to_snowflake("social_media", unique_keys=["SOCIAL_MEDIA"])


def load_web_forms_to_snowflake():
    load_s3_parquet_to_snowflake("web_forms", unique_keys=["WEB_FORM_ID"])


##########################################################################################################


def validate_pipeline(**context):
    """Validate that all extractions completed successfully."""
    ti = context["ti"]

    s3_counts = {
        "call_logs": ti.xcom_pull(task_id="extract_call_logs", key="call_logs_count")
        or 0,
        "social_media": ti.xcom_pull(
            task_id="extract_social_media", key="social_media_count"
        )
        or 0,
        "web_forms": ti.xcom_pull(task_id="extract_web_forms", key="web_forms_count")
        or 0,
    }

    sf_counts = {
        "call_logs": ti.xcom_pull(
            task_id="load_call_logs_snowflake", key="call_logs_snowflake_rows"
        )
        or 0,
        "social_media": ti.xcom_pull(
            task_id="load_social_media_snowflake", key="social_media_snowflake_rows"
        )
        or 0,
        "web_forms": ti.xcom_pull(
            task_id="load_web_forms_snowflake", key="web_forms_snowflake_rows"
        )
        or 0,
    }

    print(
        f"""
    ========================================
    PIPELINE VALIDATION SUMMARY
    ========================================
    Execution Date: {context['ds']}
    
    Daily Data:
    - Call Logs: {s3_counts['call_logs']:,} records
    - Social Media: {s3_counts['social_media']:,} records
    - Web Forms: {s3_counts['web_forms']:,} records

    Total S3: {sum(s3_counts.values()):,}

    Snowflake Staging:
    - Call Logs: {sf_counts['call_logs']:,} records
    - Social Media: {sf_counts['social_media']:,} records
    - Web Forms: {sf_counts['web_forms']:,} records

    Total Snowflake: {sum(sf_counts.values()):,}
    ========================================
    """
    )


with DAG(
    dag_id="telecom_dag",
    default_args=default_args,
    description="Unified Customer Experience Data Platform - Staging Layer Ingestion",
    start_date=datetime(2025, 11, 20),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["telecom", "customer-experience", "raw-layer"],
):

    start = EmptyOperator(task_id="start")

    with TaskGroup(
        "static_data", tooltip="Extract static reference data"
    ) as static_group:

        # Customers Task
        extract_customers_task = PythonOperator(
            task_id="extract_customers",
            python_callable=extract_and_load_customers,
        )

        load_customers_task = PythonOperator(
            task_id="load_customers_snowflake",
            python_callable=load_customers_to_snowflake,
        )

        # Agents Tasks
        extract_agents_task = PythonOperator(
            task_id="extract_agents",
            python_callable=extract_and_load_agents,
        )

        load_agents_task = PythonOperator(
            task_id="load_agents_snowflake",
            python_callable=load_agents_to_snowflake,
        )

        extract_customers_task >> load_customers_task
        extract_agents_task >> load_agents_task

    with TaskGroup(
        "daily_data", tooltip="Extract daily incremental data"
    ) as daily_group:

        # Call Logs Tasks
        extract_call_logs_task = PythonOperator(
            task_id="extract_call_logs",
            python_callable=extract_and_load_call_logs,
        )

        load_call_logs_task = PythonOperator(
            task_id="load_call_logs_snowflake",
            python_callable=load_call_logs_to_snowflake,
        )

        # Social Media Tasks
        extract_social_media_task = PythonOperator(
            task_id="extract_social_media",
            python_callable=extract_and_load_social_media,
        )

        load_social_media_task = PythonOperator(
            task_id="load_social_media_snowflake",
            python_callable=load_media_data_to_snowflake,
        )

        # Web Forms Tasks
        extract_web_forms_task = PythonOperator(
            task_id="extract_web_forms",
            python_callable=extract_and_load_web_forms,
        )

        load_web_forms_task = PythonOperator(
            task_id="load_web_forms_snowflake",
            python_callable=load_web_forms_to_snowflake,
        )

        extract_call_logs_task >> load_call_logs_task
        extract_social_media_task >> load_social_media_task
        extract_web_forms_task >> load_web_forms_task

    validate = PythonOperator(
        task_id="validate_pipeline",
        python_callable=validate_pipeline,
    )

    with TaskGroup(
        "dbt_transform", tooltip="dbt transformations: Staging >> Transformed"
    ) as dbt_group:
        # Test source data quality
        dbt_test_sources = BashOperator(
            task_id="dbt_test_sources",
            bash_command="cd /opt/airflow/dbt && dbt test --select source:*",
        )

        dbt_run_transformed = BashOperator(
            task_id="dbt_run_transformed",
            bash_command="cd /opt/airflow/dbt && dbt run --select transformed.*",
        )

        dbt_test_transformed = BashOperator(
            task_id="dbt_test_marts",
            bash_command="cd /opt/airflow/dbt && dbt test --select transformed.*",
        )

        dbt_test_sources >> dbt_run_transformed >> dbt_test_transformed

    success_notification = BashOperator(
        task_id="send_success_notification",
        bash_command="""
        echo "Pipeline completed successfully for {{ ds }}"
        """,
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> [static_group, daily_group]
        >> validate
        >> dbt_group
        >> success_notification
        >> end
    )
