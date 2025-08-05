import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import os

PROJECT_ID = "mystic-advice-466120-f1"
LOCATION = "US"

# In Composer, this environment variable points to /home/airflow/gcs/
GCS_HOME = os.environ.get("AIRFLOW_HOME")

SQL_FILE_PATH_1 = os.path.join(GCS_HOME, 'data/BQ/bronze.sql')
SQL_FILE_PATH_2 = os.path.join(GCS_HOME, 'data/BQ/silver.sql')
SQL_FILE_PATH_3 = os.path.join(GCS_HOME, 'data/BQ/gold.sql')

def read_sql_file(file_path):
    """Helper function to read a SQL file"""
    with open(file_path, "r") as file:
        return file.read()
    
# Read queries once when the DAG is parsed
BRONZE_QUERY = read_sql_file(SQL_FILE_PATH_1)
SILVER_QUERY = read_sql_file(SQL_FILE_PATH_2)
GOLD_QUERY = read_sql_file(SQL_FILE_PATH_3)

# Define default arguments
ARGS = {
    "owner": "Nancy Le",
    "start_date": days_ago(1),  # <-- FIX: Added a start date
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="bigquery_dag",
    schedule_interval=None,
    description="DAG to run the bigquery jobs",
    default_args=ARGS,
    tags=["gcs", "bq", "etl", "healthcare"]
) as dag:

    # Task to create bronze table
    bronze_tables = BigQueryInsertJobOperator(
        task_id="bronze_tables",
        configuration={
            "query": {
                "query": BRONZE_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
                # <-- FIX: Added a destination table
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": "bronze_dataset",
                    "tableId": "bronze_table",
                },
                "writeDisposition": "WRITE_TRUNCATE" # Recommended to overwrite the table
            }
        },
    )

    # Task to create silver table
    silver_tables = BigQueryInsertJobOperator(
        task_id="silver_tables",
        configuration={
            "query": {
                "query": SILVER_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
                # <-- FIX: Added a destination table
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": "silver_dataset",
                    "tableId": "silver_table",
                },
                "writeDisposition": "WRITE_TRUNCATE"
            }
        },
    )


    # Task to create gold table
    gold_tables = BigQueryInsertJobOperator(
        task_id="gold_tables",
        configuration={
            "query": {
                "query": GOLD_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
                # <-- FIX: Added a destination table
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": "gold_dataset",
                    "tableId": "gold_table",
                },
                "writeDisposition": "WRITE_TRUNCATE"
            }
        },
    )

# Define dependencies
bronze_tables >> silver_tables >> gold_tables