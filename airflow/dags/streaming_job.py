import airflow.utils.dates
from pathlib import Path
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

BASE_DIR = Path(__file__).resolve().parent.parent

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1)
}

common_conf = {
    "spark.driver.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console",
    "spark.executor.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console"
}

with DAG(
    'streaming-job',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
) as dag:

    bronze_streaming_load = SparkSubmitOperator(
        task_id="bronze_streaming_load",
        conn_id="spark",
        application=str(BASE_DIR / "scripts" / "spark_jobs" / "streaming_flow.py"),
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        ),
        conf=common_conf
    )

# --- DAG Dependencies ---
# Bronze → Bronze Quality Check
bronze_streaming_load
