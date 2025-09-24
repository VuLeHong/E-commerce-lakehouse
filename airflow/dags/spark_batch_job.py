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
    'batch-job',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    bronze_batch_load = SparkSubmitOperator(
        task_id="bronze_batch_load",
        conn_id="spark",
        application=str(BASE_DIR / "scripts" / "spark_jobs" / "bronze_batch_load.py"),
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "org.postgresql:postgresql:42.7.3"
        ),
        conf=common_conf
    )
    
    silver_clean_transform = SparkSubmitOperator(
    task_id="silver_transform",
    conn_id="spark",
    application=str(BASE_DIR / "scripts" / "spark_jobs" / "silver_clean_transform.py"),
    packages=(
        "org.apache.hadoop:hadoop-aws:3.3.1,"
        "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.2"
    ),
    conf=common_conf
)

    gold_transform = SparkSubmitOperator(
        task_id="gold_transform",
        conn_id="spark",
        application=str(BASE_DIR / "scripts" / "spark_jobs" / "gold_transfrom.py"),
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.2"
        ),
        conf=common_conf
    )
    
    show_tables = SparkSubmitOperator(
        task_id="show_tables",
        conn_id="spark",
        application=str(BASE_DIR / "scripts" / "spark_jobs" / "show_tables.py"),
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.2"
        ),
        conf=common_conf
    )


# --- DAG Dependencies ---
# Bronze → Bronze Quality Check
bronze_batch_load >> silver_clean_transform >> gold_transform >> show_tables
