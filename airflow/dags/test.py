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

SPARK_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.1,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.105.4"
)

with DAG(
    'test-job',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    silver_clean_transform = SparkSubmitOperator(
        task_id="silver_transform",
        conn_id="spark",
        application=str(BASE_DIR / "scripts" / "spark_jobs" / "silver_clean_transform.py"),
        packages=SPARK_PACKAGES,
        conf=common_conf,
        deploy_mode="client"
    )

    gold_transform = SparkSubmitOperator(
        task_id="gold_transform",
        conn_id="spark",
        application=str(BASE_DIR / "scripts" / "spark_jobs" / "gold_transfrom.py"),
        packages=SPARK_PACKAGES,
        conf=common_conf,
        deploy_mode="client"
    )
    
    show_tables = SparkSubmitOperator(
        task_id="show_tables",
        conn_id="spark",
        application=str(BASE_DIR / "scripts" / "spark_jobs" / "show_tables.py"),
        packages=SPARK_PACKAGES,
        conf=common_conf,
        deploy_mode="client"
    )


# --- DAG Dependencies ---
# Bronze → Bronze Quality Check
silver_clean_transform >> gold_transform >> show_tables
