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
    "spark.executor.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console",
    "spark.rpc.askTimeout": "600s",
    "spark.network.timeout": "600s",
    "spark.executor.heartbeatInterval": "60s",

}

SPARK_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.1,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.105.4"
)

with DAG(
    'train-job',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
) as dag:


    train_als_model = SparkSubmitOperator(
        task_id="train_als_recommendation",
        conn_id="spark",
        application=str(BASE_DIR / "scripts" / "spark_jobs" / "train_model.py"),
        packages=SPARK_PACKAGES,
        conf=common_conf,
        deploy_mode="client"
    )

# --- DAG Dependencies ---
# Bronze → Bronze Quality Check
train_als_model
