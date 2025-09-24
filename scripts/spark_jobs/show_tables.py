import logging
from pathlib import Path
import os
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parent.parent

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
)

def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Inspect Gold Layer (Iceberg)")
        # MinIO config
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # Iceberg config
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.gold.type", "hadoop")
        .config("spark.sql.catalog.gold.warehouse", "s3a://gold-layer")
        .getOrCreate()
    )


def read_gold_layer(spark: SparkSession, table: str):
    try:
        df = spark.table(f"gold.{table}")
        logging.info(f"Table: gold.{table} | Count: {df.count()}")
        df.printSchema()
        df.show(5, truncate=False)
    except Exception as e:
        logging.error(f"Failed to read table 'gold.{table}': {e}")


if __name__ == "__main__":
    spark = create_spark()

    tables = ["sales_summary", "review_summary"]

    for table in tables:
        logging.info(f"--- Reading table: gold.{table} ---")
        read_gold_layer(spark, table)

    spark.stop()
