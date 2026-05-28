import os
import sys
import logging
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

load_dotenv(BASE_DIR / ".env")


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Inspect Gold Layer Tables")

        # ---------- MinIO ----------
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # ---------- Iceberg + Nessie ----------
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
        )

        .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.gold.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog"
        )
        .config("spark.sql.catalog.gold.uri", "http://nessie:19120/api/v2")
        .config("spark.sql.catalog.gold.ref", "main")
        .config("spark.sql.catalog.gold.authentication.type", "NONE")
        .config("spark.sql.catalog.gold.warehouse", "s3a://gold-layer")

        .getOrCreate()
    )


def inspect_table(spark: SparkSession, table_name: str):
    full_name = f"gold.{table_name}"

    logger.info("=" * 80)
    logger.info(f"Checking table: {full_name}")

    try:
        df = spark.table(full_name)

        row_count = df.count()
        logger.info(f"Table exists: {full_name}")
        logger.info(f"Row count: {row_count}")

        logger.info("Schema:")
        df.printSchema()

        logger.info("Sample data:")
        df.show(10, truncate=False)

    except Exception as e:
        logger.error(f"Cannot read table {full_name}: {e}")
        raise


def main():
    spark = create_spark()

    gold_tables = [
        "dim_users",
        "dim_products",
        "dim_date",
        "fact_sales",
        "fact_reviews",
        "fact_user_interactions",
    ]

    try:
        for table in gold_tables:
            inspect_table(spark, table)

        logger.info("Gold table inspection completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()