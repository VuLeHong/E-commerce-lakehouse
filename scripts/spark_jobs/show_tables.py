import logging
from pathlib import Path
import os
from pyspark.sql import SparkSession
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

dotenv_path = BASE_DIR / ".env"
load_dotenv(dotenv_path)

def create_spark() -> SparkSession:
    return (
        SparkSession.builder
            .appName("Inspect Gold Layer (Iceberg + Nessie)")
            # --- MinIO config ---
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions, org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
            .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
            # .config("spark.sql.catalog.gold.type", "nessie")
            .config("spark.sql.catalog.gold.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
            .config("spark.sql.catalog.gold.uri", "http://nessie:19120/api/v2")
            .config("spark.sql.catalog.gold.ref", "main")
            .config("spark.sql.catalog.gold.authentication.type", "NONE")
            .config("spark.sql.catalog.gold.warehouse", "s3a://gold-layer")
            .getOrCreate()
    )

def read_gold_layer(spark: SparkSession, table: str):
    try:
        df = spark.table(f"gold.{table}")
        count = df.count()
        logger.info(f"✅ Table: gold.{table} | Row count: {count}")
        df.printSchema()
        df.show(5, truncate=False)
    except Exception as e:
        logger.error(f"❌ Failed to read table 'gold.{table}': {e}")

if __name__ == "__main__":
    spark = create_spark()

    try:
        tables = [row.tableName for row in spark.catalog.listTables("gold")]
        logger.info(f"📦 Tables found in gold catalog: {tables}")
    except Exception as e:
        logger.warning(f"⚠️ Could not list tables from gold catalog: {e}")
        tables = ["sales_summary", "review_summary"]

    success_tables, failed_tables = [], []

    for table in tables:
        logger.info(f"--- Reading table: gold.{table} ---")
        try:
            read_gold_layer(spark, table)
            success_tables.append(table)
        except Exception:
            failed_tables.append(table)

    logger.info(f"✅ Successfully read tables: {success_tables}")
    if failed_tables:
        logger.warning(f"⚠️ Failed tables: {failed_tables}")
    else:
        logger.info("🎉 All gold tables read successfully!")

    spark.stop()
