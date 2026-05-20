import logging
from pathlib import Path
import os
import sys

from pyspark.sql import SparkSession
from dotenv import load_dotenv

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..")
    )
)

# =====================================================
# SETUP
# =====================================================
BASE_DIR = Path(__file__).resolve().parent.parent.parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

dotenv_path = BASE_DIR / ".env"
load_dotenv(dotenv_path)

# =====================================================
# SPARK
# =====================================================
def create_spark() -> SparkSession:

    return (
        SparkSession.builder
        .appName("Inspect Gold Layer")

        # ---------- MinIO ----------
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.getenv("MINIO_ENDPOINT")
        )

        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.getenv("MINIO_ACCESS_KEY")
        )

        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.getenv("MINIO_SECRET_KEY")
        )

        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )

        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )

        # ---------- Iceberg + Nessie ----------
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
        )

        .config(
            "spark.sql.catalog.gold",
            "org.apache.iceberg.spark.SparkCatalog"
        )

        .config(
            "spark.sql.catalog.gold.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog"
        )

        .config(
            "spark.sql.catalog.gold.uri",
            "http://nessie:19120/api/v2"
        )

        .config(
            "spark.sql.catalog.gold.ref",
            "main"
        )

        .config(
            "spark.sql.catalog.gold.authentication.type",
            "NONE"
        )

        .config(
            "spark.sql.catalog.gold.warehouse",
            "s3a://gold-layer"
        )

        .getOrCreate()
    )

# =====================================================
# READ TABLE
# =====================================================
def inspect_table(spark: SparkSession, table_name: str):

    full_table_name = f"gold.{table_name}"

    logger.info("=" * 80)
    logger.info(f"Reading table: {full_table_name}")

    df = spark.table(full_table_name)

    row_count = df.count()

    logger.info(f"Row count: {row_count}")

    logger.info("Schema:")
    df.printSchema()

    logger.info("Sample data:")
    df.show(5, truncate=False)

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":

    spark = create_spark()

    # =================================================
    # EXPECTED GOLD TABLES
    # =================================================
    expected_tables = [
        "dim_users",
        "dim_products",
        "dim_date",
        "fact_sales",
        "fact_reviews",
        "fact_user_interactions"
    ]

    success_tables = []
    failed_tables = []

    try:

        # =============================================
        # SHOW AVAILABLE TABLES
        # =============================================
        available_tables = [
            row.tableName
            for row in spark.catalog.listTables("gold")
        ]

        logger.info(
            f"Tables found in gold catalog: {available_tables}"
        )

        # =============================================
        # READ EXPECTED TABLES
        # =============================================
        for table in expected_tables:

            try:

                if table not in available_tables:
                    raise Exception(
                        f"Table not found in catalog: gold.{table}"
                    )

                inspect_table(spark, table)

                success_tables.append(table)

            except Exception as e:

                logger.error(
                    f"Failed reading gold.{table}: {e}"
                )

                failed_tables.append(table)

        # =============================================
        # SUMMARY
        # =============================================
        logger.info("=" * 80)

        logger.info(
            f"Successfully read tables: {success_tables}"
        )

        if failed_tables:

            logger.warning(
                f"Failed tables: {failed_tables}"
            )

        else:

            logger.info(
                "All gold tables read successfully"
            )

    finally:

        spark.stop()