import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

dotenv_path = BASE_DIR / ".env"
load_dotenv(dotenv_path)

# =========================================================
# SPARK
# =========================================================
def create_spark():
    return (
        SparkSession.builder
        .appName("Bronze -> Silver Operational Layer")

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

        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.silver.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog"
        )
        .config("spark.sql.catalog.silver.uri", "http://nessie:19120/api/v2")
        .config("spark.sql.catalog.silver.ref", "main")
        .config("spark.sql.catalog.silver.authentication.type", "NONE")
        .config("spark.sql.catalog.silver.warehouse", "s3a://silver-layer")

        .getOrCreate()
    )

# =========================================================
# HELPERS
# =========================================================
def safe_read_parquet(spark, path):
    try:
        df = spark.read.parquet(path)

        # remove partition cols from bronze
        for c in ["year", "month", "day"]:
            if c in df.columns:
                df = df.drop(c)

        return df

    except AnalysisException:
        logger.warning(f"Path not found: {path}")
        return None


def overwrite_table(df, table_name):
    (
        df.writeTo(table_name)
        .tableProperty("format-version", "1")
        .createOrReplace()
    )

    logger.info(f"Created table: {table_name}")


# =========================================================
# TRANSFORM
# =========================================================
def transform(spark):

    bronze = "s3a://bronze-layer"

    # =====================================================
    # USERS
    # =====================================================
    users = safe_read_parquet(spark, f"{bronze}/brz.users")

    if users is not None:

        silver_users = (
            users
            .filter(col("user_id").isNotNull())

            .dropDuplicates(["user_id"])

            .withColumn(
                "email",
                lower(trim(col("email")))
            )

            .withColumn(
                "first_name",
                initcap(trim(col("first_name")))
            )

            .withColumn(
                "last_name",
                initcap(trim(col("last_name")))
            )

            .withColumn(
                "country",
                upper(trim(col("country")))
            )

            .withColumn(
                "city",
                initcap(trim(col("city")))
            )

            .withColumn(
                "created_at",
                to_timestamp(col("created_at"))
            )
        )

        overwrite_table(
            silver_users,
            "silver.users"
        )

    # =====================================================
    # PRODUCTS
    # =====================================================
    products = safe_read_parquet(spark, f"{bronze}/brz.products")
    categories = safe_read_parquet(spark, f"{bronze}/brz.categories")

    if products is not None and categories is not None:

        silver_products = (
            products
            .join(
                categories.select(
                    "category_id",
                    "category_name"
                ),
                "category_id",
                "left"
            )

            .filter(col("product_id").isNotNull())

            .dropDuplicates(["product_id"])

            .withColumn(
                "product_name",
                trim(col("product_name"))
            )

            .withColumn(
                "brand",
                upper(trim(col("brand")))
            )

            .withColumn(
                "category_name",
                initcap(trim(col("category_name")))
            )

            .withColumn(
                "price",
                round(col("price"), 2)
            )

            .filter(col("price") > 0)

            .withColumn(
                "updated_at",
                to_timestamp(col("updated_at"))
            )
        )

        overwrite_table(
            silver_products,
            "silver.products"
        )

    # =====================================================
    # ORDERS
    # =====================================================
    orders = safe_read_parquet(spark, f"{bronze}/brz.orders")

    if orders is not None:

        silver_orders = (
            orders
            .filter(col("order_id").isNotNull())
            .filter(col("user_id").isNotNull())

            .dropDuplicates(["order_id"])

            .withColumn(
                "total_price",
                round(col("total_price"), 2)
            )

            .filter(col("total_price") > 0)

            .withColumn(
                "order_date",
                to_timestamp(col("order_date"))
            )
        )

        overwrite_table(
            silver_orders,
            "silver.orders"
        )

    # =====================================================
    # ORDER ITEMS
    # =====================================================
    order_items = safe_read_parquet(
        spark,
        f"{bronze}/brz.order_items"
    )

    if order_items is not None:

        silver_order_items = (
            order_items
            .filter(col("order_item_id").isNotNull())

            .dropDuplicates(["order_item_id"])

            .filter(col("quantity") > 0)
            .filter(col("price") > 0)

            .withColumn(
                "price",
                round(col("price"), 2)
            )

            .withColumn(
                "item_total",
                round(col("item_total"), 2)
            )
        )

        overwrite_table(
            silver_order_items,
            "silver.order_items"
        )

    # =====================================================
    # REVIEWS
    # =====================================================
    reviews = safe_read_parquet(spark, f"{bronze}/brz.reviews")

    if reviews is not None:

        silver_reviews = (
            reviews
            .filter(col("review_id").isNotNull())

            .dropDuplicates(["review_id"])

            .filter(col("rating").between(1, 5))

            .withColumn(
                "review_text",
                trim(col("review_text"))
            )

            .withColumn(
                "review_date",
                to_timestamp(col("review_date"))
            )
        )

        overwrite_table(
            silver_reviews,
            "silver.reviews"
        )


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":

    spark = create_spark()

    try:
        transform(spark)
        logger.info("Silver transformation completed")

    finally:
        spark.stop()