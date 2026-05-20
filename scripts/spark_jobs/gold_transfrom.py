import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from dotenv import load_dotenv

# =========================================================
# SETUP
# =========================================================
BASE_DIR = Path(__file__).resolve().parent.parent.parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

load_dotenv(BASE_DIR / ".env")

# =========================================================
# SPARK
# =========================================================
def create_spark():

    return (
        SparkSession.builder
        .appName("Silver -> Gold Dimensional Layer")

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

        # ---------- SILVER ----------
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.silver.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog"
        )
        .config("spark.sql.catalog.silver.uri", "http://nessie:19120/api/v2")
        .config("spark.sql.catalog.silver.ref", "main")
        .config("spark.sql.catalog.silver.authentication.type", "NONE")
        .config("spark.sql.catalog.silver.warehouse", "s3a://silver-layer")

        # ---------- GOLD ----------
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

# =========================================================
# HELPERS
# =========================================================
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

    # =====================================================
    # LOAD SILVER
    # =====================================================
    users = spark.table("silver.users")
    products = spark.table("silver.products")
    orders = spark.table("silver.orders")
    order_items = spark.table("silver.order_items")
    reviews = spark.table("silver.reviews")

    # =====================================================
    # DIM USERS
    # =====================================================
    dim_users = (
        users
        .select(
            "user_id",
            "first_name",
            "last_name",
            "email",
            "phone_number",
            "city",
            "country",
            "created_at"
        )

        .withColumn(
            "full_name",
            concat_ws(" ", col("first_name"), col("last_name"))
        )

        .dropDuplicates(["user_id"])
    )

    overwrite_table(
        dim_users,
        "gold.dim_users"
    )

    # =====================================================
    # DIM PRODUCTS
    # =====================================================
    dim_products = (
        products
        .select(
            "product_id",
            "product_name",
            "category_id",
            "category_name",
            "brand",
            "price",
            "updated_at"
        )

        .dropDuplicates(["product_id"])
    )

    overwrite_table(
        dim_products,
        "gold.dim_products"
    )

    # =====================================================
    # DIM DATE
    # =====================================================
    order_dates = (
        orders
        .select(
            to_date(col("order_date")).alias("date")
        )
    )

    review_dates = (
        reviews
        .select(
            to_date(col("review_date")).alias("date")
        )
    )

    dim_date = (
        order_dates
        .union(review_dates)

        .distinct()

        .withColumn(
            "date_key",
            date_format(col("date"), "yyyyMMdd").cast("int")
        )

        .withColumn(
            "year",
            year(col("date"))
        )

        .withColumn(
            "month",
            month(col("date"))
        )

        .withColumn(
            "day",
            dayofmonth(col("date"))
        )

        .withColumn(
            "quarter",
            quarter(col("date"))
        )

        .withColumn(
            "week_of_year",
            weekofyear(col("date"))
        )

        .withColumn(
            "day_name",
            date_format(col("date"), "EEEE")
        )

        .withColumn(
            "month_name",
            date_format(col("date"), "MMMM")
        )
    )

    overwrite_table(
        dim_date,
        "gold.dim_date"
    )

    # =====================================================
    # FACT SALES
    # =====================================================
    fact_sales = (
        orders
        .join(order_items, "order_id", "inner")

        .withColumn(
            "date_key",
            date_format(col("order_date"), "yyyyMMdd").cast("int")
        )

        .select(
            col("order_id"),
            col("order_item_id"),

            col("user_id"),
            col("product_id"),

            col("date_key"),

            col("quantity"),
            col("price"),
            col("item_total"),

            col("order_date").alias("event_time")
        )
    )

    overwrite_table(
        fact_sales,
        "gold.fact_sales"
    )

    # =====================================================
    # FACT REVIEWS
    # =====================================================
    fact_reviews = (
        reviews
        .withColumn(
            "date_key",
            date_format(col("review_date"), "yyyyMMdd").cast("int")
        )

        .select(
            "review_id",
            "user_id",
            "product_id",
            "date_key",
            "rating",
            "review_text",
            col("review_date").alias("event_time")
        )
    )

    overwrite_table(
        fact_reviews,
        "gold.fact_reviews"
    )

    # =====================================================
    # FACT USER INTERACTIONS
    # =====================================================
    purchase_interactions = (
        fact_sales
        .select(
            "user_id",
            "product_id",

            lit("purchase").alias("interaction_type"),

            lit(3.0).alias("interaction_score"),

            col("event_time")
        )
    )

    review_interactions = (
        fact_reviews
        .select(
            "user_id",
            "product_id",

            lit("review").alias("interaction_type"),

            (col("rating") / 5.0).alias("interaction_score"),

            col("event_time")
        )
    )

    fact_user_interactions = (
        purchase_interactions
        .unionByName(review_interactions)
    )

    overwrite_table(
        fact_user_interactions,
        "gold.fact_user_interactions"
    )

    logger.info("Gold transformation completed")


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":

    spark = create_spark()

    try:
        transform(spark)

    finally:
        spark.stop()