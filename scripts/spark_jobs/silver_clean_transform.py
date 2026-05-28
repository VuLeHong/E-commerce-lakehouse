import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import logging
from pathlib import Path

from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException


# =========================================================
# SETUP
# =========================================================
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
BRONZE_META_COLS = [
    "_bronze_year",
    "_bronze_month",
    "_bronze_day"
]


def safe_read_parquet(spark, path):
    """
    Read a bronze parquet path.

    Bronze is partitioned by year/month/day.
    Instead of dropping these columns immediately, keep them as
    _bronze_year/_bronze_month/_bronze_day so silver can use them
    as a tie-breaker when selecting latest records.
    """
    try:
        df = spark.read.parquet(path)

        rename_map = {
            "year": "_bronze_year",
            "month": "_bronze_month",
            "day": "_bronze_day"
        }

        for old_col, new_col in rename_map.items():
            if old_col in df.columns:
                df = df.withColumnRenamed(old_col, new_col)

        return df

    except AnalysisException:
        logger.warning(f"Path not found: {path}")
        return None


def drop_bronze_metadata(df):
    """
    Remove bronze partition metadata before writing to silver tables.
    """
    drop_cols = [c for c in BRONZE_META_COLS if c in df.columns]
    return df.drop(*drop_cols) if drop_cols else df


def latest_by_key(df, keys, order_cols):
    """
    Select latest record per business key from append-only bronze data.

    keys:
        Business key columns, for example ["product_id"].

    order_cols:
        Columns used to define latest record.
        Example: ["updated_at", "_bronze_year", "_bronze_month", "_bronze_day"].
    """
    valid_order_cols = [c for c in order_cols if c in df.columns]

    if not valid_order_cols:
        logger.warning(
            f"No valid order columns found for keys={keys}. "
            f"Fallback to dropDuplicates({keys})."
        )
        return df.dropDuplicates(keys)

    window_spec = (
        Window
        .partitionBy(*[F.col(k) for k in keys])
        .orderBy(*[
            F.col(c).desc_nulls_last()
            for c in valid_order_cols
        ])
    )

    return (
        df
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


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
    users = safe_read_parquet(
        spark,
        f"{bronze}/brz.users"
    )

    if users is not None:

        users_clean = (
            users
            .filter(F.col("user_id").isNotNull())

            .withColumn(
                "user_id",
                F.col("user_id").cast("int")
            )

            .withColumn(
                "email",
                F.lower(F.trim(F.col("email")))
            )

            .withColumn(
                "first_name",
                F.initcap(F.trim(F.col("first_name")))
            )

            .withColumn(
                "last_name",
                F.initcap(F.trim(F.col("last_name")))
            )

            .withColumn(
                "phone_number",
                F.trim(F.col("phone_number"))
            )

            .withColumn(
                "address",
                F.trim(F.col("address"))
            )

            .withColumn(
                "country",
                F.upper(F.trim(F.col("country")))
            )

            .withColumn(
                "city",
                F.initcap(F.trim(F.col("city")))
            )

            .withColumn(
                "created_at",
                F.to_timestamp(F.col("created_at"))
            )
        )

        # users source only has created_at, not updated_at.
        # So latest is based on created_at + bronze partition tie-breaker.
        silver_users = (
            latest_by_key(
                users_clean,
                keys=["user_id"],
                order_cols=[
                    "created_at",
                    "_bronze_year",
                    "_bronze_month",
                    "_bronze_day"
                ]
            )
            .select(
                "user_id",
                "first_name",
                "last_name",
                "email",
                "phone_number",
                "address",
                "city",
                "country",
                "created_at"
            )
        )

        overwrite_table(
            silver_users,
            "silver.users"
        )

    # =====================================================
    # CATEGORIES
    # Used for enriching products
    # =====================================================
    categories = safe_read_parquet(
        spark,
        f"{bronze}/brz.categories"
    )

    categories_latest = None

    if categories is not None:

        categories_clean = (
            categories
            .filter(F.col("category_id").isNotNull())

            .withColumn(
                "category_id",
                F.col("category_id").cast("int")
            )

            .withColumn(
                "category_name",
                F.initcap(F.trim(F.col("category_name")))
            )

            .withColumn(
                "updated_at",
                F.to_timestamp(F.col("updated_at"))
            )
        )

        categories_latest = (
            latest_by_key(
                categories_clean,
                keys=["category_id"],
                order_cols=[
                    "updated_at",
                    "_bronze_year",
                    "_bronze_month",
                    "_bronze_day"
                ]
            )
            .select(
                "category_id",
                "category_name"
            )
        )

    # =====================================================
    # PRODUCTS
    # =====================================================
    products = safe_read_parquet(
        spark,
        f"{bronze}/brz.products"
    )

    if products is not None:

        products_clean = (
            products
            .filter(F.col("product_id").isNotNull())

            .withColumn(
                "product_id",
                F.col("product_id").cast("int")
            )

            .withColumn(
                "category_id",
                F.col("category_id").cast("int")
            )

            .withColumn(
                "product_name",
                F.trim(F.col("product_name"))
            )

            .withColumn(
                "brand",
                F.upper(F.trim(F.col("brand")))
            )

            .withColumn(
                "price",
                F.round(F.col("price"), 2)
            )

            .filter(F.col("price") > 0)

            .withColumn(
                "updated_at",
                F.to_timestamp(F.col("updated_at"))
            )
        )

        products_latest = latest_by_key(
            products_clean,
            keys=["product_id"],
            order_cols=[
                "updated_at",
                "_bronze_year",
                "_bronze_month",
                "_bronze_day"
            ]
        )

        if categories_latest is not None:
            silver_products = (
                products_latest
                .join(
                    categories_latest,
                    on="category_id",
                    how="left"
                )
                .select(
                    "product_id",
                    "product_name",
                    "category_id",
                    "category_name",
                    "brand",
                    "price",
                    "updated_at"
                )
            )
        else:
            silver_products = (
                products_latest
                .withColumn(
                    "category_name",
                    F.lit(None).cast("string")
                )
                .select(
                    "product_id",
                    "product_name",
                    "category_id",
                    "category_name",
                    "brand",
                    "price",
                    "updated_at"
                )
            )

        overwrite_table(
            silver_products,
            "silver.products"
        )

    # =====================================================
    # ORDERS
    # =====================================================
    orders = safe_read_parquet(
        spark,
        f"{bronze}/brz.orders"
    )

    if orders is not None:

        orders_clean = (
            orders
            .filter(F.col("order_id").isNotNull())
            .filter(F.col("user_id").isNotNull())

            .withColumn(
                "order_id",
                F.col("order_id").cast("int")
            )

            .withColumn(
                "user_id",
                F.col("user_id").cast("int")
            )

            .withColumn(
                "total_price",
                F.round(F.col("total_price"), 2)
            )

            .filter(F.col("total_price") > 0)

            .withColumn(
                "order_date",
                F.to_timestamp(F.col("order_date"))
            )
        )

        # orders are fact-like records.
        # If duplicate order_id appears because of rerun/replay, keep latest order_date.
        silver_orders = (
            latest_by_key(
                orders_clean,
                keys=["order_id"],
                order_cols=[
                    "order_date",
                    "_bronze_year",
                    "_bronze_month",
                    "_bronze_day"
                ]
            )
            .select(
                "order_id",
                "user_id",
                "total_price",
                "order_date"
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

        order_items_clean = (
            order_items
            .filter(F.col("order_item_id").isNotNull())
            .filter(F.col("order_id").isNotNull())
            .filter(F.col("product_id").isNotNull())

            .withColumn(
                "order_item_id",
                F.col("order_item_id").cast("int")
            )

            .withColumn(
                "order_id",
                F.col("order_id").cast("int")
            )

            .withColumn(
                "product_id",
                F.col("product_id").cast("int")
            )

            .withColumn(
                "quantity",
                F.col("quantity").cast("int")
            )

            .filter(F.col("quantity") > 0)

            .withColumn(
                "price",
                F.round(F.col("price"), 2)
            )

            .filter(F.col("price") > 0)

            .withColumn(
                "item_total",
                F.round(F.col("item_total"), 2)
            )

            .filter(F.col("item_total") > 0)
        )

        # order_items has no timestamp in source.
        # Bronze batch and streaming both partition order_items by current_date().
        # Use bronze partition as deterministic tie-breaker if duplicate item id exists.
        silver_order_items = (
            latest_by_key(
                order_items_clean,
                keys=["order_item_id"],
                order_cols=[
                    "_bronze_year",
                    "_bronze_month",
                    "_bronze_day"
                ]
            )
            .select(
                "order_item_id",
                "order_id",
                "product_id",
                "quantity",
                "price",
                "item_total"
            )
        )

        overwrite_table(
            silver_order_items,
            "silver.order_items"
        )

    # =====================================================
    # REVIEWS
    # =====================================================
    reviews = safe_read_parquet(
        spark,
        f"{bronze}/brz.reviews"
    )

    if reviews is not None:

        reviews_clean = (
            reviews
            .filter(F.col("review_id").isNotNull())
            .filter(F.col("user_id").isNotNull())
            .filter(F.col("product_id").isNotNull())

            .withColumn(
                "review_id",
                F.col("review_id").cast("int")
            )

            .withColumn(
                "user_id",
                F.col("user_id").cast("int")
            )

            .withColumn(
                "product_id",
                F.col("product_id").cast("int")
            )

            .withColumn(
                "rating",
                F.col("rating").cast("int")
            )

            .filter(F.col("rating").between(1, 5))

            .withColumn(
                "review_text",
                F.trim(F.col("review_text"))
            )

            .withColumn(
                "review_date",
                F.to_timestamp(F.col("review_date"))
            )
        )

        # reviews are also event/fact-like records.
        # If duplicate review_id exists, keep the latest review_date.
        silver_reviews = (
            latest_by_key(
                reviews_clean,
                keys=["review_id"],
                order_cols=[
                    "review_date",
                    "_bronze_year",
                    "_bronze_month",
                    "_bronze_day"
                ]
            )
            .select(
                "review_id",
                "user_id",
                "product_id",
                "rating",
                "review_text",
                "review_date"
            )
        )

        overwrite_table(
            silver_reviews,
            "silver.reviews"
        )

    logger.info("Silver transformation completed")


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":

    spark = create_spark()

    try:
        transform(spark)

    finally:
        spark.stop()