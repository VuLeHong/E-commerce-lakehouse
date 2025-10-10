import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lower, trim, year, month
from pyspark.sql.utils import AnalysisException
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

dotenv_path = BASE_DIR / ".env"
load_dotenv(dotenv_path)

# ===================== Spark Session ===================== #
def create_spark():
    return (
        SparkSession.builder
        .appName("Transform Bronze → Silver (Iceberg)")
        # --- MinIO ---
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions, org.projectnessie.spark.extensions.NessieSparkSessionExtensions") 
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
        # .config("spark.sql.catalog.silver.type", "nessie")
        .config("spark.sql.catalog.silver.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.silver.uri", "http://nessie:19120/api/v2")
        .config("spark.sql.catalog.silver.ref", "main")
        .config("spark.sql.catalog.silver.authentication.type", "NONE")
        .config("spark.sql.catalog.silver.warehouse", "s3a://silver-layer")
        .getOrCreate()
    )

# ===================== Helpers ===================== #
def drop_if_exists(spark, table_name: str):
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        logger.info(f"[SILVER] Dropped old table: {table_name}")
    except Exception as e:
        logger.warning(f"[SILVER] Failed to drop {table_name}: {e}")

def safe_read_parquet(spark, path: str):
    try:
        df = spark.read.parquet(path)
        for c in ["year", "month", "day"]:
            if c in df.columns:
                df = df.drop(c)
        return df
    except AnalysisException:
        logger.warning(f"[BRONZE] Path not found, skipping: {path}")
        return None

# ===================== Transform Logic ===================== #
def transform(spark):
    bronze = "s3a://bronze-layer"

    # --- DIM PRODUCTS ---
    products = safe_read_parquet(spark, f"{bronze}/brz.products")
    categories = safe_read_parquet(spark, f"{bronze}/brz.categories")
    if products is not None and categories is not None:
        dim_products = (
            products.select("product_id", "product_name", "category_id", "updated_at")
            .join(categories.select("category_id", "category_name"), "category_id", "left")
            .filter(col("product_id").isNotNull())
            .dropDuplicates(["product_id"])
        )
        drop_if_exists(spark, "silver.dim_products")
        dim_products.writeTo("silver.dim_products") \
            .createOrReplace()
        logger.info("[SILVER] dim_products created successfully")

    # --- DIM USERS ---
    users = safe_read_parquet(spark, f"{bronze}/brz.users")
    if users is not None:
        dim_users = (
            users.select("user_id", "first_name", "last_name", "email", "phone_number", "created_at")
            .filter(col("user_id").isNotNull())
            .dropDuplicates(["user_id"])
            .withColumn("email", lower(trim(col("email"))))
            .withColumn("full_name", trim(col("first_name")) + " " + trim(col("last_name")))
        )
        drop_if_exists(spark, "silver.dim_users")
        dim_users.writeTo("silver.dim_users") \
            .createOrReplace()
        logger.info("[SILVER] dim_users created successfully")

    # --- FACT PURCHASE EVENT ---
    orders = safe_read_parquet(spark, f"{bronze}/brz.orders")
    order_items = safe_read_parquet(spark, f"{bronze}/brz.order_items")
    if orders is not None and order_items is not None:
        purchase_event = (
            orders.join(order_items, "order_id", "inner")
            .select("user_id", "product_id", "quantity", "price", col("order_date").alias("event_time"))
            .filter((col("quantity") > 0) & (col("price") > 0))
        )
        drop_if_exists(spark, "silver.fact_purchase_event")
        purchase_event.writeTo("silver.fact_purchase_event") \
            .createOrReplace()
        logger.info("[SILVER] fact_purchase_event created successfully")

    # --- FACT REVIEWS ---
    reviews = safe_read_parquet(spark, f"{bronze}/brz.reviews")
    if reviews is not None:
        fact_reviews = (
            reviews.filter(col("user_id").isNotNull() & col("product_id").isNotNull())
            .filter(col("rating").between(1, 5))
            .select("review_id", "user_id", "product_id", "rating", "review_text", "review_date")
        )
        drop_if_exists(spark, "silver.fact_reviews")
        fact_reviews.writeTo("silver.fact_reviews") \
            .createOrReplace()
        logger.info("[SILVER] fact_reviews created successfully")

# ===================== MAIN ===================== #
if __name__ == "__main__":
    spark = create_spark()
    try:
        transform(spark)
        logger.info("[SILVER] Transform completed successfully")
    finally:
        spark.stop()
