import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg, month, year
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


def create_spark():
    return (
            SparkSession.builder
        .appName("Transform Silver → Gold (Iceberg + Nessie)")
        # --- MinIO config ---
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") 
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
        # .config("spark.sql.catalog.silver.type", "nessie")
        .config("spark.sql.catalog.silver.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.silver.uri", "http://nessie:19120/api/v2")
        .config("spark.sql.catalog.silver.ref", "main")
        .config("spark.sql.catalog.silver.authentication.type", "NONE")
        .config("spark.sql.catalog.silver.warehouse", "s3a://silver-layer")
        
        .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
        # .config("spark.sql.catalog.gold.type", "nessie")
        .config("spark.sql.catalog.gold.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.gold.uri", "http://nessie:19120/api/v2")
        .config("spark.sql.catalog.gold.ref", "main")
        .config("spark.sql.catalog.gold.authentication.type", "NONE")
        .config("spark.sql.catalog.gold.warehouse", "s3a://gold-layer")
        .getOrCreate()
    )


def transform(spark):
    fact_purchase = spark.table("silver.fact_purchase_event")
    fact_reviews = spark.table("silver.fact_reviews")
    dim_products = spark.table("silver.dim_products")

    # --- SALES SUMMARY ---
    sales_summary = (
        fact_purchase
        .join(dim_products, "product_id", "left")
        .withColumn("year", year(col("event_time")))
        .withColumn("month", month(col("event_time")))
        .groupBy("year", "month", "product_id", "product_name", "category_id", "category_name")
        .agg(
            _sum(col("quantity")).alias("total_quantity"),
            _sum(col("quantity") * col("price")).alias("total_sales"),
            count("*").alias("num_purchases")
        )
    )
    sales_summary.writeTo("gold.sales_summary") \
                .partitionedBy("year", "month") \
                .option("merge-schema", "true") \
                .tableProperty("format-version", "1") \
                .createOrReplace()


    # --- REVIEW SUMMARY ---
    review_summary = (
        fact_reviews
        .join(dim_products, "product_id", "left")
        .withColumn("year", year(col("review_date")))
        .withColumn("month", month(col("review_date")))
        .groupBy("year", "month", "product_id", "product_name", "category_id", "category_name")
        .agg(
            count("*").alias("num_reviews"),
            avg("rating").alias("avg_rating")
        )
    )
    review_summary.writeTo("gold.review_summary") \
                .partitionedBy("year", "month") \
                .option("merge-schema", "true") \
                .tableProperty("format-version", "1") \
                .createOrReplace()



if __name__ == "__main__":
    spark = create_spark()
    transform(spark)
    spark.stop()
