import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg, month, year

def create_spark():
    return (
        SparkSession.builder
        .appName("Transform Silver → Gold (Iceberg)")
        # MinIO config
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # Iceberg config
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.silver.type", "hadoop")
        .config("spark.sql.catalog.silver.warehouse", "s3a://silver-layer")
        .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.gold.type", "hadoop")
        .config("spark.sql.catalog.gold.warehouse", "s3a://gold-layer")
        .getOrCreate()
    )

def transform(spark):
    # đọc từ silver
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
    sales_summary.writeTo("gold.sales_summary").createOrReplace()

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
    review_summary.writeTo("gold.review_summary").createOrReplace()


if __name__ == "__main__":
    spark = create_spark()
    transform(spark)
    spark.stop()
