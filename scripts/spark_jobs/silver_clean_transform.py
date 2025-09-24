import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lower, trim

def create_spark():
    return (
        SparkSession.builder
        .appName("Transform Bronze → Silver (Iceberg)")
        # MinIO config
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # Iceberg config
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.silver.type", "hadoop")
        .config("spark.sql.catalog.silver.warehouse", "s3a://silver-layer")
        .getOrCreate()
    )

def transform(spark):
    bronze = "s3a://bronze-layer"

    # --- DIM PRODUCTS ---
    products = spark.read.parquet(f"{bronze}/brz.products")
    categories = spark.read.parquet(f"{bronze}/brz.categories")

    dim_products = (
        products.join(categories, "category_id", "left")
                .filter(col("product_id").isNotNull() & col("category_id").isNotNull())
                .dropDuplicates(["product_id"])
    )
    dim_products.writeTo("silver.dim_products").createOrReplace()

    # --- DIM USERS ---
    users = spark.read.parquet(f"{bronze}/brz.users")

    dim_users = (
        users.filter(col("user_id").isNotNull())
             .dropDuplicates(["user_id"])
             .withColumn("email", lower(trim(col("email"))))
    )
    dim_users.writeTo("silver.dim_users").createOrReplace()

    # --- FACT PURCHASE EVENT ---
    orders = spark.read.parquet(f"{bronze}/brz.orders")
    order_items = spark.read.parquet(f"{bronze}/brz.order_items")
    purchase_event = spark.read.parquet(f"{bronze}/brz.purchase_event")

    # chuẩn hóa orders + order_items thành dạng purchase
    from_orders = (
        orders.join(order_items, "order_id", "inner")
              .filter((col("quantity") > 0) & (col("price") > 0))
              .select(
                  "user_id",
                  "product_id",
                  "quantity",
                  "price",
                  col("order_date").alias("event_time")
              )
    )

    # chuẩn hóa purchase_event (streaming)
    from_events = (
        purchase_event
            .filter((col("quantity") > 0) & (col("price") > 0))
            .select(
                "user_id",
                "product_id",
                "quantity",
                "price",
                coalesce(col("timestamp"), col("event_time")).alias("event_time")
            )
    )

    # gộp lại thành fact_purchase_event
    fact_purchase = from_orders.unionByName(from_events, allowMissingColumns=True)

    # validate user_id & product_id tồn tại
    fact_purchase = (
        fact_purchase.join(dim_users, "user_id", "inner")
                     .join(dim_products, "product_id", "inner")
                     .select("user_id", "product_id", "quantity", "price", "event_time")
    )

    fact_purchase.writeTo("silver.fact_purchase_event").createOrReplace()

    # --- FACT REVIEWS ---
    reviews = spark.read.parquet(f"{bronze}/brz.reviews")
    review_event = spark.read.parquet(f"{bronze}/brz.review_event")

    fact_reviews = (
        reviews.unionByName(review_event, allowMissingColumns=True)
               .withColumn("review_date", coalesce(col("review_date"), col("timestamp")))
               .drop("timestamp")
               .filter(
                   col("user_id").isNotNull() &
                   col("product_id").isNotNull() &
                   (col("rating").between(1, 5))
               )
    )

    fact_reviews = (
        fact_reviews.join(dim_users, "user_id", "inner")
                    .join(dim_products, "product_id", "inner")
                    .select("review_id", "user_id", "product_id", "rating", "review_text", "review_date")
    )

    fact_reviews.writeTo("silver.fact_reviews").createOrReplace()


if __name__ == "__main__":
    spark = create_spark()
    transform(spark)
    spark.stop()
