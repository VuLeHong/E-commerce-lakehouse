import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utils import check_minio_has_data
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

dotenv_path = BASE_DIR / ".env"
load_dotenv(dotenv_path)

def create_SparkSession() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Ingestion - Postgres & Kafka to MinIO Bronze")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )


def read_postgres_table(spark: SparkSession, table: str):
    host = "backend-postgres"
    port = "5432"
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    database = os.getenv("POSTGRES_DATABASE")

    return (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{host}:{port}/{database}")
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

def incremental_load_orders_with_items(spark: SparkSession) -> None:
    orders_path = "s3a://bronze-layer/brz.orders"
    order_items_path = "s3a://bronze-layer/brz.order_items"

    try:
        logger.info("[BRONZE][Orders] --- Start processing orders & order_items ---")
        orders_df = read_postgres_table(spark, "orders")
        order_items_df = read_postgres_table(spark, "order_items")

        if check_minio_has_data(bucket="bronze-layer", prefix="brz.orders/"):
            existing_orders = spark.read.parquet(orders_path)
            latest_time = existing_orders.select(max("order_date")).first()[0]
            new_orders = orders_df.filter(col("order_date") > latest_time)
            logger.info(f"[BRONZE][Orders] Found {new_orders.count()} new records since {latest_time}")
        else:
            logger.info("[BRONZE][Orders] No Bronze data found -> Full load")
            new_orders = orders_df

        if new_orders.rdd.isEmpty():
            logger.info("[BRONZE][Orders] No new orders to load. Skipping.")
            return

        enriched_orders = (
            new_orders.withColumn("year", year("order_date"))
                      .withColumn("month", month("order_date"))
                      .withColumn("day", dayofmonth("order_date"))
        )

        new_items = order_items_df.join(
            enriched_orders.select("order_id"),
            on="order_id",
            how="inner"
        ).withColumn("year", year(current_date())) \
         .withColumn("month", month(current_date())) \
         .withColumn("day", dayofmonth(current_date()))

        enriched_orders.write.partitionBy("year", "month", "day").mode("append").parquet(orders_path)
        new_items.write.partitionBy("year", "month", "day").mode("append").parquet(order_items_path)

        logger.info("[BRONZE][Orders] Successfully wrote Orders & Order_items.")

    except Exception as e:
        logger.error(f"[BRONZE][Orders] Error during load: {str(e)}")
        raise

def incremental_load_table(spark: SparkSession, table_name: str, time_col: str = None) -> None:
    bronze_path = f"s3a://bronze-layer/brz.{table_name}"
    try:
        logger.info(f"[BRONZE][{table_name}] --- Start processing ---")
        source_df = read_postgres_table(spark, table_name)

        if not time_col or time_col not in source_df.columns:
            logger.warning(f"[BRONZE][{table_name}] No time column -> always full load.")
            new_data = source_df
        else:
            if check_minio_has_data(bucket="bronze-layer", prefix=f"brz.{table_name}/"):
                existing_df = spark.read.parquet(bronze_path)
                latest_time = existing_df.select(max(time_col)).first()[0]
                new_data = source_df.filter(col(time_col) > latest_time)
            else:
                logger.info(f"[BRONZE][{table_name}] No Bronze data found -> Full load.")
                new_data = source_df

        if new_data.rdd.isEmpty():
            logger.info(f"[BRONZE][{table_name}] No new records to write. Skipping.")
            return

        if time_col and time_col in new_data.columns:
            output_df = (
                new_data.withColumn("year", year(col(time_col)))
                        .withColumn("month", month(col(time_col)))
                        .withColumn("day", dayofmonth(col(time_col)))
            )
        else:
            output_df = (
                new_data.withColumn("year", year(current_date()))
                        .withColumn("month", month(current_date()))
                        .withColumn("day", dayofmonth(current_date()))
            )

        logger.info(f"[BRONZE][{table_name}] Writing {output_df.count()} new records to Bronze...")
        output_df.write.partitionBy("year", "month", "day").mode("append").parquet(bronze_path)
        logger.info(f"[BRONZE][{table_name}] Successfully wrote to Bronze.")

    except Exception as e:
        logger.error(f"[BRONZE][{table_name}] Error during load: {str(e)}")
        raise

# ============ MAIN ============

def main():
    logger.info("======== [BRONZE] START ========")
    try:
        spark = create_SparkSession()

        # Batch load
        incremental_load_orders_with_items(spark)

        dimension_tables = {
            "categories": "updated_at",
            "products": "updated_at",
            "users": "created_at",
            "reviews": "review_date",
        }
        for table, time_col in dimension_tables.items():
            incremental_load_table(spark, table, time_col)


        spark.stop()
        logger.info("======== [BRONZE] COMPLETED ========")
    except:
        sys.exit(1)

if __name__ == "__main__":
    main()
