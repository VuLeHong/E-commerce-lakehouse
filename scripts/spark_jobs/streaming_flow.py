import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import redis
from pyspark.sql.streaming import ForeachWriter

# ====== Setup ======
BASE_DIR = Path(__file__).resolve().parent.parent.parent
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

dotenv_path = BASE_DIR / ".env"
load_dotenv(dotenv_path)

# ====== Spark Session ======
def create_SparkSession() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Streaming - Kafka to Lakehouse + Redis")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )

# ====== Schema cho từng event ======
def get_event_schema(event_type: str) -> StructType:
    base_fields = [
        StructField("event_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("user_id", StringType(), True),
    ]

    if event_type == "page_view":
        extra = [StructField("product_id", StringType(), True)]
    elif event_type == "add_to_cart":
        extra = [StructField("product_id", StringType(), True),
                 StructField("quantity", IntegerType(), True)]
    elif event_type == "purchase":
        extra = [StructField("product_id", StringType(), True),
                 StructField("quantity", IntegerType(), True),
                 StructField("price", DoubleType(), True)]
    elif event_type == "review":
        extra = [StructField("product_id", StringType(), True),
                 StructField("rating", IntegerType(), True),
                 StructField("review_text", StringType(), True)]
    else:
        extra = []

    return StructType(base_fields + extra)

# ====== Redis Sink ======
class RedisSink(ForeachWriter):
    def open(self, partition_id, epoch_id):
        # Redis service name trong docker-compose phải là "redis"
        self.r = redis.Redis(host="redis", port=6379, db=0)
        return True

    def process(self, row):
        try:
            user_id = row["user_id"]
            product_id = row["product_id"]
            event_type = row["event_type"]

            if not user_id or not product_id:
                return

            if event_type == "page_view":
                key = f"user:{user_id}:views"
            elif event_type == "add_to_cart":
                key = f"user:{user_id}:cart"
            else:
                return

            # Push sản phẩm vào list
            self.r.lpush(key, product_id)
            # Giữ tối đa 50 sản phẩm gần nhất
            self.r.ltrim(key, 0, 49)
            # Đặt TTL (vd 24h)
            self.r.expire(key, 24 * 3600)

        except Exception as e:
            logger.error(f"RedisSink error: {e}")

    def close(self, error):
        if hasattr(self, "r"):
            self.r.close()

# ====== Streaming Job ======
def streaming_load_events(spark: SparkSession) -> None:
    BOOTSTRAP_SERVERS = "kafka:9092"
    TOPIC_PREFIX = "events"
    EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "review"]

    for etype in EVENT_TYPES:
        topic = f"{TOPIC_PREFIX}.{etype}"
        bronze_path = f"s3a://bronze-layer/brz.{etype}_event"

        # Đọc từ Kafka
        df_raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )

        logger.info(f"[{etype}] --- Start processing ---")

        df_parsed = (
            df_raw.selectExpr("CAST(value AS STRING) as json_str")
                  .select(from_json(col("json_str"), get_event_schema(etype)).alias("data"))
                  .select("data.*")
                  .withColumn("year", year(current_timestamp()))
                  .withColumn("month", month(current_timestamp()))
                  .withColumn("day", dayofmonth(current_timestamp()))
        )

        # Sink 1: Lakehouse (Parquet Bronze)
        (
            df_parsed.writeStream
            .format("parquet")
            .option("checkpointLocation", f"s3a://bronze-layer/_checkpoints/{etype}")
            .option("path", bronze_path)
            .partitionBy("year", "month", "day")
            .outputMode("append")
            .start()
        )
        logger.info(f"[BRONZE][{etype}] Streaming write -> {bronze_path}")

        # Sink 2: Redis (chỉ cho page_view & add_to_cart)
        if etype in ["page_view", "add_to_cart"]:
            df_for_redis = (
                df_parsed
                .filter(col("user_id").isNotNull() & col("product_id").isNotNull())
                .withColumn("event_type", lit(etype))
            )

            (
                df_for_redis.writeStream
                .foreach(RedisSink())
                .outputMode("append")
                .start()
            )
            logger.info(f"[REDIS][{etype}] Streaming write -> Redis")

    spark.streams.awaitAnyTermination()

# ====== Main ======
def main():
    logger.info("======== [STREAMING FLOW START] ========")
    try:
        spark = create_SparkSession()
        streaming_load_events(spark)
        spark.stop()
        logger.info("======== [STREAMING FLOW COMPLETED] ========")
    except Exception as e:
        logger.error(f"Streaming job failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
