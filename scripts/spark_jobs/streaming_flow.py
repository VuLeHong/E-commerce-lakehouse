import os
import sys
import json
import logging
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, from_json, broadcast,
    collect_list, row_number
)
from pyspark.sql.types import *
from pyspark.sql.window import Window

import redis
from kafka import KafkaProducer

# =====================================================
# SETUP
# =====================================================
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("streaming-als")

# ===== Paths from training =====
USER_FACTORS_PATH = "s3a://gold-layer/ml/als/user_factors"
ITEM_FACTORS_PATH = "s3a://gold-layer/ml/als/item_factors"

USER_MAPPING_PATH = "s3a://gold-layer/ml/als/user_mapping"
ITEM_MAPPING_PATH = "s3a://gold-layer/ml/als/item_mapping"

CANDIDATE_POOL_SIZE = int(os.getenv("CANDIDATE_POOL_SIZE", 200))
FINAL_TOPK = int(os.getenv("FINAL_TOPK", 10))

# =====================================================
# SPARK
# =====================================================
def create_spark():
    return (
        SparkSession.builder
        .appName("Realtime-ALS-Serving")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        )
        .getOrCreate()
    )

# =====================================================
# DOT PRODUCT (ALS)
# =====================================================
DOT_EXPR = """
aggregate(
  zip_with(u_features, i_features, (x, y) -> x * y),
  0D,
  (acc, x) -> acc + x
)
"""

# =====================================================
# STREAMING JOB
# =====================================================
def streaming_job(spark: SparkSession):

    # ---------- Load ALS artifacts ----------
    user_factors = (
        spark.read.parquet(USER_FACTORS_PATH)
        .withColumnRenamed("id", "userIdx")
        .withColumnRenamed("features", "u_features")
    )

    item_factors = (
        spark.read.parquet(ITEM_FACTORS_PATH)
        .withColumnRenamed("id", "itemIdx")
        .withColumnRenamed("features", "i_features")
    )

    user_mapping = spark.read.parquet(USER_MAPPING_PATH)
    item_mapping = spark.read.parquet(ITEM_MAPPING_PATH)

    # ---------- Broadcast ----------
    user_factors_b = broadcast(user_factors)
    user_mapping_b = broadcast(user_mapping)
    item_mapping_b = broadcast(item_mapping)

    candidate_items_b = broadcast(
        item_factors.orderBy(expr("rand()")).limit(CANDIDATE_POOL_SIZE)
    )

    logger.info("✅ ALS artifacts loaded & broadcasted")

    # ---------- Redis ----------
    r = redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        decode_responses=True
    )
    r.ping()

    # ---------- Kafka Producer ----------
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )

    # ---------- Kafka Stream ----------
    events = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
        .option("subscribe", "events.page_view,events.add_to_cart")
        .load()
    )

    # 🔥 FIX: rename product_id from event
    parsed = (
        events
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(
            from_json(
                col("json_str"),
                StructType([
                    StructField("event_id", StringType()),
                    StructField("user_id", StringType()),
                    StructField("product_id", StringType()),
                    StructField("timestamp", StringType()),
                ])
            ).alias("data")
        )
        .select(
            col("data.user_id"),
            col("data.product_id").alias("event_product_id"),
            col("data.timestamp")
        )
        .filter(col("user_id").isNotNull())
    )

    # =================================================
    # foreachBatch
    # =================================================
    def process_batch(batch_df, batch_id):
        start_time = datetime.now()

        if batch_df.isEmpty():
            logger.debug(f"[Batch {batch_id}] empty")
            return

        input_rows = batch_df.count()
        logger.info(f"[Batch {batch_id}] input_rows={input_rows}")

        try:
            base = (
                batch_df
                .join(user_mapping_b, "user_id", "inner")
                .join(user_factors_b, "userIdx", "inner")
            )

            scored = (
                base
                .crossJoin(candidate_items_b)
                .withColumn("score", expr(DOT_EXPR))
                .join(item_mapping_b, "itemIdx", "left")
            )

            w = Window.partitionBy("user_id").orderBy(col("score").desc())

            topk = (
                scored
                .withColumn("rn", row_number().over(w))
                .filter(col("rn") <= FINAL_TOPK)
                .groupBy("user_id")
                .agg(
                    collect_list(col("product_id")).alias("candidates")
                )
            )

            result_rows = topk.collect()

        except Exception:
            logger.exception(f"[Batch {batch_id}] scoring / topK failed")
            return

        messages = []

        for row in result_rows:
            user_id = row["user_id"]
            candidates = [x for x in row["candidates"] if x]
            if not candidates:
                continue

            views = r.lrange(f"user:{user_id}:views", 0, 9)
            cart = r.lrange(f"user:{user_id}:cart", 0, 9)

            boosted = list(cart) + list(views) + [
                x for x in candidates if x not in cart and x not in views
            ]

            final, seen = [], set()
            for pid in boosted:
                if pid not in seen:
                    final.append(pid)
                    seen.add(pid)
                if len(final) >= FINAL_TOPK:
                    break

            key = f"recommend:realtime:{user_id}"
            r.delete(key)
            r.rpush(key, *final)
            r.expire(key, 300)

            messages.append({
                "user_id": user_id,
                "recommendations": final
            })

        emitted_users = len(messages)

        end_time = datetime.now()
        latency = (end_time - start_time).total_seconds()
        throughput = input_rows / latency if latency > 0 else 0.0

        for msg in messages:
            producer.send(
                "recommend.reranked",
                key=msg["user_id"],
                value={
                    "user_id": msg["user_id"],
                    "recommendations": msg["recommendations"],
                    "source": "als_realtime",
                    "metrics": {
                        "batch_id": batch_id,
                        "input_rows": input_rows,
                        "emitted_users": emitted_users,
                        "latency_seconds": latency,
                        "throughput_events_per_sec": throughput
                    }
                }
            )

        producer.flush()

        logger.info(f"[Batch {batch_id}] latency={latency:.3f} seconds")
        logger.info(f"[Batch {batch_id}] throughput={throughput:.2f} events/sec")
        logger.info(f"[Batch {batch_id}] emitted_users={emitted_users}")
        
    (
        parsed.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .start()
    )

    spark.streams.awaitAnyTermination()

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    spark = create_spark()
    streaming_job(spark)
