import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import json
import time
import logging
from pathlib import Path
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import redis
from kafka import KafkaProducer

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
        .appName("Streaming - Kafka to Lakehouse + Redis + Rerank")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )

# ====== Schema ======
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

# ====== Rerank helper ======
def rerank_simple(offline_recs, views, cart):
    if offline_recs is None:
        offline_recs = []
    if views is None:
        views = []
    if cart is None:
        cart = []
    boosted = list(cart) + list(views) + [x for x in offline_recs if x not in cart and x not in views]
    # dedupe giữ thứ tự, giới hạn 10
    seen, out = set(), []
    for pid in boosted:
        if pid not in seen:
            out.append(pid)
            seen.add(pid)
        if len(out) >= 10:
            break
    return out

# ====== foreachBatch Sinks ======
def write_to_redis(batch_df, batch_id):
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    r = redis.Redis(host=redis_host, port=redis_port, db=0)

    rows = batch_df.collect()
    for row in rows:
        rowd = row.asDict()
        user_id = rowd.get("user_id")
        product_id = rowd.get("product_id")
        event_type = rowd.get("event_type")

        if not user_id or not product_id:
            continue

        if event_type == "page_view":
            key = f"user:{user_id}:views"
        elif event_type == "add_to_cart":
            key = f"user:{user_id}:cart"
        else:
            continue

        r.lpush(key, product_id)
        r.ltrim(key, 0, 49)
        r.expire(key, 24 * 3600)

    r.close()


def write_rerank_kafka(batch_df, batch_id):
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    r = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        acks="all",
        retries=5,
        linger_ms=20,
        batch_size=32 * 1024,
        compression_type="snappy",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    topic = os.getenv("RERANK_TOPIC", "recommend.reranked")

    rows = batch_df.collect()
    for row in rows:
        rowd = row.asDict()
        user_id = rowd.get("user_id")
        if not user_id:
            continue

        product_id = rowd.get("product_id")
        event_id = rowd.get("event_id")
        event_type = rowd.get("event_type")

        offline_recs = r.lrange(f"recommend:offline:{user_id}", 0, -1) or []
        views = r.lrange(f"user:{user_id}:views", 0, 9) or []
        cart = r.lrange(f"user:{user_id}:cart", 0, 9) or []

        final_recs = rerank_simple(offline_recs, views, cart)

        payload = {
            "user_id": user_id,
            "event_id": event_id,
            "event_type": event_type,
            "product_trigger": product_id,
            "timestamp": int(time.time()),
            "recommendations": final_recs,
            "baseline_len": len(offline_recs),
            "views_len": len(views),
            "cart_len": len(cart),
        }

        producer.send(topic, key=user_id, value=payload)

    producer.flush()
    producer.close()
    r.close()

# ====== Streaming Job ======
def streaming_load_events(spark: SparkSession) -> None:
    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    TOPIC_PREFIX = os.getenv("EVENT_TOPIC_PREFIX", "events")
    CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "s3a://checkpoints")
    EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "review"]

    for etype in EVENT_TYPES:
        topic = f"{TOPIC_PREFIX}.{etype}"
        bronze_path = f"s3a://bronze-layer/brz.{etype}_event"

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

        # Sink 1: Lakehouse
        (
            df_parsed.writeStream
            .format("parquet")
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/{etype}")
            .option("path", bronze_path)
            .partitionBy("year", "month", "day")
            .outputMode("append")
            .start()
        )
        logger.info(f"[BRONZE][{etype}] Streaming write -> {bronze_path}")

        # Sink 2: Realtime
        if etype in ["page_view", "add_to_cart"]:
            df_for_realtime = (
                df_parsed
                .filter(col("user_id").isNotNull() & col("product_id").isNotNull())
                .withColumn("event_type", lit(etype))
            )

            # Redis cache 
            (
                df_for_realtime.writeStream
                .foreachBatch(write_to_redis)
                .outputMode("append")
                .start()
            )
            logger.info(f"[REDIS][{etype}] Streaming write -> Redis")

            # Rerank -> Kafka
            (
                df_for_realtime.writeStream
                .foreachBatch(write_rerank_kafka)
                .outputMode("append")
                .option("checkpointLocation", f"{CHECKPOINT_BASE}/rerank_{etype}")
                .start()
            )
            logger.info(f"[RERANK][{etype}] Streaming rerank -> Kafka recommend.reranked")

    spark.streams.awaitAnyTermination()

# ====== Main ======
def main():
    logger.info("======== [STREAMING FLOW START] ========")
    try:
        logger.info(f"MINIO_ENDPOINT={os.getenv('MINIO_ENDPOINT')}")
        logger.info(f"MINIO_ACCESS_KEY={os.getenv('MINIO_ACCESS_KEY')}")
        logger.info(f"KAFKA_BOOTSTRAP_SERVERS={os.getenv('KAFKA_BOOTSTRAP_SERVERS')}")
        logger.info(f"REDIS_HOST={os.getenv('REDIS_HOST')}")
        logger.info(f"REDIS_PORT={os.getenv('REDIS_PORT')}")
        
        spark = create_SparkSession()
        streaming_load_events(spark)
        logger.info("======== [STREAMING FLOW COMPLETED] ========")
    except Exception as e:
        logger.error(f"Streaming job failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
