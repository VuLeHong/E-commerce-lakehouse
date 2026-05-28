import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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

logger = logging.getLogger("streaming-flow")


# =====================================================
# PATHS FROM ALS TRAINING
# =====================================================
USER_FACTORS_PATH = "s3a://gold-layer/ml/als/user_factors"
ITEM_FACTORS_PATH = "s3a://gold-layer/ml/als/item_factors"

USER_MAPPING_PATH = "s3a://gold-layer/ml/als/user_mapping"
ITEM_MAPPING_PATH = "s3a://gold-layer/ml/als/item_mapping"


# =====================================================
# BRONZE PATHS
# Must match bronze_batch_load.py output paths
# =====================================================
BRONZE_ORDERS_PATH = "s3a://bronze-layer/brz.orders"
BRONZE_ORDER_ITEMS_PATH = "s3a://bronze-layer/brz.order_items"
BRONZE_REVIEWS_PATH = "s3a://bronze-layer/brz.reviews"


# =====================================================
# STREAMING CONFIG
# =====================================================
SUBSCRIBE_TOPICS = (
    "events.page_view,"
    "events.add_to_cart,"
    "events.purchase,"
    "events.review"
)

SERVING_CHECKPOINT_PATH = "s3a://checkpoints/streaming_flow_serving"
LAKEHOUSE_CHECKPOINT_PATH = "s3a://checkpoints/streaming_flow_lakehouse"

SERVING_TRIGGER = os.getenv("SERVING_TRIGGER", "2 seconds")
LAKEHOUSE_TRIGGER = os.getenv("LAKEHOUSE_TRIGGER", "10 seconds")

SERVING_MAX_OFFSETS_PER_TRIGGER = os.getenv(
    "SERVING_MAX_OFFSETS_PER_TRIGGER",
    "100"
)

LAKEHOUSE_MAX_OFFSETS_PER_TRIGGER = os.getenv(
    "LAKEHOUSE_MAX_OFFSETS_PER_TRIGGER",
    "300"
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

CANDIDATE_POOL_SIZE = int(os.getenv("CANDIDATE_POOL_SIZE", 50))
FINAL_TOPK = int(os.getenv("FINAL_TOPK", 10))

# ID range for streaming-generated records.
# This avoids collision with batch source IDs.
STREAM_ID_OFFSET = 900000000
PARTITION_ID_MULTIPLIER = 1000000


# =====================================================
# SPARK
# =====================================================
def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Unified-Streaming-Flow")

        # ---------- MinIO ----------
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # ---------- Kafka + S3A ----------
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "org.apache.hadoop:hadoop-aws:3.3.1",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262"
            ])
        )

        .getOrCreate()
    )


# =====================================================
# DOT PRODUCT FOR ALS FACTORS
# =====================================================
DOT_EXPR = """
aggregate(
  zip_with(u_features, i_features, (x, y) -> x * y),
  0D,
  (acc, x) -> acc + x
)
"""


# =====================================================
# EVENT SCHEMA FROM KAFKA
# producer sends different fields depending on event type
# =====================================================
event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("_ingest_time", StringType()),

    StructField("user_id", StringType()),
    StructField("product_id", StringType()),

    StructField("quantity", IntegerType()),
    StructField("price", DoubleType()),

    StructField("rating", IntegerType()),
    StructField("review_text", StringType()),
])


# =====================================================
# PARSE KAFKA EVENTS
# =====================================================
def parse_events(events):
    return (
        events
        .select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("value").cast("string").alias("json_str")
        )
        .select(
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            F.from_json(
                F.col("json_str"),
                event_schema
            ).alias("data")
        )
        .select(
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",

            F.col("data.event_id").alias("event_id"),
            F.col("data.timestamp").alias("event_timestamp"),
            F.col("data._ingest_time").alias("_ingest_time"),

            F.col("data.user_id").alias("user_id"),
            F.col("data.product_id").alias("product_id"),

            F.col("data.quantity").alias("quantity"),
            F.col("data.price").alias("price"),

            F.col("data.rating").alias("rating"),
            F.col("data.review_text").alias("review_text")
        )
        .withColumn(
            "event_ts",
            F.coalesce(
                F.to_timestamp(F.col("event_timestamp")),
                F.col("kafka_timestamp")
            )
        )
        .filter(F.col("user_id").isNotNull())
    )


# =====================================================
# BRONZE WRITE
# =====================================================
def persist_purchase_review_to_bronze(batch_df):
    """
    Run inside Spark Structured Streaming foreachBatch.

    events.purchase -> brz.orders + brz.order_items
    events.review   -> brz.reviews
    """

    prepared = (
        batch_df
        .withColumn(
            "stream_id",
            (
                F.lit(STREAM_ID_OFFSET)
                + F.col("partition").cast("int") * F.lit(PARTITION_ID_MULTIPLIER)
                + F.col("offset").cast("int")
            ).cast("int")
        )
        .dropDuplicates(["topic", "partition", "offset"])
    )

    # =================================================
    # PURCHASE -> brz.orders + brz.order_items
    # =================================================
    purchase_events = (
        prepared
        .filter(F.col("topic") == "events.purchase")
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("quantity").isNotNull())
        .filter(F.col("price").isNotNull())
        .filter(F.col("quantity") > 0)
        .filter(F.col("price") > 0)
        .filter(F.col("event_ts").isNotNull())
    )

    if not purchase_events.isEmpty():

        stream_orders = (
            purchase_events
            .select(
                F.col("stream_id").cast("int").alias("order_id"),
                F.col("user_id").cast("int").alias("user_id"),
                F.round(
                    F.col("quantity") * F.col("price"),
                    2
                ).cast(DecimalType(10, 2)).alias("total_price"),
                F.col("event_ts").cast("timestamp").alias("order_date")
            )
            .withColumn("year", F.year(F.col("order_date")))
            .withColumn("month", F.month(F.col("order_date")))
            .withColumn("day", F.dayofmonth(F.col("order_date")))
        )

        (
            stream_orders.write
            .mode("append")
            .partitionBy("year", "month", "day")
            .parquet(BRONZE_ORDERS_PATH)
        )

        stream_order_items = (
            purchase_events
            .select(
                F.col("stream_id").cast("int").alias("order_item_id"),
                F.col("stream_id").cast("int").alias("order_id"),
                F.col("product_id").cast("int").alias("product_id"),
                F.col("quantity").cast("int").alias("quantity"),
                F.round(
                    F.col("price"),
                    2
                ).cast(DecimalType(10, 2)).alias("price"),
                F.round(
                    F.col("quantity") * F.col("price"),
                    2
                ).cast(DecimalType(10, 2)).alias("item_total")
            )
            # Same as bronze_batch_load.py:
            # order_items has no timestamp column, so partition by current_date()
            .withColumn("year", F.year(F.current_date()))
            .withColumn("month", F.month(F.current_date()))
            .withColumn("day", F.dayofmonth(F.current_date()))
        )

        (
            stream_order_items.write
            .mode("append")
            .partitionBy("year", "month", "day")
            .parquet(BRONZE_ORDER_ITEMS_PATH)
        )

        logger.info("[BRONZE] purchase events written to brz.orders and brz.order_items")

    # =================================================
    # REVIEW -> brz.reviews
    # =================================================
    review_events = (
        prepared
        .filter(F.col("topic") == "events.review")
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("rating").isNotNull())
        .filter(F.col("rating").between(1, 5))
        .filter(F.col("event_ts").isNotNull())
    )

    if not review_events.isEmpty():

        stream_reviews = (
            review_events
            .select(
                F.col("stream_id").cast("int").alias("review_id"),
                F.col("user_id").cast("int").alias("user_id"),
                F.col("product_id").cast("int").alias("product_id"),
                F.col("rating").cast("int").alias("rating"),
                F.trim(F.col("review_text")).alias("review_text"),
                F.col("event_ts").cast("timestamp").alias("review_date")
            )
            .withColumn("year", F.year(F.col("review_date")))
            .withColumn("month", F.month(F.col("review_date")))
            .withColumn("day", F.dayofmonth(F.col("review_date")))
        )

        (
            stream_reviews.write
            .mode("append")
            .partitionBy("year", "month", "day")
            .parquet(BRONZE_REVIEWS_PATH)
        )

        logger.info("[BRONZE] review events written to brz.reviews")


# =====================================================
# REDIS ACTIVITY UPDATE
# =====================================================
def update_redis_user_activity(batch_df, redis_client):
    """
    Store recent user behaviour in Redis for real-time reranking.
    """

    activity_rows = (
        batch_df
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .select(
            "topic",
            "user_id",
            "product_id",
            "rating"
        )
        .collect()
    )

    if not activity_rows:
        return

    pipe = redis_client.pipeline()

    for row in activity_rows:
        topic = row["topic"]
        user_id = str(row["user_id"])
        product_id = str(row["product_id"])
        rating = row["rating"]

        if topic == "events.page_view":
            key = f"user:{user_id}:views"
            pipe.lpush(key, product_id)
            pipe.ltrim(key, 0, 9)
            pipe.expire(key, 3600)

        elif topic == "events.add_to_cart":
            key = f"user:{user_id}:cart"
            pipe.lpush(key, product_id)
            pipe.ltrim(key, 0, 9)
            pipe.expire(key, 3600)

        elif topic == "events.purchase":
            key = f"user:{user_id}:purchases"
            pipe.lpush(key, product_id)
            pipe.ltrim(key, 0, 9)
            pipe.expire(key, 3600)

        elif topic == "events.review" and rating is not None and rating >= 4:
            key = f"user:{user_id}:positive_reviews"
            pipe.lpush(key, product_id)
            pipe.ltrim(key, 0, 9)
            pipe.expire(key, 3600)

    pipe.execute()

    logger.info(f"[REDIS] updated activity for {len(activity_rows)} events")


# =====================================================
# STREAMING JOB
# =====================================================
def streaming_job(spark: SparkSession):

    # =================================================
    # LOAD ALS ARTIFACTS
    # =================================================
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

    user_factors_b = F.broadcast(user_factors)
    user_mapping_b = F.broadcast(user_mapping)
    item_mapping_b = F.broadcast(item_mapping)

    candidate_items = (
        item_factors
        .orderBy(F.expr("rand()"))
        .limit(CANDIDATE_POOL_SIZE)
        .cache()
    )

    candidate_items.count()
    candidate_items_b = F.broadcast(candidate_items)

    logger.info("ALS artifacts loaded and broadcasted")
    logger.info(f"CANDIDATE_POOL_SIZE={CANDIDATE_POOL_SIZE}")
    logger.info(f"FINAL_TOPK={FINAL_TOPK}")

    # =================================================
    # REDIS
    # =================================================
    redis_client = redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        decode_responses=True
    )

    redis_client.ping()
    logger.info("Redis connected")

    # =================================================
    # KAFKA PRODUCER FOR RERANKED OUTPUT
    # =================================================
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )

    # =================================================
    # SERVING BATCH
    # Low-latency path:
    # Kafka events -> Redis -> ALS rerank -> recommend.reranked
    # =================================================
    def process_serving_batch(batch_df, batch_id):
        start_time = datetime.now()

        if batch_df.isEmpty():
            logger.debug(f"[Serving Batch {batch_id}] empty")
            return

        batch_df = batch_df.persist()

        try:
            input_rows = batch_df.count()

            logger.info("=" * 60)
            logger.info(f"[Serving Batch {batch_id}] input_rows={input_rows}")

            update_redis_user_activity(batch_df, redis_client)

            users_to_rerank = (
                batch_df
                .select("user_id")
                .dropDuplicates(["user_id"])
            )

            base = (
                users_to_rerank
                .join(user_mapping_b, "user_id", "inner")
                .join(user_factors_b, "userIdx", "inner")
            )

            scored = (
                base
                .crossJoin(candidate_items_b)
                .withColumn("score", F.expr(DOT_EXPR))
                .join(item_mapping_b, "itemIdx", "left")
            )

            w = Window.partitionBy("user_id").orderBy(F.col("score").desc())

            topk = (
                scored
                .withColumn("rn", F.row_number().over(w))
                .filter(F.col("rn") <= FINAL_TOPK)
                .groupBy("user_id")
                .agg(
                    F.collect_list(F.col("product_id")).alias("candidates")
                )
            )

            result_rows = topk.collect()

            messages = []

            for row in result_rows:
                user_id = str(row["user_id"])
                candidates = [str(x) for x in row["candidates"] if x]

                if not candidates:
                    continue

                positive_reviews = redis_client.lrange(
                    f"user:{user_id}:positive_reviews",
                    0,
                    9
                )

                purchases = redis_client.lrange(
                    f"user:{user_id}:purchases",
                    0,
                    9
                )

                cart = redis_client.lrange(
                    f"user:{user_id}:cart",
                    0,
                    9
                )

                views = redis_client.lrange(
                    f"user:{user_id}:views",
                    0,
                    9
                )

                recent_items = (
                    list(positive_reviews)
                    + list(purchases)
                    + list(cart)
                    + list(views)
                )

                boosted = recent_items + [
                    item for item in candidates
                    if item not in recent_items
                ]

                final = []
                seen = set()

                for product_id in boosted:
                    if product_id not in seen:
                        final.append(product_id)
                        seen.add(product_id)

                    if len(final) >= FINAL_TOPK:
                        break

                if not final:
                    continue

                redis_key = f"recommend:realtime:{user_id}"

                redis_client.delete(redis_key)
                redis_client.rpush(redis_key, *final)
                redis_client.expire(redis_key, 300)

                messages.append({
                    "user_id": user_id,
                    "recommendations": final
                })

            emitted_users = len(messages)

            latency = (datetime.now() - start_time).total_seconds()
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

            logger.info(f"[Serving Batch {batch_id}] latency={latency:.3f} seconds")
            logger.info(f"[Serving Batch {batch_id}] throughput={throughput:.2f} events/sec")
            logger.info(f"[Serving Batch {batch_id}] emitted_users={emitted_users}")
            logger.info("=" * 60)

        except Exception:
            logger.exception(f"[Serving Batch {batch_id}] failed")
            raise

        finally:
            batch_df.unpersist()

    # =================================================
    # LAKEHOUSE BATCH
    # Slower path:
    # Kafka purchase/review -> bronze parquet
    # =================================================
    def process_lakehouse_batch(batch_df, batch_id):
        start_time = datetime.now()

        if batch_df.isEmpty():
            logger.debug(f"[Lakehouse Batch {batch_id}] empty")
            return

        batch_df = batch_df.persist()

        try:
            input_rows = batch_df.count()

            logger.info("=" * 60)
            logger.info(f"[Lakehouse Batch {batch_id}] input_rows={input_rows}")

            persist_purchase_review_to_bronze(batch_df)

            latency = (datetime.now() - start_time).total_seconds()
            throughput = input_rows / latency if latency > 0 else 0.0

            logger.info(f"[Lakehouse Batch {batch_id}] latency={latency:.3f} seconds")
            logger.info(f"[Lakehouse Batch {batch_id}] throughput={throughput:.2f} events/sec")
            logger.info("=" * 60)

        except Exception:
            logger.exception(f"[Lakehouse Batch {batch_id}] failed")
            raise

        finally:
            batch_df.unpersist()

    # =================================================
    # STREAM 1: REAL-TIME SERVING
    # =================================================
    serving_events = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", SUBSCRIBE_TOPICS)
        .option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "latest"))
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", SERVING_MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    serving_parsed = parse_events(serving_events)

    serving_query = (
        serving_parsed.writeStream
        .queryName("streaming_flow_serving")
        .foreachBatch(process_serving_batch)
        .outputMode("append")
        .trigger(processingTime=SERVING_TRIGGER)
        .option("checkpointLocation", SERVING_CHECKPOINT_PATH)
        .start()
    )

    logger.info("Serving streaming query started")

    # =================================================
    # STREAM 2: LAKEHOUSE INGESTION
    # =================================================
    lakehouse_events = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", "events.purchase,events.review")
        .option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "latest"))
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", LAKEHOUSE_MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    lakehouse_parsed = parse_events(lakehouse_events)

    lakehouse_query = (
        lakehouse_parsed.writeStream
        .queryName("streaming_flow_lakehouse")
        .foreachBatch(process_lakehouse_batch)
        .outputMode("append")
        .trigger(processingTime=LAKEHOUSE_TRIGGER)
        .option("checkpointLocation", LAKEHOUSE_CHECKPOINT_PATH)
        .start()
    )

    logger.info("Lakehouse streaming query started")

    spark.streams.awaitAnyTermination()


# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    spark = create_spark()

    try:
        streaming_job(spark)

    finally:
        spark.stop()