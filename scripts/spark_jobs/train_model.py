import os
import sys
import json
import tempfile
import logging
from pathlib import Path

from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    collect_set,
    size,
    array_intersect,
    avg,
    expr,
    countDistinct
)
from pyspark.storagelevel import StorageLevel

from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import RegressionEvaluator

import mlflow
import mlflow.spark


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

logger = logging.getLogger("als-training")


# =====================================================
# MLFLOW CONFIG
# =====================================================
MLFLOW_TRACKING_URI = os.getenv(
    "MLFLOW_TRACKING_URI",
    "http://mlflow_server:5000"
)

MLFLOW_EXPERIMENT = os.getenv(
    "MLFLOW_EXPERIMENT",
    "als_recommend"
)

MLFLOW_REGISTRY = os.getenv(
    "MLFLOW_REGISTRY",
    "als-model"
)


# =====================================================
# MODEL ARTIFACT PATHS
# =====================================================
USER_FACTORS_PATH = os.getenv(
    "USER_FACTORS_PATH",
    "s3a://gold-layer/ml/als/user_factors"
)

ITEM_FACTORS_PATH = os.getenv(
    "ITEM_FACTORS_PATH",
    "s3a://gold-layer/ml/als/item_factors"
)

USER_MAPPING_PATH = os.getenv(
    "USER_MAPPING_PATH",
    "s3a://gold-layer/ml/als/user_mapping"
)

ITEM_MAPPING_PATH = os.getenv(
    "ITEM_MAPPING_PATH",
    "s3a://gold-layer/ml/als/item_mapping"
)


# =====================================================
# ALS PARAMETERS
# =====================================================
ALS_RANK = int(os.getenv("ALS_RANK", "16"))
ALS_MAX_ITER = int(os.getenv("ALS_MAX_ITER", "5"))
ALS_REG_PARAM = float(os.getenv("ALS_REG_PARAM", "0.1"))
ALS_ALPHA = float(os.getenv("ALS_ALPHA", "10"))

TOP_K = int(os.getenv("ALS_TOP_K", "10"))

TRAIN_RATIO = float(os.getenv("TRAIN_RATIO", "0.8"))
TEST_RATIO = float(os.getenv("TEST_RATIO", "0.2"))

SHUFFLE_PARTITIONS = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8")

# Nếu máy yếu, có thể set false để bỏ bước log full Spark model.
# Metrics, mappings, user/item factors vẫn được lưu đầy đủ.
LOG_SPARK_MODEL = os.getenv("MLFLOW_LOG_SPARK_MODEL", "true").lower() == "true"
REGISTER_MODEL = os.getenv("MLFLOW_REGISTER_MODEL", "true").lower() == "true"


# =====================================================
# SPARK SESSION
# =====================================================
def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ALS-Training")

        # ---------- Basic Spark tuning ----------
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config("spark.default.parallelism", SHUFFLE_PARTITIONS)
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # ---------- MinIO ----------
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        # ---------- Iceberg + Nessie ----------
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
        )

        # ---------- GOLD catalog ----------
        .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.gold.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog"
        )
        .config("spark.sql.catalog.gold.uri", "http://nessie:19120/api/v2")
        .config("spark.sql.catalog.gold.ref", "main")
        .config("spark.sql.catalog.gold.authentication.type", "NONE")
        .config("spark.sql.catalog.gold.warehouse", "s3a://gold-layer")

        .getOrCreate()
    )


# =====================================================
# LOAD + PREPARE DATA
# =====================================================
def load_and_prepare(spark: SparkSession):
    logger.info("Loading gold.fact_user_interactions")

    # Quan trọng:
    # Chỉ đổi user_id/product_id/interaction_score -> user/item/rating một lần.
    # Không select lại bằng user_id/product_id sau đoạn này.
    interactions = (
        spark.table("gold.fact_user_interactions")
        .filter(col("user_id").isNotNull())
        .filter(col("product_id").isNotNull())
        .filter(col("interaction_score").isNotNull())
        .select(
            col("user_id").cast("string").alias("user"),
            col("product_id").cast("string").alias("item"),
            col("interaction_score").cast("double").alias("rating")
        )
        .filter(col("user").isNotNull())
        .filter(col("item").isNotNull())
        .filter(col("rating").isNotNull())
        .repartition(int(SHUFFLE_PARTITIONS), "user")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    row_count = interactions.count()
    if row_count == 0:
        raise ValueError("gold.fact_user_interactions has no valid rows for ALS training.")

    logger.info(f"Interactions rows: {row_count}")

    stats = interactions.agg(
        countDistinct("user").alias("users"),
        countDistinct("item").alias("products")
    ).first()

    logger.info(f"Users: {stats['users']}")
    logger.info(f"Products: {stats['products']}")

    # =================================================
    # USER INDEXER
    # =================================================
    logger.info("Fitting user StringIndexer")

    user_indexer = StringIndexer(
        inputCol="user",
        outputCol="userIdx",
        handleInvalid="skip"
    ).fit(interactions)

    # =================================================
    # ITEM INDEXER
    # =================================================
    logger.info("Fitting item StringIndexer")

    item_indexer = StringIndexer(
        inputCol="item",
        outputCol="itemIdx",
        handleInvalid="skip"
    ).fit(interactions)

    logger.info("Transforming interactions with indexers")

    indexed_df = user_indexer.transform(interactions)
    indexed_df = item_indexer.transform(indexed_df)

    indexed_df = indexed_df.persist(StorageLevel.MEMORY_AND_DISK)

    # Force cache
    indexed_count = indexed_df.count()
    logger.info(f"Indexed rows: {indexed_count}")

    # =================================================
    # USER MAPPING
    # streaming_flow.py cần columns: user_id, userIdx
    # =================================================
    user_mapping = (
        indexed_df
        .select("user", "userIdx")
        .dropDuplicates()
        .withColumn("userIdx", col("userIdx").cast("int"))
        .withColumnRenamed("user", "user_id")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # =================================================
    # ITEM MAPPING
    # streaming_flow.py cần columns: product_id, itemIdx
    # =================================================
    item_mapping = (
        indexed_df
        .select("item", "itemIdx")
        .dropDuplicates()
        .withColumn("itemIdx", col("itemIdx").cast("int"))
        .withColumnRenamed("item", "product_id")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    logger.info(f"User mapping rows: {user_mapping.count()}")
    logger.info(f"Item mapping rows: {item_mapping.count()}")

    # =================================================
    # ALS DATASET
    # ALS cần user/item là numeric int
    # =================================================
    dataset = (
        indexed_df
        .select(
            col("userIdx").cast("int").alias("user"),
            col("itemIdx").cast("int").alias("item"),
            col("rating").cast("double").alias("rating")
        )
        .filter(col("user").isNotNull())
        .filter(col("item").isNotNull())
        .filter(col("rating").isNotNull())
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    dataset_count = dataset.count()
    logger.info(f"ALS dataset rows: {dataset_count}")

    if dataset_count == 0:
        raise ValueError("ALS dataset is empty after indexing.")

    return (
        dataset,
        user_indexer,
        item_indexer,
        user_mapping,
        item_mapping
    )


# =====================================================
# TRAIN + LOG MODEL
# =====================================================
def train_and_log(
    train_df,
    test_df,
    user_indexer,
    item_indexer,
    user_mapping,
    item_mapping
):
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    with mlflow.start_run(run_name="als_training"):

        logger.info("Starting ALS training")

        mlflow.log_param("rank", ALS_RANK)
        mlflow.log_param("maxIter", ALS_MAX_ITER)
        mlflow.log_param("regParam", ALS_REG_PARAM)
        mlflow.log_param("alpha", ALS_ALPHA)
        mlflow.log_param("implicitPrefs", True)
        mlflow.log_param("top_k", TOP_K)
        mlflow.log_param("shuffle_partitions", SHUFFLE_PARTITIONS)

        als = ALS(
            userCol="user",
            itemCol="item",
            ratingCol="rating",
            implicitPrefs=True,
            rank=ALS_RANK,
            maxIter=ALS_MAX_ITER,
            regParam=ALS_REG_PARAM,
            alpha=ALS_ALPHA,
            nonnegative=True,
            coldStartStrategy="drop"
        )

        model = als.fit(train_df)

        logger.info("ALS model training completed")

        # =================================================
        # RMSE
        # =================================================
        logger.info("Evaluating RMSE")

        predictions = model.transform(test_df)

        evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol="rating",
            predictionCol="prediction"
        )

        rmse = evaluator.evaluate(predictions)

        logger.info(f"RMSE: {rmse}")
        mlflow.log_metric("rmse", float(rmse))

        # =================================================
        # PRECISION@K / RECALL@K
        # =================================================
        logger.info(f"Evaluating Precision@{TOP_K} and Recall@{TOP_K}")

        user_recs = model.recommendForAllUsers(TOP_K)

        pred = user_recs.select(
            col("user"),
            expr("transform(recommendations, x -> x.item)").alias("pred_items")
        )

        actual = (
            test_df
            .groupBy("user")
            .agg(
                collect_set("item").alias("actual_items")
            )
        )

        joined = pred.join(actual, "user", "inner")

        if joined.rdd.isEmpty():
            precision_at_k = 0.0
            recall_at_k = 0.0
        else:
            precision_df = joined.withColumn(
                "precision",
                size(array_intersect("pred_items", "actual_items")) / TOP_K
            )

            recall_df = joined.withColumn(
                "recall",
                size(array_intersect("pred_items", "actual_items")) /
                (size("actual_items") + 1e-9)
            )

            precision_at_k = precision_df.select(avg("precision")).first()[0]
            recall_at_k = recall_df.select(avg("recall")).first()[0]

            if precision_at_k is None:
                precision_at_k = 0.0

            if recall_at_k is None:
                recall_at_k = 0.0

        logger.info(f"Precision@{TOP_K}: {precision_at_k}")
        logger.info(f"Recall@{TOP_K}: {recall_at_k}")

        mlflow.log_metric(f"precision_at_{TOP_K}", float(precision_at_k))
        mlflow.log_metric(f"recall_at_{TOP_K}", float(recall_at_k))

        # =================================================
        # SAVE ALS FACTORS
        # streaming_flow.py đọc các path này
        # =================================================
        logger.info("Saving ALS user factors")

        (
            model.userFactors
            .write
            .mode("overwrite")
            .parquet(USER_FACTORS_PATH)
        )

        logger.info("Saving ALS item factors")

        (
            model.itemFactors
            .write
            .mode("overwrite")
            .parquet(ITEM_FACTORS_PATH)
        )

        # =================================================
        # SAVE USER / ITEM MAPPING
        # =================================================
        logger.info("Saving user mapping")

        (
            user_mapping
            .write
            .mode("overwrite")
            .parquet(USER_MAPPING_PATH)
        )

        logger.info("Saving item mapping")

        (
            item_mapping
            .write
            .mode("overwrite")
            .parquet(ITEM_MAPPING_PATH)
        )

        # =================================================
        # LOG INDEXER LABELS
        # =================================================
        logger.info("Logging StringIndexer labels")

        with tempfile.TemporaryDirectory() as tmp:
            user_labels_path = f"{tmp}/user_labels.json"
            item_labels_path = f"{tmp}/item_labels.json"

            with open(user_labels_path, "w", encoding="utf-8") as f:
                json.dump(list(user_indexer.labels), f)

            with open(item_labels_path, "w", encoding="utf-8") as f:
                json.dump(list(item_indexer.labels), f)

            mlflow.log_artifact(user_labels_path, "indexer")
            mlflow.log_artifact(item_labels_path, "indexer")

        # =================================================
        # LOG / REGISTER SPARK MODEL
        # Có thể nặng trên Docker local, nên cho phép tắt bằng env.
        # =================================================
        if LOG_SPARK_MODEL:
            logger.info("Logging Spark ALS model to MLflow")

            mlflow.spark.log_model(
                model,
                "als_model"
            )

            if REGISTER_MODEL:
                logger.info("Registering Spark ALS model")

                mlflow.register_model(
                    f"runs:/{mlflow.active_run().info.run_id}/als_model",
                    MLFLOW_REGISTRY
                )
        else:
            logger.info("Skipping mlflow.spark.log_model because MLFLOW_LOG_SPARK_MODEL=false")

        logger.info("ALS training completed successfully")


# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":

    spark = create_spark()

    try:
        (
            dataset,
            user_indexer,
            item_indexer,
            user_mapping,
            item_mapping
        ) = load_and_prepare(spark)

        logger.info("Train / Test split")

        train_df, test_df = dataset.randomSplit(
            [TRAIN_RATIO, TEST_RATIO],
            seed=42
        )

        train_df = train_df.persist(StorageLevel.MEMORY_AND_DISK)
        test_df = test_df.persist(StorageLevel.MEMORY_AND_DISK)

        logger.info(f"Train rows: {train_df.count()}")
        logger.info(f"Test rows: {test_df.count()}")

        train_and_log(
            train_df,
            test_df,
            user_indexer,
            item_indexer,
            user_mapping,
            item_mapping
        )

    finally:
        spark.stop()
        logger.info("Spark session stopped")