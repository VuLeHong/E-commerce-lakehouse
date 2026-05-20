import os
import sys
import json
import tempfile
import logging
from pathlib import Path

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

from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import RegressionEvaluator

import mlflow
import mlflow.spark
from dotenv import load_dotenv

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
# MLFLOW
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
USER_FACTORS_PATH = "s3a://gold-layer/ml/als/user_factors"
ITEM_FACTORS_PATH = "s3a://gold-layer/ml/als/item_factors"

USER_MAPPING_PATH = "s3a://gold-layer/ml/als/user_mapping"
ITEM_MAPPING_PATH = "s3a://gold-layer/ml/als/item_mapping"

# =====================================================
# SPARK
# =====================================================
def create_spark():

    return (
        SparkSession.builder
        .appName("ALS-Training")

        # ---------- MinIO ----------
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # ---------- Iceberg + Nessie ----------
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
        )

        # ---------- GOLD ----------
        .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")

        .config(
            "spark.sql.catalog.gold.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog"
        )

        .config(
            "spark.sql.catalog.gold.uri",
            "http://nessie:19120/api/v2"
        )

        .config("spark.sql.catalog.gold.ref", "main")

        .config(
            "spark.sql.catalog.gold.authentication.type",
            "NONE"
        )

        .config(
            "spark.sql.catalog.gold.warehouse",
            "s3a://gold-layer"
        )

        .getOrCreate()
    )

# =====================================================
# LOAD DATA
# =====================================================
def load_and_prepare(spark):

    logger.info("Loading gold.fact_user_interactions")

    interactions = spark.table(
        "gold.fact_user_interactions"
    )

    interactions = (
        interactions
        .filter(col("user_id").isNotNull())
        .filter(col("product_id").isNotNull())
        .filter(col("interaction_score").isNotNull())
    )

    logger.info(
        f"Interactions rows: {interactions.count()}"
    )

    logger.info(
        f"Users: {interactions.select(countDistinct('user_id')).first()[0]}"
    )

    logger.info(
        f"Products: {interactions.select(countDistinct('product_id')).first()[0]}"
    )

    # =================================================
    # ALS INPUT
    # =================================================
    interactions = interactions.select(
        col("user_id").cast("string").alias("user"),
        col("product_id").cast("string").alias("item"),
        col("interaction_score").cast("double").alias("rating")
    )

    # =================================================
    # USER INDEXER
    # =================================================
    user_indexer = StringIndexer(
        inputCol="user",
        outputCol="userIdx",
        handleInvalid="skip"
    ).fit(interactions)

    # =================================================
    # ITEM INDEXER
    # =================================================
    item_indexer = StringIndexer(
        inputCol="item",
        outputCol="itemIdx",
        handleInvalid="skip"
    ).fit(interactions)

    df = user_indexer.transform(interactions)
    df = item_indexer.transform(df)

    # =================================================
    # USER MAPPING
    # =================================================
    user_mapping = (
        df
        .select("user", "userIdx")
        .dropDuplicates()

        .withColumn(
            "userIdx",
            col("userIdx").cast("int")
        )

        .withColumnRenamed(
            "user",
            "user_id"
        )
    )

    # =================================================
    # ITEM MAPPING
    # =================================================
    item_mapping = (
        df
        .select("item", "itemIdx")
        .dropDuplicates()

        .withColumn(
            "itemIdx",
            col("itemIdx").cast("int")
        )

        .withColumnRenamed(
            "item",
            "product_id"
        )
    )

    # =================================================
    # TRAIN DATASET
    # =================================================
    dataset = (
        df.select(
            col("userIdx").cast("int").alias("user"),
            col("itemIdx").cast("int").alias("item"),
            col("rating")
        )
    )

    return (
        dataset,
        user_indexer,
        item_indexer,
        user_mapping,
        item_mapping
    )

# =====================================================
# TRAIN MODEL
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

        logger.info("Training ALS model")

        als = ALS(
            userCol="user",
            itemCol="item",
            ratingCol="rating",

            implicitPrefs=True,

            rank=32,
            maxIter=10,
            regParam=0.05,
            alpha=20,

            nonnegative=True,
            coldStartStrategy="drop"
        )

        model = als.fit(train_df)

        # =================================================
        # RMSE
        # =================================================
        predictions = model.transform(test_df)

        evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol="rating",
            predictionCol="prediction"
        )

        rmse = evaluator.evaluate(predictions)

        logger.info(f"RMSE: {rmse}")

        mlflow.log_metric("rmse", rmse)

        # =================================================
        # PRECISION@K / RECALL@K
        # =================================================
        K = 10

        user_recs = model.recommendForAllUsers(K)

        pred = user_recs.select(
            col("user"),
            expr(
                "transform(recommendations, x -> x.item)"
            ).alias("pred_items")
        )

        actual = (
            test_df
            .groupBy("user")
            .agg(
                collect_set("item").alias("actual_items")
            )
        )

        joined = pred.join(actual, "user", "inner")

        precision_df = joined.withColumn(
            "precision",
            size(
                array_intersect(
                    "pred_items",
                    "actual_items"
                )
            ) / K
        )

        recall_df = joined.withColumn(
            "recall",
            size(
                array_intersect(
                    "pred_items",
                    "actual_items"
                )
            ) /
            (size("actual_items") + 1e-9)
        )

        precision_at_k = (
            precision_df
            .select(avg("precision"))
            .first()[0]
        )

        recall_at_k = (
            recall_df
            .select(avg("recall"))
            .first()[0]
        )

        logger.info(f"Precision@{K}: {precision_at_k}")
        logger.info(f"Recall@{K}: {recall_at_k}")

        mlflow.log_metric(
            f"precision_at_{K}",
            precision_at_k
        )

        mlflow.log_metric(
            f"recall_at_{K}",
            recall_at_k
        )

        # =================================================
        # LOG MODEL
        # =================================================
        mlflow.spark.log_model(
            model,
            "als_model"
        )

        mlflow.register_model(
            f"runs:/{mlflow.active_run().info.run_id}/als_model",
            MLFLOW_REGISTRY
        )

        # =================================================
        # SAVE FACTORS
        # =================================================
        logger.info("Saving ALS artifacts")

        model.userFactors.write \
            .mode("overwrite") \
            .parquet(USER_FACTORS_PATH)

        model.itemFactors.write \
            .mode("overwrite") \
            .parquet(ITEM_FACTORS_PATH)

        user_mapping.write \
            .mode("overwrite") \
            .parquet(USER_MAPPING_PATH)

        item_mapping.write \
            .mode("overwrite") \
            .parquet(ITEM_MAPPING_PATH)

        # =================================================
        # SAVE LABELS
        # =================================================
        with tempfile.TemporaryDirectory() as tmp:

            json.dump(
                user_indexer.labels,
                open(f"{tmp}/user_labels.json", "w")
            )

            json.dump(
                item_indexer.labels,
                open(f"{tmp}/item_labels.json", "w")
            )

            mlflow.log_artifact(
                f"{tmp}/user_labels.json",
                "indexer"
            )

            mlflow.log_artifact(
                f"{tmp}/item_labels.json",
                "indexer"
            )

        logger.info("ALS training completed successfully")

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":

    spark = create_spark()

    (
        dataset,
        user_indexer,
        item_indexer,
        user_mapping,
        item_mapping
    ) = load_and_prepare(spark)

    logger.info("Train / Test split")

    train_df, test_df = dataset.randomSplit(
        [0.8, 0.2],
        seed=42
    )

    train_and_log(
        train_df,
        test_df,
        user_indexer,
        item_indexer,
        user_mapping,
        item_mapping
    )

    spark.stop()