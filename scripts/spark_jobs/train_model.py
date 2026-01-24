# train_als_mlflow.py
import os
import sys
import json
import tempfile
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer

import mlflow
import mlflow.spark
from dotenv import load_dotenv

# ================= SETUP =================
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

load_dotenv(BASE_DIR / ".env")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow_server:5000")
MLFLOW_EXPERIMENT = os.getenv("MLFLOW_EXPERIMENT", "als_recommend")
MLFLOW_REGISTRY = os.getenv("MLFLOW_REGISTRY", "als-model")

USER_FACTORS_PATH = "s3a://gold-layer/als/user_factors"
ITEM_FACTORS_PATH = "s3a://gold-layer/als/item_factors"
USER_MAPPING_PATH = "s3a://gold-layer/als/user_mapping"
ITEM_MAPPING_PATH = "s3a://gold-layer/als/item_mapping"

# ================= SPARK =================
def create_spark():
    return (
        SparkSession.builder
        .appName("ALS-Training")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        # ===== Iceberg + Nessie =====
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
        )
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.silver.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.silver.uri", "http://nessie:19120/api/v2")
        .config("spark.sql.catalog.silver.ref", "main")
        .config("spark.sql.catalog.silver.authentication.type", "NONE")
        .config("spark.sql.catalog.silver.warehouse", "s3a://silver-layer")
        .getOrCreate()
    )

# ================= DATA =================
def load_and_prepare(spark):
    purchases = spark.table("silver.fact_purchase_event")
    reviews = spark.table("silver.fact_reviews")

    purchases = purchases.select(
        col("user_id").alias("user"),
        col("product_id").alias("item"),
        lit(3.0).alias("rating")
    )

    reviews = reviews.select(
        col("user_id").alias("user"),
        col("product_id").alias("item"),
        (col("rating") / 5.0).alias("rating")
    )

    interactions = purchases.unionByName(reviews)

    user_indexer = StringIndexer(
        inputCol="user", outputCol="userIdx", handleInvalid="skip"
    ).fit(interactions)

    item_indexer = StringIndexer(
        inputCol="item", outputCol="itemIdx", handleInvalid="skip"
    ).fit(interactions)

    df = user_indexer.transform(interactions)
    df = item_indexer.transform(df)

    # ===== MAPPINGS (SERVING ARTIFACTS) =====
    user_mapping = (
        df.select("user", "userIdx")
        .dropDuplicates()
        .withColumn("userIdx", col("userIdx").cast("int"))
        .withColumnRenamed("user", "user_id")
    )

    item_mapping = (
        df.select("item", "itemIdx")
        .dropDuplicates()
        .withColumn("itemIdx", col("itemIdx").cast("int"))
        .withColumnRenamed("item", "product_id")
    )

    train_df = df.select(
        col("userIdx").cast("int").alias("user"),
        col("itemIdx").cast("int").alias("item"),
        col("rating")
    )

    return train_df, user_indexer, item_indexer, user_mapping, item_mapping

# ================= TRAIN =================
def train_and_log(train_df, user_indexer, item_indexer, user_mapping, item_mapping):
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    with mlflow.start_run(run_name="als_training"):
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

        # ===== MLflow =====
        mlflow.spark.log_model(model, "als_model")
        mlflow.register_model(
            f"runs:/{mlflow.active_run().info.run_id}/als_model",
            MLFLOW_REGISTRY
        )

        # ===== SAVE GOLD ARTIFACTS =====
        model.userFactors.write.mode("overwrite").parquet(USER_FACTORS_PATH)
        model.itemFactors.write.mode("overwrite").parquet(ITEM_FACTORS_PATH)
        user_mapping.write.mode("overwrite").parquet(USER_MAPPING_PATH)
        item_mapping.write.mode("overwrite").parquet(ITEM_MAPPING_PATH)

        # ===== DEBUG LABELS =====
        with tempfile.TemporaryDirectory() as tmp:
            json.dump(user_indexer.labels, open(f"{tmp}/user_labels.json", "w"))
            json.dump(item_indexer.labels, open(f"{tmp}/item_labels.json", "w"))
            mlflow.log_artifact(f"{tmp}/user_labels.json", "indexer")
            mlflow.log_artifact(f"{tmp}/item_labels.json", "indexer")

        logger.info("ALS training completed successfully")

# ================= MAIN =================
if __name__ == "__main__":
    spark = create_spark()
    train_df, u_idx, i_idx, user_mapping, item_mapping = load_and_prepare(spark)
    train_and_log(train_df, u_idx, i_idx, user_mapping, item_mapping)
    spark.stop()
