"""
src/processing/silver_to_gold.py
Owner: C (Processing 2)
Reviewer: B

Silver → Gold:
  - fact_sessions   (sessionization)
  - dim_product
  - dim_user
  - agg_daily
  - agg_hourly
  - agg_funnel

Chạy:
    spark-submit src/processing/silver_to_gold.py --tier M
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import get_paths, load_config
from src.common.logger import get_logger
from src.common.spark_session import get_spark

log = get_logger(__name__)


# =============================================================================
# Sessionization
# =============================================================================

def build_fact_sessions(df: DataFrame) -> DataFrame:
    """
    Tạo fact_sessions từ silver_events.
    Dùng user_session gốc làm session key chính.
    Mỗi phiên = (user_id, user_session).
    """
    log.info("Xây dựng fact_sessions...")

    sessions = df.groupBy("user_id", "user_session").agg(
        F.min("event_ts").alias("session_start"),
        F.max("event_ts").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("event_type") == "view",     1).otherwise(0)).alias("view_count"),
        F.sum(F.when(F.col("event_type") == "cart",     1).otherwise(0)).alias("cart_count"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
        F.sum(
            F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0)
        ).alias("revenue"),
        F.countDistinct("product_id").alias("distinct_products"),
        F.first("category_l1").alias("category_l1"),   # category chính của phiên
    )

    sessions = sessions.withColumn(
        "session_duration_s",
        F.unix_timestamp("session_end") - F.unix_timestamp("session_start"),
    )
    sessions = sessions.withColumn(
        "converted",
        F.col("purchase_count") > 0,
    )
    return sessions


# =============================================================================
# Dimensions
# =============================================================================

def build_dim_product(df: DataFrame) -> DataFrame:
    log.info("Xây dựng dim_product...")
    return df.groupBy("product_id").agg(
        F.first("category_id").alias("category_id"),
        F.first("category_l1").alias("category_l1"),
        F.first("category_l2").alias("category_l2"),
        F.first("category_l3").alias("category_l3"),
        F.first("brand").alias("brand"),
        F.min("price").alias("price_min"),
        F.max("price").alias("price_max"),
        F.avg("price").alias("price_avg"),
    )


def build_dim_user(df: DataFrame) -> DataFrame:
    log.info("Xây dựng dim_user...")
    return df.groupBy("user_id").agg(
        F.min("event_ts").alias("first_seen"),
        F.max("event_ts").alias("last_seen"),
        F.countDistinct("user_session").alias("session_count"),
    )


# =============================================================================
# Aggregations
# =============================================================================

def build_agg_daily(df: DataFrame) -> DataFrame:
    log.info("Xây dựng agg_daily...")
    return df.groupBy("event_date", "category_l1").agg(
        F.sum(F.when(F.col("event_type") == "view",     1).otherwise(0)).alias("views"),
        F.sum(F.when(F.col("event_type") == "cart",     1).otherwise(0)).alias("carts"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
        F.sum(
            F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0)
        ).alias("revenue"),
        F.countDistinct("user_id").alias("unique_users"),
        F.countDistinct("user_session").alias("sessions"),
    )


def build_agg_hourly(df: DataFrame) -> DataFrame:
    log.info("Xây dựng agg_hourly...")
    return df.groupBy(
        F.dayofweek("event_ts").alias("day_of_week"),
        "event_hour",
    ).agg(
        F.count("*").alias("event_count"),
        F.sum(
            F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0)
        ).alias("revenue"),
    )


def build_agg_funnel(df: DataFrame) -> DataFrame:
    """
    Funnel theo category_l1.
    Định nghĩa: phiên có ≥1 sự kiện ở bước đó.
    """
    log.info("Xây dựng agg_funnel...")
    session_steps = df.groupBy("user_session", "category_l1").agg(
        F.max(F.when(F.col("event_type") == "view",     1).otherwise(0)).alias("has_view"),
        F.max(F.when(F.col("event_type") == "cart",     1).otherwise(0)).alias("has_cart"),
        F.max(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("has_purchase"),
    )
    return session_steps.groupBy("category_l1").agg(
        F.sum("has_view").alias("sessions_with_view"),
        F.sum("has_cart").alias("sessions_with_cart"),
        F.sum("has_purchase").alias("sessions_with_purchase"),
    )


# =============================================================================
# Main
# =============================================================================

def write_gold(df: DataFrame, path: str, name: str) -> None:
    out = f"{path}/{name}"
    log.info(f"Ghi {name} → {out}")
    df.write.mode("overwrite").parquet(out)


def run(tier: str) -> None:
    cfg = load_config()
    paths = get_paths(cfg)

    spark = get_spark(f"gold_{tier}")
    df = spark.read.parquet(paths["silver"])
    df.cache()
    log.info(f"Silver loaded: {df.count():,} dòng")

    gold = paths["gold"]

    write_gold(build_fact_sessions(df),  gold, "fact_sessions")
    write_gold(build_dim_product(df),    gold, "dim_product")
    write_gold(build_dim_user(df),       gold, "dim_user")
    write_gold(build_agg_daily(df),      gold, "agg_daily")
    write_gold(build_agg_hourly(df),     gold, "agg_hourly")
    write_gold(build_agg_funnel(df),     gold, "agg_funnel")

    df.unpersist()
    spark.stop()
    log.info("Gold hoàn thành.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tier", default="S")
    args = parser.parse_args()
    run(args.tier)
