"""
src/prepare_input/main.py
Owner: B (Processing 1)
Reviewer: A

Chuyển CSV thô từ Kaggle thành Parquet bronze theo data contract.
- Ép kiểu schema (chưa làm sạch — đó là việc của silver)
- Thêm cột source_file, ingest_ts
- Partition theo source_month
- Hỗ trợ tier S (lấy mẫu), M, L, XL

Chạy:
    spark-submit src/prepare_input/main.py --tier S
"""
from __future__ import annotations

import argparse
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType,
)

from src.common.spark_session import get_spark
from src.common.config import load_config, get_paths, get_tier_config
from src.common.logger import get_logger

log = get_logger(__name__)

# Schema gốc của CSV REES46 (xác nhận lại bằng profiling)
RAW_SCHEMA = StructType([
    StructField("event_time",    StringType(),  True),
    StructField("event_type",    StringType(),  True),
    StructField("product_id",    LongType(),    True),
    StructField("category_id",   LongType(),    True),
    StructField("category_code", StringType(),  True),
    StructField("brand",         StringType(),  True),
    StructField("price",         DoubleType(),  True),
    StructField("user_id",       LongType(),    True),
    StructField("user_session",  StringType(),  True),
])


MONTH_ALIASES = {
    "2019-10": ["2019-Oct", "2019-10"],
    "2019-11": ["2019-Nov", "2019-11"],
}


def _resolve_file(raw_path: str, month: str) -> str:
    import os
    aliases = MONTH_ALIASES.get(month, [month])
    for alias in aliases:
        for ext in [".csv", ".csv.gz"]:
            candidate = f"{raw_path}/{alias}{ext}"
            local_cand = candidate.replace("/app/", "")
            if os.path.exists(candidate) or os.path.exists(local_cand):
                return candidate
    return f"{raw_path}/{month}.csv"


def read_csv(spark: SparkSession, raw_path: str, months: list[str]) -> DataFrame:
    """Đọc CSV của các tháng cần thiết (hỗ trợ cả 2019-10.csv và 2019-Oct.csv)."""
    files = [_resolve_file(raw_path, m) for m in months]
    log.info(f"Đọc {len(files)} file(s): {files}")
    df = spark.read.schema(RAW_SCHEMA).option("header", "true").csv(files)

    # Thêm metadata
    df = df.withColumn("source_file", F.input_file_name())
    df = df.withColumn("ingest_ts", F.current_timestamp())

    # Trích source_month từ tên file (YYYY-MM hoặc YYYY-Oct -> YYYY-10, YYYY-Nov -> YYYY-11)
    df = df.withColumn(
        "source_month",
        F.when(F.col("source_file").rlike(r"2019-Oct"), F.lit("2019-10"))
         .when(F.col("source_file").rlike(r"2019-Nov"), F.lit("2019-11"))
         .otherwise(F.regexp_extract(F.col("source_file"), r"(\d{4}-\d{2})", 1)),
    )
    return df


def sample_tier_s(df: DataFrame, fraction: float, seed: int) -> DataFrame:
    log.info(f"Lấy mẫu tier S: fraction={fraction}, seed={seed}")
    return df.sample(fraction=fraction, seed=seed)


def amplify_xl(df: DataFrame, factor: int) -> DataFrame:
    """Nhân bản dữ liệu có biến đổi user_id và product_id (synthetic)."""
    log.info(f"Khuếch đại XL: factor={factor} (synthetic)")
    dfs = [df]
    for i in range(1, factor):
        offset = i * 10_000_000
        clone = (
            df
            .withColumn("user_id",    F.col("user_id")    + offset)
            .withColumn("product_id", F.col("product_id") + offset)
        )
        dfs.append(clone)
    from functools import reduce
    return reduce(DataFrame.union, dfs)


def write_bronze(df: DataFrame, bronze_path: str) -> None:
    log.info(f"Ghi bronze → {bronze_path}")
    (
        df.write
        .mode("overwrite")
        .partitionBy("source_month")
        .parquet(bronze_path)
    )
    log.info("Ghi bronze xong.")


def main(tier: str) -> None:
    cfg = load_config()
    paths = get_paths(cfg)
    tier_cfg = get_tier_config(cfg, tier)

    spark = get_spark(f"prepare_input_{tier}")
    log.info(f"Bắt đầu prepare_input | tier={tier}")

    df = read_csv(spark, paths["raw"], tier_cfg["months"])

    if tier == "S":
        df = sample_tier_s(df, tier_cfg["sample_fraction"], tier_cfg["seed"])
    elif tier == "XL":
        df = amplify_xl(df, tier_cfg["amplify_factor"])

    write_bronze(df, paths["bronze"])

    # Thống kê nhanh
    count = df.count()
    log.info(f"Tổng số dòng bronze [{tier}]: {count:,}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tier", default="S", choices=["S", "M", "L", "XL"])
    args = parser.parse_args()
    main(args.tier)
