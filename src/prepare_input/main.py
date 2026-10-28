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


def read_csv(spark: SparkSession, raw_path: str, months: list[str]) -> DataFrame:
    """Đọc CSV của các tháng cần thiết (tự động nhận diện 2019-Oct.csv hoặc 2019-10.csv)."""
    import os
    MONTH_ALT = {
        "2019-10": "2019-Oct", "2019-Oct": "2019-10",
        "2019-11": "2019-Nov", "2019-Nov": "2019-11",
        "2019-12": "2019-Dec", "2019-Dec": "2019-12",
    }
    files = []
    for m in months:
        p1 = f"{raw_path}/{m}.csv"
        p2 = f"{raw_path}/{MONTH_ALT.get(m, m)}.csv"
        if os.path.exists(p1):
            files.append(p1)
        elif os.path.exists(p2):
            files.append(p2)
        else:
            files.append(p1)  # Giữ path mặc định để Spark ném thông báo nếu chưa có file

    log.info(f"Đọc {len(files)} file(s): {files}")
    df = spark.read.schema(RAW_SCHEMA).option("header", "true").csv(files)

    # Thêm metadata
    df = df.withColumn("source_file", F.input_file_name())
    df = df.withColumn("ingest_ts", F.current_timestamp())

    # Trích source_month từ tên file (hỗ trợ cả 2019-Oct.csv và 2019-10.csv)
    # Cần chuyển về YYYY-MM (vd: "2019-10") để partition nhất quán
    year_part = F.regexp_extract(F.col("source_file"), r"(\d{4})-([A-Za-z0-9]+)", 1)
    month_token = F.regexp_extract(F.col("source_file"), r"(\d{4})-([A-Za-z0-9]+)", 2)
    month_upper = F.upper(month_token)

    # Map tên tháng viết tắt hoặc số → 2 chữ số
    month_num = (
        F.when(month_upper.isin("JAN", "01", "1"), "01")
         .when(month_upper.isin("FEB", "02", "2"), "02")
         .when(month_upper.isin("MAR", "03", "3"), "03")
         .when(month_upper.isin("APR", "04", "4"), "04")
         .when(month_upper.isin("MAY", "05", "5"), "05")
         .when(month_upper.isin("JUN", "06", "6"), "06")
         .when(month_upper.isin("JUL", "07", "7"), "07")
         .when(month_upper.isin("AUG", "08", "8"), "08")
         .when(month_upper.isin("SEP", "09", "9"), "09")
         .when(month_upper.isin("OCT", "10"), "10")
         .when(month_upper.isin("NOV", "11"), "11")
         .when(month_upper.isin("DEC", "12"), "12")
         .otherwise(month_token)
    )

    df = df.withColumn(
        "source_month",
        F.concat_ws("-", year_part, month_num),
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
