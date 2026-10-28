"""
src/common/spark_session.py
Tạo SparkSession với cấu hình từ spark-defaults.conf.
Dùng: from src.common.spark_session import get_spark
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark(app_name: str, extra_conf: dict[str, str] | None = None) -> SparkSession:
    """
    Trả về SparkSession.
    - Đọc spark-defaults.conf từ môi trường Spark (cluster).
    - extra_conf cho phép override tham số cụ thể khi cần (thí nghiệm E4).
    """
    builder = SparkSession.builder.appName(app_name)

    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()

    # Ghi log phiên bản để trace
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark
