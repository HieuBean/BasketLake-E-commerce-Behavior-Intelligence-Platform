"""
tests/conftest.py
SparkSession dùng chung cho tất cả test (local mode, 1 core).
"""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
