"""
src/analytics/rfm_kmeans.py
Owner: E (Analytics 2)
Reviewer: D

RFM (Recency, Frequency, Monetary) + KMeans phân khúc khách hàng.
Chỉ áp dụng cho user có ít nhất 1 purchase.
"""
from __future__ import annotations

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.logger import get_logger

log = get_logger(__name__)


def build_rfm(fact_sessions: DataFrame, as_of_date: str) -> DataFrame:
    """
    Tính RFM:
    - Recency  = số ngày kể từ lần purchase gần nhất đến as_of_date
    - Frequency = số phiên có purchase
    - Monetary  = tổng revenue
    Chỉ giữ user có converted = True.
    """
    log.info(f"Xây dựng RFM | as_of={as_of_date}")
    as_of = F.to_date(F.lit(as_of_date))

    buyers = fact_sessions.filter(F.col("converted") == True)  # noqa: E712

    rfm = buyers.groupBy("user_id").agg(
        F.datediff(as_of, F.max("session_start")).alias("recency"),
        F.count("*").alias("frequency"),
        F.sum("revenue").alias("monetary"),
    )
    return rfm


def run_kmeans(rfm: DataFrame, k_range: range, seed: int, max_iter: int) -> DataFrame:
    """
    Chạy KMeans với nhiều k, chọn k tốt nhất theo silhouette.
    Ghi elbow + silhouette scores vào results/kmeans_scores.csv.
    Trả về DataFrame với cột cluster.
    """
    import csv
    from pathlib import Path

    features = ["recency", "frequency", "monetary"]

    # Log transform để giảm skew (rfm thường phân phối lệch phải)
    for feat in features:
        rfm = rfm.withColumn(f"log_{feat}", F.log1p(F.col(feat)))
    log_features = [f"log_{f}" for f in features]

    assembler = VectorAssembler(inputCols=log_features, outputCol="features_raw")
    rfm_vec = assembler.transform(rfm)

    scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                            withStd=True, withMean=True)
    scaler_model = scaler.fit(rfm_vec)
    rfm_scaled = scaler_model.transform(rfm_vec).cache()

    best_k, best_score, best_model = None, -1.0, None
    evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette")

    score_rows = []
    for k in k_range:
        km = KMeans(k=k, seed=seed, maxIter=max_iter, featuresCol="features")
        model = km.fit(rfm_scaled)
        predictions = model.transform(rfm_scaled)
        silhouette = evaluator.evaluate(predictions)
        inertia = model.summary.trainingCost
        log.info(f"KMeans k={k} | silhouette={silhouette:.4f} | inertia={inertia:.2f}")
        score_rows.append({"k": k, "silhouette": round(silhouette, 6), "inertia": round(inertia, 4)})
        if silhouette > best_score:
            best_score = silhouette
            best_k = k
            best_model = model

    # Ghi scores để vẽ elbow + silhouette chart
    results_dir = Path("/app/results")
    results_dir.mkdir(parents=True, exist_ok=True)
    with open(results_dir / "kmeans_scores.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["k", "silhouette", "inertia"])
        writer.writeheader()
        writer.writerows(score_rows)

    log.info(f"Best k={best_k} | silhouette={best_score:.4f}")
    rfm_scaled.unpersist()

    final = best_model.transform(scaler_model.transform(assembler.transform(rfm)))
    return final.select(
        "user_id", "recency", "frequency", "monetary",
        F.col("prediction").alias("cluster"),
    )


def cluster_profiles(segmented: DataFrame) -> DataFrame:
    """Tóm tắt trung vị RFM của từng cụm."""
    return segmented.groupBy("cluster").agg(
        F.count("*").alias("user_count"),
        F.percentile_approx("recency",   0.5).alias("median_recency"),
        F.percentile_approx("frequency", 0.5).alias("median_frequency"),
        F.percentile_approx("monetary",  0.5).alias("median_monetary"),
    ).orderBy("cluster")
