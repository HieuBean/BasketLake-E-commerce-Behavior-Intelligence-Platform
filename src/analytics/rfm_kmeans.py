"""
src/analytics/rfm_kmeans.py
Owner: E (Analytics 2)
Reviewer: D

RFM (Recency, Frequency, Monetary) + KMeans phân khúc khách hàng.
Chỉ áp dụng cho user có ít nhất 1 purchase.
"""
from __future__ import annotations

import os
from pathlib import Path

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.analytics.kmeans_selection import (
    DEFAULT_STABILITY_TOLERANCE,
    select_k,
    stability_spread,
)
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
        F.datediff(as_of, F.max("last_purchase_ts")).alias("recency"),
        F.count("*").alias("frequency"),
        F.sum("revenue").alias("monetary"),
    )
    return rfm


def _cluster_sizes_by_rank(model, rfm_scaled: DataFrame) -> list[int]:
    """
    Kích thước các cụm của một model, sắp theo THỨ HẠNG median_monetary.

    Cluster ID bị hoán vị giữa các seed (KMeans không đảm bảo thứ tự nhãn),
    nên không so sánh được theo index. Thứ hạng theo Monetary là bất biến
    duy nhất dùng được để ghép cụm giữa các lần fit.
    """
    rows = (
        model.transform(rfm_scaled)
             .select(F.col("prediction").alias("cluster"), "monetary")
             .groupBy("cluster")
             .agg(F.count("*").alias("n"),
                  F.percentile_approx("monetary", 0.5).alias("m"))
             .orderBy(F.desc("m"))
             .collect()
    )
    return [int(r["n"]) for r in rows]


def _stability_spread(sizes_by_seed: list[list[int]]) -> float:
    """Alias tương thích ngược — logic thật nằm ở kmeans_selection.stability_spread."""
    return stability_spread(sizes_by_seed)


def run_kmeans(
    rfm: DataFrame,
    k_range: range,
    seed: int,
    max_iter: int,
    results_dir: Path | str | None = None,
    stability_tolerance: float = DEFAULT_STABILITY_TOLERANCE,
) -> DataFrame:
    """
    Chạy KMeans với nhiều k, chọn k tốt nhất theo silhouette **có cổng ổn định**.

    Vì sao cần cổng ổn định?
    -----------------------
    Chọn thuần `argmax(silhouette)` có thể chọn phải nghiệm "degenerate": một
    cụm rất nhỏ tách hẳn ra xa làm silhouette trung bình tăng vọt, nhưng kích
    thước cụm thay đổi nhiều lần giữa các seed → không tái lập được.

    Đo trên tier S: k=2 có silhouette 0,8359 nhưng cụm chi tiêu cao chứa
    904 / 7.411 / 7.447 người tùy seed (lệch 124,5%). k=4 chỉ có silhouette
    0,5498 nhưng lệch tối đa 1,7%. Cổng này loại k=2 và chọn k=4.

    Chỉ số ổn định tính từ chính các model đã fit (mỗi k fit 3 seed), không
    fit thêm. Nếu không k nào đạt ngưỡng, quay về argmax(silhouette) và cảnh báo.

    Ghi elbow + silhouette + độ ổn định vào results/kmeans_scores.csv.
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

    candidate_seeds = list(dict.fromkeys([seed, 0, 123]))
    log.info(f"Số seed thử cho mỗi k: {len(candidate_seeds)} → {candidate_seeds}")

    evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette")

    score_rows = []
    for k in k_range:
        # KMeans may converge to different local minima. Keep the lowest WCSS.
        candidates = [
            (KMeans(k=k, seed=initial_seed, maxIter=max_iter, featuresCol="features")
             .fit(rfm_scaled), initial_seed)
            for initial_seed in candidate_seeds
        ]
        model, selected_seed = min(candidates, key=lambda pair: pair[0].summary.trainingCost)
        predictions = model.transform(rfm_scaled)
        silhouette = evaluator.evaluate(predictions)
        inertia = model.summary.trainingCost

        # Độ ổn định: dùng lại model đã fit, chỉ thêm transform + groupBy
        sizes_by_seed = [_cluster_sizes_by_rank(m, rfm_scaled) for m, _ in candidates]
        spread = _stability_spread(sizes_by_seed)

        log.info(f"KMeans k={k} | silhouette={silhouette:.4f} | "
                 f"inertia={inertia:.2f} | stability_spread={spread:.2%}")
        score_rows.append({
            "k": k, "silhouette": round(silhouette, 6),
            "inertia": round(inertia, 4), "selected_seed": selected_seed,
            "stability_spread": round(spread, 6),
        })

    # --- Chọn k: silhouette cao nhất trong các k ĐẠT cổng ổn định ---
    best_row, excluded_ks = select_k(score_rows, tolerance=stability_tolerance)
    if excluded_ks:
        log.info(f"Bị loại bởi cổng ổn định (>{stability_tolerance:.0%}): k={excluded_ks}")
    if not any(r["stability_spread"] <= stability_tolerance for r in score_rows):
        log.warning(f"Không k nào đạt cổng ổn định {stability_tolerance:.0%}. "
                    f"Quay về argmax(silhouette) — kết quả có thể không tái lập.")

    best_k = best_row["k"]
    best_score = best_row["silhouette"]
    for r in score_rows:
        r["selected"] = 1 if r["k"] == best_k else 0

    # Fit lại đúng k đã chọn với seed đã chọn để lấy model cuối
    best_model = (
        KMeans(k=int(best_k), seed=int(best_row["selected_seed"]), maxIter=max_iter,
               featuresCol="features")
        .fit(rfm_scaled)
    )

    # Ghi scores để vẽ elbow + silhouette chart và để run_analytics đọc lại
    results_dir = Path(results_dir or os.getenv("RESULTS_DIR", "/app/results" if Path("/app").exists() else "results"))
    results_dir.mkdir(parents=True, exist_ok=True)
    fieldnames = ["k", "silhouette", "inertia", "selected_seed",
                  "stability_spread", "selected"]
    with open(results_dir / "kmeans_scores.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(score_rows)

    log.info(f"Best k={best_k} | silhouette={best_score:.4f} | "
             f"stability_spread={best_row['stability_spread']:.2%}")
    rfm_scaled.unpersist()

    final = best_model.transform(scaler_model.transform(assembler.transform(rfm)))
    return final.select(
        "user_id", "recency", "frequency", "monetary",
        F.col("prediction").alias("cluster"),
    )


def cluster_profiles(segmented: DataFrame) -> DataFrame:
    """Tóm tắt trung vị RFM của từng cụm và sắp xếp theo Monetary giảm dần."""
    profiles = segmented.groupBy("cluster").agg(
        F.count("*").alias("user_count"),
        F.percentile_approx("recency",   0.5).alias("median_recency"),
        F.percentile_approx("frequency", 0.5).alias("median_frequency"),
        F.percentile_approx("monetary",  0.5).alias("median_monetary"),
    ).orderBy(F.desc("median_monetary"))
    return profiles


def assign_business_labels(profiles: DataFrame) -> DataFrame:
    """
    Gán nhãn kinh doanh chuẩn cho từng cụm dựa vào các giá trị trung vị RFM.
    Quy tắc phân loại (4 nhóm điển hình):
      - Cụm top chi tiêu & mua thường xuyên -> 'VIP / Champions'
      - Cụm mua gần đây & tần suất khá      -> 'Loyal Customers (Trung thành)'
      - Cụm mua gần đây nhưng ít lần         -> 'New / Promising (Khách mới)'
      - Cụm lâu ngày không quay lại          -> 'At Risk / Hibernating (Ngủ đông)'
    """
    # Tính ngưỡng phân vị chung để so sánh tương đối
    pdf = profiles.toPandas()
    if pdf.empty:
        return profiles

    med_r = pdf["median_recency"].median()
    med_m = pdf["median_monetary"].median()

    labels = []
    for _, row in pdf.iterrows():
        r = row["median_recency"]
        m = row["median_monetary"]

        if len(pdf) < 4:
            lbl = "VIP / Champions" if m >= med_m else "Other buyers (needs further segmentation)"
        elif m >= med_m and r <= med_r:
            lbl = "VIP / Champions"
        elif m >= med_m and r > med_r:
            lbl = "At Risk / Can't Lose (Nguy cơ rời bỏ)"
        elif m < med_m and r <= med_r:
            lbl = "New / Promising (Khách mới tiềm năng)"
        else:
            lbl = "Hibernating (Ngủ đông / Không hoạt động)"
        labels.append(lbl)

    pdf["segment_name"] = labels
    spark = SparkSession.getActiveSession()
    if spark:
        return spark.createDataFrame(pdf)
    return profiles


def plot_elbow_and_silhouette(
    scores_path: Path | str | None = None,
    output_path: Path | str | None = None,
) -> None:
    """
    Vẽ 2 biểu đồ trực quan (Elbow Method & Silhouette Score) từ file kết quả kmeans_scores.csv.
    Phục vụ trực tiếp cho báo cáo Chương 2 và Chương 6.
    """
    import matplotlib.pyplot as plt
    import pandas as pd

    if scores_path is None:
        base_dir = Path("/app/results" if Path("/app").exists() else "results")
        scores_path = base_dir / "kmeans_scores.csv"

    scores_file = Path(scores_path)
    if not scores_file.exists():
        log.warning(f"Chưa có file {scores_file} để vẽ biểu đồ.")
        return

    df = pd.read_csv(scores_file)
    if df.empty or "k" not in df.columns:
        return

    if output_path is None:
        fig_dir = Path("/app/docs/figures" if Path("/app").exists() else "docs/figures")
        fig_dir.mkdir(parents=True, exist_ok=True)
        output_path = fig_dir / "kmeans_elbow_silhouette.png"

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))

    # Subplot 1: Elbow method (Inertia)
    ax1.plot(df["k"], df["inertia"], marker="o", color="#2b5c8f", linewidth=2, markersize=6)
    ax1.set_title("Elbow Method (Inertia / WCSS theo k)", fontsize=11, fontweight="bold")
    ax1.set_xlabel("Số cụm (k)")
    ax1.set_ylabel("Inertia (Tổng bình phương khoảng cách)")
    ax1.grid(True, linestyle="--", alpha=0.5)

    # Subplot 2: Silhouette score
    best_row = df.loc[df["silhouette"].idxmax()]
    ax2.plot(df["k"], df["silhouette"], marker="s", color="#e27c3e", linewidth=2, markersize=6)
    ax2.axvline(best_row["k"], color="red", linestyle=":", alpha=0.7,
                label=f"Silhouette cao nhất: k={int(best_row['k'])} ({best_row['silhouette']:.4f})")
    ax2.set_title("Silhouette Coefficient theo k", fontsize=11, fontweight="bold")
    ax2.set_xlabel("Số cụm (k)")
    ax2.set_ylabel("Silhouette Score")
    ax2.legend()
    ax2.grid(True, linestyle="--", alpha=0.5)

    fig.text(0.5, 0.01, "Mỗi k chọn nghiệm có inertia thấp nhất trong ba seed; xem bảng seed để đánh giá độ ổn định.",
             ha="center", fontsize=9)
    plt.tight_layout(rect=(0, 0.05, 1, 1))
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    log.info(f"Đã lưu biểu đồ Elbow & Silhouette tại: {output_path}")


def check_kmeans_stability(rfm: DataFrame, k: int, seeds: list[int] | None = None,
                           max_iter: int = 20) -> list[dict]:
    """
    Kiểm tra tính ổn định của thuật toán KMeans khi thay đổi seed khởi tạo.
    Nếu phân phối các cụm tương đồng thì thuật toán đạt độ ổn định cao.

    `max_iter` phải được truyền từ cùng cấu hình với `run_kmeans`; nếu không,
    phép kiểm tra ổn định được đo ở cấu hình khác với mô hình chính và không
    còn so sánh được.
    """
    seeds = seeds or [42, 0, 123]
    log.info(f"Kiểm tra tính ổn định KMeans với k={k} qua các seed {seeds} "
             f"(maxIter={max_iter})...")
    results = []
    features = ["recency", "frequency", "monetary"]
    for feat in features:
        rfm = rfm.withColumn(f"log_{feat}", F.log1p(F.col(feat)))
    assembler = VectorAssembler(inputCols=[f"log_{f}" for f in features], outputCol="features_raw")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    scaler_model = scaler.fit(assembler.transform(rfm))
    rfm_scaled = scaler_model.transform(assembler.transform(rfm)).cache()

    for seed in seeds:
        km = KMeans(k=k, seed=seed, maxIter=max_iter, featuresCol="features")
        model = km.fit(rfm_scaled)
        preds = model.transform(rfm_scaled)
        profiles = cluster_profiles(
            preds.select("user_id", "recency", "frequency", "monetary",
                         F.col("prediction").alias("cluster"))
        )
        for profile in profiles.collect():
            results.append({
                "seed": seed, "k": k, "cluster": profile["cluster"],
                "user_count": profile["user_count"],
                "median_recency": profile["median_recency"],
                "median_frequency": profile["median_frequency"],
                "median_monetary": profile["median_monetary"],
                "inertia": round(model.summary.trainingCost, 2),
            })
        log.info(f"  Seed {seed}: {len([r for r in results if r['seed'] == seed])} profiles")

    rfm_scaled.unpersist()
    return results
