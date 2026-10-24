"""
experiments/e6_correctness.py
Owner: A (bản tham chiếu oracle), E (đối chiếu)
Reviewer: D

E6 — Đối chiếu kết quả Spark với bản tham chiếu độc lập.
Dùng DuckDB tính 3 KPI trên tầng Silver Parquet:
  - total_revenue
  - total_sessions
  - conversion_rate
So sánh với kết quả từ Spark pipeline (results/kpi_summary.csv).
Ngưỡng chấp nhận: sai số diff_pct <= 0.1%.
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from src.common.config import get_paths, load_config
from src.common.input_manifest import verify_kpi_manifest, verify_silver_manifest
from src.common.logger import get_logger
from src.common.metrics import RunRecord, write_record

log = get_logger(__name__)

ORACLE_QUERIES = {
    "total_revenue": """
        SELECT ROUND(SUM(price), 2) AS total_revenue
        FROM read_parquet('{silver}/**/*.parquet')
        WHERE event_type = 'purchase' AND price > 0
    """,
    "total_sessions": """
        SELECT COUNT(*) AS total_sessions
        FROM (
            SELECT user_id, user_session
            FROM read_parquet('{silver}/**/*.parquet')
            GROUP BY user_id, user_session
        )
    """,
    "conversion_rate": """
        WITH sessions AS (
            SELECT user_id, user_session,
                   MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS has_view,
                   MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS converted
            FROM read_parquet('{silver}/**/*.parquet')
            GROUP BY user_id, user_session
        )
        SELECT SUM(converted) * 1.0 / NULLIF(SUM(has_view), 0)
        FROM sessions
    """,
}


def run_oracle(silver_path: str) -> dict[str, float]:
    """Chạy SQL trên Parquet bằng DuckDB (không qua Spark) làm tham chiếu chuẩn."""
    log.info(f"Đang tính KPI Oracle bằng DuckDB trên: {silver_path}")
    con = duckdb.connect()
    results = {}
    for kpi, query in ORACLE_QUERIES.items():
        q = query.format(silver=silver_path)
        val = con.execute(q).fetchone()[0]
        results[kpi] = float(val) if val is not None else 0.0
        log.info(f"  [Oracle] {kpi:16s} = {results[kpi]}")
    con.close()
    return results


def read_spark_kpi(results_dir: Path) -> dict[str, float]:
    """Đọc kết quả KPI do Spark sinh ra tại results/kpi_summary.csv."""
    kpi_file = results_dir / "kpi_summary.csv"
    if not kpi_file.exists():
        raise FileNotFoundError(
            f"Chưa có {kpi_file}. Hãy chạy pipeline trước: 'make analytics TIER=S'"
        )
    df = pd.read_csv(kpi_file)
    row = df.iloc[0].to_dict()
    return {k: float(v) for k, v in row.items()}


def compare(
    oracle: dict[str, float],
    spark_results: dict[str, float],
    tolerance_pct: float = 0.1,
) -> tuple[bool, list[dict[str, Any]]]:
    """
    So sánh giá trị từng KPI giữa Oracle và Spark.
    Trả về (all_ok, report_rows).
    """
    all_ok = True
    report_rows = []

    print("\n" + "=" * 80)
    print(f"{'KPI':<20} | {'Oracle (DuckDB)':<16} | {'Spark Pipeline':<16} | {'Diff %':<10} | {'Status'}")
    print("-" * 80)

    for kpi, o_val in oracle.items():
        s_val = spark_results.get(kpi)
        if s_val is None:
            log.warning(f"Spark kpi_summary.csv thiếu chỉ số: {kpi}")
            all_ok = False
            report_rows.append({
                "metric": kpi, "oracle_value": o_val, "spark_value": None,
                "diff_abs": None, "diff_pct": None, "tolerance_pct": tolerance_pct, "status": "MISSING",
            })
            continue

        diff_abs = abs(o_val - s_val)
        diff_pct = (diff_abs / (abs(o_val) + 1e-9)) * 100.0
        status = "PASSED" if diff_pct <= tolerance_pct else "FAILED"
        if status == "FAILED":
            all_ok = False

        print(f"{kpi:<20} | {o_val:<16.8f} | {s_val:<16.8f} | {diff_pct:<9.4f}% | [{status}]")

        report_rows.append({
            "metric": kpi,
            "oracle_value": round(o_val, 8),
            "spark_value": round(s_val, 8),
            "diff_abs": round(diff_abs, 8),
            "diff_pct": round(diff_pct, 4),
            "tolerance_pct": tolerance_pct,
            "status": status,
        })

    print("=" * 80 + "\n")
    return all_ok, report_rows


def write_e6_report(results_dir: Path, report_rows: list[dict[str, Any]]) -> Path:
    """Ghi bảng đối chiếu ra results/e6_correctness_report.csv."""
    results_dir.mkdir(parents=True, exist_ok=True)
    out_file = results_dir / "e6_correctness_report.csv"
    if report_rows:
        with open(out_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(report_rows[0].keys()))
            writer.writeheader()
            writer.writerows(report_rows)
        log.info(f"Đã lưu báo cáo kiểm chứng E6 tại: {out_file}")
    return out_file


def run(tier: str = "S") -> bool:
    """Entry point thực thi thí nghiệm E6."""
    log.info(f"===== Bắt đầu thí nghiệm E6: Correctness Oracle (Tier={tier}) =====")
    cfg = load_config()
    paths = get_paths(cfg)
    results_dir = Path(paths["results"])
    verify_silver_manifest(paths["silver"], tier)
    verify_kpi_manifest(results_dir, tier, paths["silver"])

    oracle_results = run_oracle(paths["silver"])
    spark_results = read_spark_kpi(results_dir)

    all_ok, report = compare(oracle_results, spark_results, tolerance_pct=0.1)
    write_e6_report(results_dir, report)

    # Ghi nhận vào log thí nghiệm chung
    write_record(RunRecord(
        run_id=f"E6_correctness_{tier}",
        experiment="E6",
        engine="duckdb_oracle",
        tier=tier,
        wall_time_s=0.0,
        success=all_ok,
        notes=f"All metrics within 0.1% tolerance: {all_ok}",
    ))

    return all_ok


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="E6 Correctness Oracle Benchmark")
    parser.add_argument("--tier", default="S", choices=["S", "M", "L", "XL"])
    args = parser.parse_args()
    success = run(args.tier)
    exit(0 if success else 1)
