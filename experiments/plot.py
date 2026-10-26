"""
experiments/plot.py
Sinh biểu đồ từ results/experiment_log.csv.
Chạy: python experiments/plot.py [--experiments E1 E2 E3 E4 E5]

Tất cả biểu đồ lưu vào docs/figures/.
"""
from __future__ import annotations

import argparse
import statistics
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

RESULTS_DIR = Path(__file__).parent.parent / "results"
FIGURES_DIR = Path(__file__).parent.parent / "docs" / "figures"
LOG_FILE = RESULTS_DIR / "experiment_log.csv"

plt.rcParams.update({"figure.dpi": 150, "font.size": 10})


def _save(fig: plt.Figure, name: str) -> None:
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    out = FIGURES_DIR / name
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"[OK] {out}")


def load_log() -> pd.DataFrame:
    if not LOG_FILE.exists():
        raise FileNotFoundError(f"Chưa có {LOG_FILE}. Chạy thí nghiệm trước.")
    df = pd.read_csv(LOG_FILE)
    df = df[df["success"].astype(str).str.lower() == "true"]
    return df


# =============================================================================
# E1: Wall time theo fraction dữ liệu
# =============================================================================

def plot_e1(df: pd.DataFrame) -> None:
    e1 = df[df["experiment"].str.match(r"E1_frac\d+")].copy()
    if e1.empty:
        print("E1: chưa có dữ liệu."); return

    e1["fraction_pct"] = e1["experiment"].str.extract(r"E1_frac(\d+)").astype(float)
    agg = (
        e1.groupby("fraction_pct")["wall_time_s"]
          .agg(["median", "min", "max"])
          .reset_index()
          .sort_values("fraction_pct")
    )

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(agg["fraction_pct"], agg["median"], marker="o", color="steelblue", label="Trung vị")
    ax.fill_between(agg["fraction_pct"], agg["min"], agg["max"],
                    alpha=0.2, color="steelblue", label="Min–Max")
    ax.set_xlabel("Dữ liệu (%)")
    ax.set_ylabel("Wall time (s)")
    ax.set_title("E1 — Wall time theo kích thước dữ liệu (Spark, tài nguyên cố định)")
    ax.legend()
    ax.xaxis.set_major_formatter(ticker.PercentFormatter())
    ax.grid(True, linestyle="--", alpha=0.4)
    _save(fig, "e1_scale_data.png")


# =============================================================================
# E2: Speedup
# =============================================================================

def plot_e2(df: pd.DataFrame) -> None:
    e2 = df[df["experiment"].str.startswith("E2_")].copy()
    if e2.empty:
        print("E2: chưa có dữ liệu."); return

    agg = e2.groupby("experiment")["wall_time_s"].median().reset_index()
    t1_row = agg[agg["experiment"] == "E2_local_1"]
    if t1_row.empty:
        print("E2: thiếu E2_local_1 để tính speedup."); return

    t1 = t1_row["wall_time_s"].values[0]
    agg["speedup"] = t1 / agg["wall_time_s"]

    order = ["E2_local_1", "E2_local_2", "E2_local_4", "E2_local_star", "E2_cluster"]
    agg["experiment"] = pd.Categorical(agg["experiment"], categories=order, ordered=True)
    agg = agg.sort_values("experiment").dropna(subset=["experiment"])

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(agg["experiment"], agg["speedup"], color="steelblue")
    ax.axhline(1, color="gray", linestyle="--", linewidth=0.8)
    for bar, val in zip(bars, agg["speedup"]):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
                f"{val:.2f}×", ha="center", va="bottom", fontsize=9)
    ax.set_xlabel("Cấu hình")
    ax.set_ylabel("Speedup (T₁ / Tₙ)")
    ax.set_title("E2 — Speedup theo tài nguyên (Spark)")
    ax.tick_params(axis="x", rotation=25)
    ax.grid(True, axis="y", linestyle="--", alpha=0.4)
    _save(fig, "e2_speedup.png")


# =============================================================================
# E3: Dung lượng và thời gian theo định dạng
# =============================================================================

def plot_e3(df: pd.DataFrame) -> None:
    e3 = df[df["experiment"].str.startswith("E3_")].copy()
    if e3.empty:
        print("E3: chưa có dữ liệu."); return

    # Tách label và query (VD: E3_parquet_snappy_p_Q3 → parquet_snappy_p, Q3)
    e3[["fmt", "query"]] = e3["experiment"].str.extract(r"E3_(.+)_(Q\d)")

    order = ["csv", "parquet_snappy", "parquet_snappy_p", "parquet_zstd", "parquet_zstd_p"]
    agg = (
        e3.groupby(["fmt", "query"])["wall_time_s"]
          .median()
          .unstack("query")
    )
    agg = agg.reindex([f for f in order if f in agg.index] +
                      [f for f in agg.index if f not in order]).reset_index()

    fig, ax = plt.subplots(figsize=(10, 5))
    x = range(len(agg))
    width = 0.25
    queries = [c for c in ["Q1", "Q2", "Q3"] if c in agg.columns]
    colors = ["#4c72b0", "#dd8452", "#55a868"]
    for i, (q, color) in enumerate(zip(queries, colors)):
        bars = ax.bar([xi + i * width for xi in x], agg[q], width, label=q, color=color)
        for bar, val in zip(bars, agg[q]):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                    f"{val:.1f}", ha="center", va="bottom", fontsize=7)

    ax.set_xticks([xi + width for xi in x])
    ax.set_xticklabels(agg["fmt"], rotation=20, ha="right")
    ax.set_ylabel("Wall time (s) — trung vị")
    ax.set_title("E3 — Thời gian truy vấn theo định dạng lưu trữ")
    ax.legend(title="Truy vấn")
    ax.grid(True, axis="y", linestyle="--", alpha=0.4)
    _save(fig, "e3_format_time.png")

    # Bảng tóm tắt cho báo cáo: speedup so với CSV và partition vs không partition
    summary = agg.set_index("fmt")
    rows = []
    for fmt in summary.index:
        row = {"fmt": fmt}
        for q in queries:
            row[f"{q}_median_s"] = round(summary.loc[fmt, q], 3)
            if "csv" in summary.index:
                row[f"{q}_speedup_vs_csv"] = round(summary.loc["csv", q] / summary.loc[fmt, q], 2)
        rows.append(row)
    pd.DataFrame(rows).to_csv(RESULTS_DIR / "e3_summary.csv", index=False)
    print(f"[OK] {RESULTS_DIR / 'e3_summary.csv'}")

    for base in ("parquet_snappy", "parquet_zstd"):
        if base in summary.index and f"{base}_p" in summary.index and "Q3" in summary.columns:
            ratio = summary.loc[base, "Q3"] / summary.loc[f"{base}_p", "Q3"]
            print(f"  Q3 {base}: partition nhanh hon {ratio:.2f}x so voi khong partition")
    if "csv" in summary.index and "parquet_snappy" in summary.index and "Q2" in summary.columns:
        ratio = summary.loc["csv", "Q2"] / summary.loc["parquet_snappy", "Q2"]
        print(f"  Q2: parquet_snappy nhanh hon CSV {ratio:.2f}x")

    # Dung lượng (từ results/e3_sizes.csv do e3_format.py ghi)
    sizes_file = RESULTS_DIR / "e3_sizes.csv"
    if sizes_file.exists():
        sizes = pd.read_csv(sizes_file)
        sizes["label"] = pd.Categorical(
            sizes["label"],
            categories=[f for f in order if f in set(sizes["label"])] +
                       [f for f in sizes["label"] if f not in order],
            ordered=True,
        )
        sizes = sizes.sort_values("label")
        fig, ax = plt.subplots(figsize=(8, 4.5))
        bars = ax.bar(sizes["label"].astype(str), sizes["size_mb"], color="#8172b2")
        for bar, mb, ratio in zip(bars, sizes["size_mb"], sizes.get("ratio_vs_csv", [""] * len(sizes))):
            label = f"{mb:,.0f} MB" + (f"\n({ratio}×)" if str(ratio) not in ("", "nan") else "")
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                    label, ha="center", va="bottom", fontsize=8)
        ax.set_ylabel("Dung lượng (MB)")
        ax.set_title("E3 — Dung lượng lưu trữ theo định dạng (tỷ lệ nén so với CSV)")
        ax.tick_params(axis="x", rotation=20)
        ax.grid(True, axis="y", linestyle="--", alpha=0.4)
        _save(fig, "e3_format_size.png")
    else:
        print("E3: chưa có results/e3_sizes.csv — bỏ qua biểu đồ dung lượng.")


# =============================================================================
# E4: Tuning — shuffle partitions và salting
# =============================================================================

def plot_e4(df: pd.DataFrame) -> None:
    e4 = df[df["experiment"].str.startswith("E4_")].copy()
    if e4.empty:
        print("E4: chưa có dữ liệu."); return

    # T1: shuffle partitions
    t1 = e4[e4["experiment"].str.startswith("E4_T1_")].copy()
    if not t1.empty:
        t1["sp"] = t1["experiment"].str.extract(r"E4_T1_sp(\d+)").astype(float)
        agg_t1 = t1.groupby("sp")["wall_time_s"].median().reset_index().sort_values("sp")

        fig, ax = plt.subplots(figsize=(7, 4))
        ax.plot(agg_t1["sp"], agg_t1["wall_time_s"], marker="s", color="darkorange")
        ax.set_xlabel("shuffle.partitions")
        ax.set_ylabel("Wall time (s)")
        ax.set_title("E4-T1 — Wall time theo shuffle.partitions")
        ax.grid(True, linestyle="--", alpha=0.4)
        _save(fig, "e4_shuffle_partitions.png")

    # T4: skew — salting
    t4 = e4[e4["experiment"].str.startswith("E4_T4_")].copy()
    if not t4.empty:
        agg_t4 = t4.groupby("experiment")["wall_time_s"].median().reset_index()

        fig, ax = plt.subplots(figsize=(5, 4))
        ax.bar(agg_t4["experiment"].str.replace("E4_T4_", ""), agg_t4["wall_time_s"],
               color=["#c44e52", "#55a868"])
        ax.set_ylabel("Wall time (s)")
        ax.set_title("E4-T4 — Skew: không salting vs salting")
        ax.grid(True, axis="y", linestyle="--", alpha=0.4)
        _save(fig, "e4_salting.png")


# =============================================================================
# E5: Crossover
# =============================================================================

def plot_e5(df: pd.DataFrame) -> None:
    e5 = df[df["experiment"].str.startswith("E5_")].copy()
    if e5.empty:
        print("E5: chưa có dữ liệu."); return

    e5["engine"] = e5["experiment"].str.extract(r"E5_(.+)_frac\d+")
    e5["fraction_pct"] = e5["experiment"].str.extract(r"E5_.+_frac(\d+)").astype(float)
    agg = (
        e5.groupby(["engine", "fraction_pct"])["wall_time_s"]
          .median()
          .reset_index()
          .sort_values("fraction_pct")
    )

    fig, ax = plt.subplots(figsize=(9, 5))
    for engine, grp in agg.groupby("engine"):
        ax.plot(grp["fraction_pct"], grp["wall_time_s"], marker="o", label=engine)

    ax.set_xlabel("Dữ liệu (%)")
    ax.set_ylabel("Wall time (s) — trung vị")
    ax.set_title("E5 — Crossover: Pandas / DuckDB / Spark")
    ax.legend()
    ax.xaxis.set_major_formatter(ticker.PercentFormatter())
    ax.grid(True, linestyle="--", alpha=0.4)
    _save(fig, "e5_crossover.png")


# =============================================================================
# Entry point
# =============================================================================

PLOTTERS = {
    "E1": plot_e1,
    "E2": plot_e2,
    "E3": plot_e3,
    "E4": plot_e4,
    "E5": plot_e5,
}


def main(experiments: list[str]) -> None:
    df = load_log()
    for exp in experiments:
        fn = PLOTTERS.get(exp)
        if fn is None:
            print(f"Không có plotter cho {exp}")
            continue
        fn(df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sinh biểu đồ từ experiment_log.csv")
    parser.add_argument(
        "--experiments", nargs="+",
        default=list(PLOTTERS.keys()),
        choices=list(PLOTTERS.keys()),
    )
    args = parser.parse_args()
    main(args.experiments)
