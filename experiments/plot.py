"""
experiments/plot.py
Sinh biểu đồ từ results/experiment_log.csv.
Chạy: python experiments/plot.py [--experiments E1 E2 E3 E4 E5]

Tất cả biểu đồ lưu vào docs/figures/.
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd

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
        print("E1: chưa có dữ liệu.")
        return

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
        print("E2: chưa có dữ liệu.")
        return

    agg = e2.groupby("experiment")["wall_time_s"].median().reset_index()
    t1_row = agg[agg["experiment"] == "E2_local_1"]
    if t1_row.empty:
        print("E2: thiếu E2_local_1 để tính speedup.")
        return

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
        print("E3: chưa có dữ liệu.")
        return

    # Tách label và query
    e3["fmt_label"] = e3["experiment"].str.extract(r"E3_(.+)_(Q\d)")
    e3[["fmt", "query"]] = e3["experiment"].str.extract(r"E3_(.+)_(Q\d)")

    agg = (
        e3.groupby(["fmt", "query"])["wall_time_s"]
          .median()
          .unstack("query")
          .reset_index()
    )

    fig, ax = plt.subplots(figsize=(10, 5))
    x = range(len(agg))
    width = 0.25
    queries = [c for c in ["Q1", "Q2", "Q3"] if c in agg.columns]
    colors = ["#4c72b0", "#dd8452", "#55a868"]
    for i, (q, color) in enumerate(zip(queries, colors)):
        ax.bar([xi + i * width for xi in x], agg[q], width, label=q, color=color)

    ax.set_xticks([xi + width for xi in x])
    ax.set_xticklabels(agg["fmt"], rotation=20, ha="right")
    ax.set_ylabel("Wall time (s) — trung vị")
    ax.set_title("E3 — Thời gian truy vấn theo định dạng lưu trữ")
    ax.legend(title="Truy vấn")
    ax.grid(True, axis="y", linestyle="--", alpha=0.4)
    _save(fig, "e3_format_time.png")


# =============================================================================
# E4: Tuning — shuffle partitions và salting
# =============================================================================

def plot_e4(df: pd.DataFrame) -> None:
    e4 = df[df["experiment"].str.startswith("E4_")].copy()
    if e4.empty:
        print("E4: chưa có dữ liệu.")
        return

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
    # Earlier E5 runs used different samples and Spark counted only groups.
    # Their timings must not be mixed with the corrected benchmark.
    e5 = df[
        df["experiment"].str.startswith("E5_")
        & df["notes"].fillna("").str.contains("validated_sample", regex=False)
    ].copy()
    if e5.empty:
        print("E5: chưa có dữ liệu.")
        return

    e5["engine"] = e5["experiment"].str.extract(r"E5_(.+)_frac\d+")
    e5["fraction_pct"] = e5["experiment"].str.extract(r"E5_.+_frac(\d+)").astype(float)
    # Keep the latest complete three repetitions per series; earlier smoke runs
    # and reruns should not shift the reported median.
    e5 = e5.groupby(["tier", "engine", "fraction_pct"], sort=False).tail(3)
    agg = (
        e5.groupby(["tier", "engine", "fraction_pct"])["wall_time_s"]
          .median()
          .reset_index()
          .sort_values("fraction_pct")
    )

    colors = {
        "duckdb": "#1f77b4", "pandas_chunk": "#ff7f0e",
        "spark_local": "#d62728", "spark_cluster": "#2ca02c",
    }
    panels = [
        ("Tất cả engine", list(colors)),
        ("Chi tiết: trừ Spark Cluster", ["duckdb", "pandas_chunk", "spark_local"]),
        ("Chi tiết: DuckDB và Pandas", ["duckdb", "pandas_chunk"]),
    ]
    for tier, tier_rows in agg.groupby("tier"):
        fig, axes = plt.subplots(1, 3, figsize=(18, 5), sharex=True)
        for ax, (title, visible) in zip(axes, panels):
            for engine in visible:
                grp = tier_rows[tier_rows["engine"] == engine]
                if not grp.empty:
                    ax.plot(grp["fraction_pct"], grp["wall_time_s"], marker="o",
                            color=colors[engine], label=engine)
            ax.set_title(title)
            ax.set_xlabel("Dữ liệu (%)")
            ax.set_ylabel("Giây — trung vị 3 lần")
            ax.set_xlim(0, 105)
            ax.xaxis.set_major_formatter(ticker.PercentFormatter())
            ax.grid(True, linestyle="--", alpha=0.4)
            ax.legend(fontsize=8)
        fig.suptitle(f"E5 — Thời gian truy vấn tier {tier} trên cùng tệp mẫu")
        fig.text(0.5, 0.01, "Tạo mẫu và khởi tạo SparkSession nằm ngoài phép đo.",
                 ha="center", fontsize=9)
        fig.tight_layout(rect=(0, 0.04, 1, 0.95))
        _save(fig, f"e5_crossover_{tier}.png")
        if tier == "S":
            shutil.copyfile(FIGURES_DIR / "e5_crossover_S.png", FIGURES_DIR / "e5_crossover.png")


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
