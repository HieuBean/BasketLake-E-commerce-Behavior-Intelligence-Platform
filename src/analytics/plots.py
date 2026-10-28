"""
src/analytics/plots.py
Owner: M4 (Analytics 1)
Reviewer: M5

Sinh toàn bộ biểu đồ analytics từ results/.
Chạy sau make analytics:
    python src/analytics/plots.py

Output: docs/figures/*.png
"""
from __future__ import annotations

import glob
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

RESULTS = Path("results")
FIGURES = Path("docs/figures")

plt.rcParams.update({
    "figure.dpi": 150,
    "font.size": 10,
    "axes.spines.top": False,
    "axes.spines.right": False,
})


# =============================================================================
# Helper
# =============================================================================

def _read_csv_dir(name: str) -> pd.DataFrame:
    """Đọc tất cả part-*.csv trong results/<name>/."""
    pattern = str(RESULTS / name / "*.csv")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(
            f"Không tìm thấy file tại {pattern}. Chạy 'make analytics TIER=M' trước."
        )
    return pd.concat([pd.read_csv(f) for f in files], ignore_index=True)


def _save(fig: plt.Figure, name: str) -> None:
    FIGURES.mkdir(parents=True, exist_ok=True)
    out = FIGURES / name
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"  ✓ {out}")


# =============================================================================
# 1. Doanh thu theo ngày (line chart)
# =============================================================================

def plot_revenue_timeseries() -> None:
    """Line chart: doanh thu theo ngày."""
    df = _read_csv_dir("revenue_by_day")
    df["event_date"] = pd.to_datetime(df["event_date"])
    df = df.sort_values("event_date")

    fig, axes = plt.subplots(2, 1, figsize=(13, 7), sharex=True)

    # Subplot 1: Doanh thu
    axes[0].plot(df["event_date"], df["revenue"] / 1e6,
                 color="steelblue", linewidth=1.5, label="Revenue")
    axes[0].fill_between(df["event_date"], df["revenue"] / 1e6, alpha=0.15, color="steelblue")
    axes[0].set_ylabel("Doanh thu (triệu USD)")
    axes[0].set_title("Doanh thu và số phiên theo ngày")
    axes[0].yaxis.set_major_formatter(mticker.FormatStrFormatter("$%.1fM"))
    axes[0].grid(True, linestyle="--", alpha=0.35)

    # Subplot 2: Số phiên
    axes[1].bar(df["event_date"], df["sessions"] / 1e3,
                color="coral", alpha=0.7, label="Sessions (nghìn)")
    axes[1].set_ylabel("Số phiên (nghìn)")
    axes[1].set_xlabel("Ngày")
    axes[1].yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0fK"))
    axes[1].grid(True, linestyle="--", alpha=0.35, axis="y")

    fig.autofmt_xdate(rotation=30)
    fig.tight_layout()
    _save(fig, "revenue_timeseries.png")


# =============================================================================
# 2. Doanh thu theo danh mục (horizontal bar)
# =============================================================================

def plot_revenue_by_category() -> None:
    """Horizontal bar: top 15 category theo doanh thu."""
    df = _read_csv_dir("revenue_by_category")
    df = df.nlargest(15, "revenue").sort_values("revenue")

    fig, ax = plt.subplots(figsize=(9, 6))
    bars = ax.barh(df["category_l1"], df["revenue"] / 1e6,
                   color="steelblue", edgecolor="white")

    # Nhãn giá trị
    for bar in bars:
        w = bar.get_width()
        ax.text(w + 0.02, bar.get_y() + bar.get_height() / 2,
                f"${w:.2f}M", va="center", fontsize=8)

    ax.set_xlabel("Doanh thu (triệu USD)")
    ax.set_title("Top 15 danh mục theo doanh thu")
    ax.xaxis.set_major_formatter(mticker.FormatStrFormatter("$%.1fM"))
    ax.grid(True, linestyle="--", alpha=0.35, axis="x")
    fig.tight_layout()
    _save(fig, "revenue_by_category.png")


# =============================================================================
# 3. Heatmap giờ × thứ (trafic mùa vụ)
# =============================================================================

def plot_hourly_heatmap() -> None:
    """Heatmap: event_count theo thứ (1–7) × giờ (0–23)."""
    df = _read_csv_dir("hourly_pattern")

    # Pivot thành ma trận 7×24
    pivot = df.pivot_table(
        index="day_of_week", columns="event_hour",
        values="event_count", aggfunc="sum", fill_value=0,
    )

    day_labels = ["CN", "T2", "T3", "T4", "T5", "T6", "T7"]
    pivot.index = [day_labels[i - 1] for i in pivot.index]

    fig, ax = plt.subplots(figsize=(14, 4))
    im = ax.imshow(pivot.values, aspect="auto", cmap="YlOrRd",
                   interpolation="nearest")
    plt.colorbar(im, ax=ax, label="Số sự kiện")

    ax.set_xticks(range(24))
    ax.set_xticklabels(range(24), fontsize=8)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    ax.set_xlabel("Giờ trong ngày")
    ax.set_ylabel("Thứ")
    ax.set_title("Phân phối sự kiện theo thứ và giờ (heatmap)")
    fig.tight_layout()
    _save(fig, "hourly_heatmap.png")


# =============================================================================
# 4. Funnel chart theo danh mục (grouped bar)
# =============================================================================

def plot_funnel_by_category() -> None:
    """Grouped bar: view → cart → purchase cho top 10 danh mục."""
    df = _read_csv_dir("funnel_by_category")
    top10 = df.nlargest(10, "sessions_with_view").copy()
    top10 = top10.sort_values("sessions_with_view", ascending=False)

    cats = top10["category_l1"].tolist()
    x = np.arange(len(cats))
    w = 0.25

    fig, ax = plt.subplots(figsize=(13, 5))
    ax.bar(x - w,   top10["sessions_with_view"],    w, label="View",     color="#4c72b0")
    ax.bar(x,       top10["sessions_with_cart"],     w, label="Cart",     color="#dd8452")
    ax.bar(x + w,   top10["sessions_with_purchase"], w, label="Purchase", color="#55a868")

    # Tỷ lệ overall conversion trên đỉnh bar purchase
    for i, row in enumerate(top10.itertuples()):
        rate = row.sessions_with_purchase / max(row.sessions_with_view, 1) * 100
        ax.text(i + w, row.sessions_with_purchase + 200,
                f"{rate:.1f}%", ha="center", va="bottom", fontsize=7.5, color="#55a868")

    ax.set_xticks(x)
    ax.set_xticklabels(cats, rotation=30, ha="right", fontsize=9)
    ax.set_ylabel("Số phiên")
    ax.set_title("Funnel View → Cart → Purchase theo danh mục (Top 10)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.legend()
    ax.grid(True, linestyle="--", alpha=0.35, axis="y")
    fig.tight_layout()
    _save(fig, "funnel_by_category.png")


# =============================================================================
# 5. Funnel waterfall (toàn pipeline)
# =============================================================================

def plot_funnel_waterfall() -> None:
    """Waterfall chart cho funnel tổng (tổng tất cả danh mục)."""
    df = _read_csv_dir("funnel_by_category")

    total_view     = df["sessions_with_view"].sum()
    total_cart     = df["sessions_with_cart"].sum()
    total_purchase = df["sessions_with_purchase"].sum()

    steps = ["View", "Cart", "Purchase"]
    values = [total_view, total_cart, total_purchase]
    colors = ["#4c72b0", "#dd8452", "#55a868"]

    fig, ax = plt.subplots(figsize=(7, 5))
    bars = ax.bar(steps, values, color=colors, edgecolor="white", width=0.5)

    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + total_view * 0.01,
                f"{val:,.0f}", ha="center", va="bottom", fontsize=10)

    # Drop-off rate giữa các bước
    for i in range(len(values) - 1):
        drop = (values[i] - values[i + 1]) / values[i] * 100
        ax.annotate(
            f"−{drop:.1f}%",
            xy=(i + 0.5, (values[i] + values[i + 1]) / 2),
            ha="center", va="center", fontsize=9,
            color="crimson", fontweight="bold",
        )

    ax.set_ylabel("Số phiên")
    ax.set_title("Funnel tổng: View → Cart → Purchase")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.grid(True, linestyle="--", alpha=0.35, axis="y")
    fig.tight_layout()
    _save(fig, "funnel_waterfall.png")


# =============================================================================
# 6. Cohort retention heatmap
# =============================================================================

def plot_cohort_retention() -> None:
    """Heatmap cohort × tuần với retention rate."""
    try:
        df = _read_csv_dir("cohort_retention")
    except FileNotFoundError:
        print("  ⚠ cohort_retention chưa có — bỏ qua (cần ≥2 tháng dữ liệu)")
        return

    df["cohort_week"] = pd.to_datetime(df["cohort_week"]).dt.strftime("%Y-%m-%d")
    pivot = df.pivot_table(
        index="cohort_week", columns="week_number",
        values="retention_rate", aggfunc="mean", fill_value=0,
    )

    fig, ax = plt.subplots(figsize=(12, max(4, len(pivot) * 0.5)))
    im = ax.imshow(pivot.values, aspect="auto", cmap="Blues",
                   vmin=0, vmax=1, interpolation="nearest")
    plt.colorbar(im, ax=ax, label="Retention rate")

    # Nhãn giá trị trong ô
    for i in range(pivot.shape[0]):
        for j in range(pivot.shape[1]):
            v = pivot.values[i, j]
            if v > 0:
                ax.text(j, i, f"{v:.0%}", ha="center", va="center",
                        fontsize=7, color="black" if v < 0.6 else "white")

    ax.set_xticks(range(pivot.shape[1]))
    ax.set_xticklabels([f"T+{w}" for w in pivot.columns], fontsize=8)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index, fontsize=8)
    ax.set_xlabel("Tuần kể từ lần mua đầu tiên")
    ax.set_ylabel("Cohort (tuần mua đầu tiên)")
    ax.set_title("Cohort Retention — % user quay lại mua theo tuần")
    fig.tight_layout()
    _save(fig, "cohort_retention.png")


# =============================================================================
# 7. RFM cluster profiles (radar + bar)
# =============================================================================

def plot_cluster_profiles() -> None:
    """Bar chart: RFM trung vị của từng cụm KMeans."""
    try:
        df = _read_csv_dir("cluster_profiles")
    except FileNotFoundError:
        print("  ⚠ cluster_profiles chưa có — bỏ qua")
        return

    df = df.sort_values("cluster")
    n_clusters = len(df)
    metrics = ["median_recency", "median_frequency", "median_monetary"]
    labels  = ["Recency (ngày)", "Frequency (lần)", "Monetary (USD)"]
    colors  = plt.cm.tab10.colors

    fig, axes = plt.subplots(1, 3, figsize=(13, 4))
    for ax, metric, label in zip(axes, metrics, labels):
        bars = ax.bar(
            [f"Cụm {c}" for c in df["cluster"]],
            df[metric],
            color=colors[:n_clusters],
            edgecolor="white",
        )
        for bar, val in zip(bars, df[metric]):
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() * 1.02,
                    f"{val:.0f}", ha="center", va="bottom", fontsize=9)
        ax.set_title(label)
        ax.set_xlabel("Cụm")
        ax.grid(True, linestyle="--", alpha=0.35, axis="y")

    fig.suptitle("Hồ sơ các cụm KMeans (RFM trung vị)", fontsize=12, fontweight="bold")
    fig.tight_layout()
    _save(fig, "cluster_profiles.png")


# =============================================================================
# 8. KMeans elbow + silhouette (nếu có log)
# =============================================================================

def plot_kmeans_selection() -> None:
    """Elbow + silhouette chart để chọn k tối ưu."""
    score_file = RESULTS / "kmeans_scores.csv"
    if not score_file.exists():
        print("  ⚠ kmeans_scores.csv chưa có — bỏ qua")
        print("    Hint: thêm log inertia + silhouette vào rfm_kmeans.py")
        return

    df = pd.read_csv(score_file)  # columns: k, inertia, silhouette

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4))

    # Elbow
    ax1.plot(df["k"], df["inertia"], marker="o", color="steelblue")
    ax1.set_xlabel("k (số cụm)")
    ax1.set_ylabel("Inertia (WCSS)")
    ax1.set_title("Elbow Method — chọn k")
    ax1.grid(True, linestyle="--", alpha=0.35)

    # Silhouette
    best_k = df.loc[df["silhouette"].idxmax(), "k"]
    ax2.plot(df["k"], df["silhouette"], marker="s", color="darkorange")
    ax2.axvline(best_k, color="crimson", linestyle="--",
                label=f"k={int(best_k)} (silhouette max)")
    ax2.set_xlabel("k (số cụm)")
    ax2.set_ylabel("Silhouette score")
    ax2.set_title("Silhouette Score — chọn k")
    ax2.legend(fontsize=9)
    ax2.grid(True, linestyle="--", alpha=0.35)

    fig.suptitle("Lựa chọn k cho KMeans", fontsize=12, fontweight="bold")
    fig.tight_layout()
    _save(fig, "kmeans_selection.png")


# =============================================================================
# Entry point
# =============================================================================

PLOTTERS = {
    "revenue_timeseries":    plot_revenue_timeseries,
    "revenue_by_category":   plot_revenue_by_category,
    "hourly_heatmap":        plot_hourly_heatmap,
    "funnel_by_category":    plot_funnel_by_category,
    "funnel_waterfall":      plot_funnel_waterfall,
    "cohort_retention":      plot_cohort_retention,
    "cluster_profiles":      plot_cluster_profiles,
    "kmeans_selection":      plot_kmeans_selection,
}


def main(targets: list[str] | None = None) -> None:
    run = targets or list(PLOTTERS.keys())
    print(f"Đang vẽ {len(run)} biểu đồ → {FIGURES}/")
    for name in run:
        fn = PLOTTERS.get(name)
        if fn is None:
            print(f"  ✗ Không tìm thấy plotter '{name}'")
            continue
        try:
            fn()
        except FileNotFoundError as e:
            print(f"  ✗ {name}: {e}")
        except Exception as e:
            print(f"  ✗ {name}: lỗi không mong muốn — {e}")
    print("Xong.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Sinh biểu đồ analytics")
    parser.add_argument(
        "--targets", nargs="+",
        choices=list(PLOTTERS.keys()),
        help="Biểu đồ cần vẽ (mặc định: tất cả)",
    )
    args = parser.parse_args()
    main(args.targets)
