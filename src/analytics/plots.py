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
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless: render ra file, không cần display

import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker as mticker  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

# Console Windows mặc định cp1252 → in tiếng Việt sẽ crash UnicodeEncodeError.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

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


def _read_single_row(name: str) -> pd.Series | None:
    """Đọc file CSV một dòng (kpi_summary.csv / funnel_totals.csv)."""
    path = RESULTS / name
    if not path.exists():
        return None
    return pd.read_csv(path).iloc[0]


# =============================================================================
# Funnel totals — grain PHIÊN, không cộng dồn theo danh mục
# =============================================================================

FUNNEL_SUM_COLUMNS = ("sessions_with_view", "sessions_with_cart", "sessions_with_purchase")


def funnel_steps_from_totals(totals: dict) -> list[dict]:
    """
    Chuẩn hoá số liệu funnel toàn cục thành list các bước để vẽ.

    Đầu vào là funnel ở grain phiên (mỗi phiên đếm 1 lần), KHÔNG phải tổng
    cộng dồn của `funnel_by_category` — grain của bảng đó là
    (user_session, category_l1) nên cộng lại sẽ đếm trùng phiên chạm nhiều
    danh mục (tier S: 905.652 thay vì 886.657 phiên có view).

    Trả về list các dict: name / count / share_of_view.
    """
    view = int(totals["sessions_with_view"])
    cart = int(totals["sessions_with_cart"])
    purchase = int(totals["sessions_with_purchase"])

    def share(value: int) -> float:
        return value / view if view else 0.0

    return [
        {"name": "View",     "count": view,     "share_of_view": share(view)},
        {"name": "Cart",     "count": cart,     "share_of_view": share(cart)},
        {"name": "Purchase", "count": purchase, "share_of_view": share(purchase)},
    ]


def _read_funnel_totals() -> tuple[dict, str]:
    """
    Lấy funnel toàn cục theo thứ tự ưu tiên:
      1. results/funnel_totals.csv  (run_analytics.py ghi từ fact_sessions)
      2. results/kpi_summary.csv    (cùng nguồn, tên cột khác)
      3. tổng cộng dồn funnel_by_category (phương án cuối, CÓ đếm trùng)
    """
    row = _read_single_row("funnel_totals.csv")
    if row is not None:
        return {
            "sessions_with_view":     int(row["sessions_with_view"]),
            "sessions_with_cart":     int(row["sessions_with_cart"]),
            "sessions_with_purchase": int(row["sessions_with_purchase"]),
        }, "funnel_totals.csv"

    row = _read_single_row("kpi_summary.csv")
    if row is not None:
        return {
            "sessions_with_view":     int(row["sessions_with_view"]),
            "sessions_with_cart":     int(row["sessions_with_cart"]),
            "sessions_with_purchase": int(row["converted_sessions"]),
        }, "kpi_summary.csv"

    df = _read_csv_dir("funnel_by_category")
    print("  ⚠ Không có funnel_totals.csv / kpi_summary.csv —"
          " dùng tổng funnel_by_category (có thể đếm trùng phiên đa danh mục)")
    return {
        "sessions_with_view":     int(df["sessions_with_view"].sum()),
        "sessions_with_cart":     int(df["sessions_with_cart"].sum()),
        "sessions_with_purchase": int(df["sessions_with_purchase"].sum()),
    }, "funnel_by_category (đếm trùng)"



# =============================================================================
# 1. Doanh thu theo ngày (line chart)
# =============================================================================

def plot_revenue_timeseries() -> None:
    """Line chart: doanh thu theo ngày (kèm số purchase, không cộng dồn phiên)."""
    df = _read_csv_dir("revenue_by_day")
    df["event_date"] = pd.to_datetime(df["event_date"])
    df = df.sort_values("event_date")

    fig, axes = plt.subplots(2, 1, figsize=(13, 7), sharex=True)

    # Subplot 1: Doanh thu
    axes[0].plot(df["event_date"], df["revenue"] / 1e6,
                 color="steelblue", linewidth=1.5, label="Revenue")
    axes[0].fill_between(df["event_date"], df["revenue"] / 1e6, alpha=0.15, color="steelblue")
    axes[0].set_ylabel("Doanh thu (triệu USD)")
    axes[0].set_title("Doanh thu và số lượt purchase theo ngày")
    axes[0].yaxis.set_major_formatter(mticker.FormatStrFormatter("$%.1fM"))
    axes[0].grid(True, linestyle="--", alpha=0.35)

    # Subplot 2: Số lượt purchase.
    # Không dùng cột `sessions` ở đây: agg_daily có grain (event_date ×
    # category_l1), nên cộng `sessions` theo ngày sẽ đếm trùng một phiên
    # chạm nhiều danh mục (tier S: 941.133 thay vì 921.388 phiên duy nhất).
    axes[1].bar(df["event_date"], df["purchases"],
                color="coral", alpha=0.75, label="Purchases")
    axes[1].set_ylabel("Lượt purchase")
    axes[1].set_xlabel("Ngày")
    axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
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

    # Nhãn theo GIÁ TRỊ day_of_week (iso: 1=T2 … 7=CN), không theo vị trí
    # index — nếu một thứ không có dữ liệu, map theo vị trí sẽ gán sai nhãn.
    DAY_LABELS = {1: "T2", 2: "T3", 3: "T4", 4: "T5", 5: "T6", 6: "T7", 7: "CN"}
    tick_labels = [DAY_LABELS.get(int(d), f"?{d}") for d in pivot.index]

    fig, ax = plt.subplots(figsize=(14, 4))
    im = ax.imshow(pivot.values, aspect="auto", cmap="YlOrRd",
                   interpolation="nearest")
    plt.colorbar(im, ax=ax, label="Số sự kiện")

    ax.set_xticks(range(pivot.shape[1]))
    ax.set_xticklabels([int(c) for c in pivot.columns], fontsize=8)
    ax.set_yticks(range(len(tick_labels)))
    ax.set_yticklabels(tick_labels)
    ax.set_xlabel("Giờ trong ngày")
    ax.set_ylabel("Thứ")
    ax.set_title("Phân phối sự kiện theo thứ và giờ (heatmap)")
    fig.tight_layout()
    _save(fig, "hourly_heatmap.png")


# =============================================================================
# 4. Funnel chart theo danh mục (grouped bar)
# =============================================================================

def plot_funnel_by_category() -> None:
    """Bar: view / cart / purchase cho top 10 danh mục + tỷ lệ so với view."""
    df = _read_csv_dir("funnel_by_category")
    top10 = df.nlargest(10, "sessions_with_view").copy()
    top10 = top10.sort_values("sessions_with_view", ascending=False)

    cats = top10["category_l1"].tolist()
    x = np.arange(len(cats))
    w = 0.27

    fig, ax = plt.subplots(figsize=(13, 5.5))
    ax.bar(x - w, top10["sessions_with_view"],     w, label="View",     color="#4c72b0")
    ax.bar(x,     top10["sessions_with_cart"],     w, label="Cart",     color="#dd8452")
    ax.bar(x + w, top10["sessions_with_purchase"], w, label="Purchase", color="#55a868")

    # Nhãn = số phiên ở bước purchase + tỷ lệ purchase/view của chính danh mục.
    # (Đây là tỷ lệ View→Purchase, KHÔNG phải cart→purchase: ở grain
    #  (phiên, danh mục) một phiên có thể mua mà không qua cart, nên tỷ lệ
    #  cart→purchase có thể > 1 ở một số danh mục.)
    ymax = top10["sessions_with_view"].max()
    for i, row in enumerate(top10.itertuples()):
        rate = (row.sessions_with_purchase / row.sessions_with_view * 100
                if row.sessions_with_view else 0.0)
        ax.text(i + w, row.sessions_with_purchase + ymax * 0.012,
                f"{rate:.2f}%", ha="center", va="bottom", fontsize=7.5, color="#3d7a4d")

    ax.set_xticks(x)
    ax.set_xticklabels(cats, rotation=30, ha="right", fontsize=9)
    ax.set_ylabel("Số phiên (theo danh mục)")
    ax.set_title("Funnel View → Cart → Purchase theo danh mục (Top 10)\n"
                 "Nhãn xanh = tỷ lệ View→Purchase của danh mục", fontsize=11)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.legend()
    ax.grid(True, linestyle="--", alpha=0.35, axis="y")
    fig.tight_layout()
    _save(fig, "funnel_by_category.png")


# =============================================================================
# 5. Funnel waterfall (toàn pipeline)
# =============================================================================

def plot_funnel_waterfall() -> None:
    """Funnel tổng (toàn hệ thống) ở grain phiên — không cộng dồn danh mục."""
    totals, source = _read_funnel_totals()
    steps = funnel_steps_from_totals(totals)

    names  = [s["name"] for s in steps]
    values = [s["count"] for s in steps]
    colors = ["#4c72b0", "#dd8452", "#55a868"]
    total_view = values[0]

    fig, ax = plt.subplots(figsize=(8, 5.5))
    bars = ax.bar(names, values, color=colors, edgecolor="white", width=0.55)

    for bar, step in zip(bars, steps):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + total_view * 0.015,
                f"{step['count']:,}  ({step['share_of_view']:.1%} của View)",
                ha="center", va="bottom", fontsize=9)

    # Tỷ lệ chuyển đổi thực đo (không giả định lồng tuyến tính)
    cart_rate     = steps[1]["count"] / steps[0]["count"] * 100 if steps[0]["count"] else 0.0
    purchase_rate = steps[2]["count"] / steps[0]["count"] * 100 if steps[0]["count"] else 0.0
    ax.annotate(f"View→Cart\n{cart_rate:.2f}%", xy=(0.5, total_view * 0.55),
                ha="center", va="center", fontsize=9, color="crimson", fontweight="bold")
    ax.annotate(f"View→Purchase\n{purchase_rate:.2f}%", xy=(1.5, total_view * 0.45),
                ha="center", va="center", fontsize=9, color="crimson", fontweight="bold")

    ax.set_ylabel("Số phiên (đếm 1 lần/phiên)")
    ax.set_title(
        "Funnel tổng: View → Cart → Purchase\n"
        f"(grain phiên, nguồn: {source})",
        fontsize=11,
    )
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.grid(True, linestyle="--", alpha=0.35, axis="y")
    if steps[2]["count"] > steps[1]["count"]:
        ax.text(0.5, -0.16,
                "Lưu ý: có phiên mua trực tiếp không qua Cart nên Purchase > Cart "
                "là hợp lệ — 3 bước không lồng nhau.",
                transform=ax.transAxes, ha="center", fontsize=8, style="italic", color="0.35")
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

    df = pd.read_csv(score_file)  # k, silhouette, inertia, selected_seed[, stability_spread, selected]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4))

    # Elbow
    ax1.plot(df["k"], df["inertia"], marker="o", color="steelblue")
    ax1.set_xlabel("k (số cụm)")
    ax1.set_ylabel("Inertia (WCSS)")
    ax1.set_title("Elbow Method — chọn k")
    ax1.grid(True, linestyle="--", alpha=0.35)

    # Silhouette.
    # Ưu tiên cột `selected` do run_kmeans ghi (đã tính cả cổng ổn định).
    # Chỉ fallback về argmax(silhouette) khi file cũ chưa có cột đó — argmax
    # thuần có thể chọn nghiệm không tái lập được giữa các seed.
    selected = df[df["selected"] == 1] if "selected" in df.columns else df.iloc[0:0]
    if not selected.empty:
        best_k = int(selected.iloc[0]["k"])
        note = "k được chọn (silhouette + cổng ổn định)"
    else:
        best_k = int(df.loc[df["silhouette"].idxmax(), "k"])
        note = "k=… (silhouette max — chưa có cổng ổn định)"

    ax2.plot(df["k"], df["silhouette"], marker="s", color="darkorange")
    ax2.axvline(best_k, color="crimson", linestyle="--",
                label=f"k={best_k} — {note}")
    ax2.set_xlabel("k (số cụm)")
    ax2.set_ylabel("Silhouette score")
    ax2.set_title("Silhouette Score — chọn k")
    ax2.legend(fontsize=8)
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


def main(targets: list[str] | None = None, tier: str = "S") -> None:
    from src.common.config import get_paths, load_config

    global RESULTS, FIGURES
    RESULTS = Path(get_paths(load_config())["results"])
    figures_root = Path(__file__).resolve().parents[2] / "docs" / "figures"
    FIGURES = figures_root if tier == "S" else figures_root / f"tier_{tier.lower()}"
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
    parser.add_argument("--tier", default="S", choices=["S", "M", "L", "XL"])
    args = parser.parse_args()
    main(args.targets, args.tier)
