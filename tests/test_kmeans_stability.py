"""
tests/test_kmeans_stability.py
Owner: M5 (module rfm_kmeans) — bổ sung bởi M4 trong đợt review.

Test phần LOGIC THUẦN của cổng ổn định dùng khi chọn k cho KMeans.
Import từ `src.analytics.kmeans_selection` (không phụ thuộc Spark) nên chạy
được cả ngoài container, kể cả trên máy không có JVM.

Bối cảnh: `run_kmeans` trước đây chọn k theo `argmax(silhouette)`. Trên tier S,
cách đó chọn k=2 với silhouette 0,8359 — nhưng cụm chi tiêu cao chứa
904 / 7.411 / 7.447 người tùy seed (lệch 124,5%), tức không tái lập được.
k=4 có silhouette thấp hơn (0,5498) nhưng lệch tối đa 1,7%.

Các test dưới đây khoá lại định nghĩa "spread" và hành vi của cổng chọn k.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.analytics.kmeans_selection import (  # noqa: E402
    DEFAULT_STABILITY_TOLERANCE,
    select_k,
    stability_spread,
)

TOLERANCE = DEFAULT_STABILITY_TOLERANCE


# =============================================================================
# stability_spread
# =============================================================================

def test_spread_zero_when_all_seeds_agree():
    """Ba seed cho cùng kích thước cụm → spread = 0."""
    assert stability_spread([[904, 5000], [904, 5000], [904, 5000]]) == 0.0


def test_spread_empty_input():
    assert stability_spread([]) == 0.0


def test_spread_single_seed_is_zero():
    """Một seed thì không có gì để so sánh."""
    assert stability_spread([[904, 5000]]) == 0.0


def test_spread_formula_max_min_over_mean():
    """(max - min) / mean cho từng hạng cụm, lấy giá trị lớn nhất."""
    # hạng 0: [10, 20] → (20-10)/15 = 0.6667
    # hạng 1: [100, 110] → (110-100)/105 = 0.0952
    spread = stability_spread([[10, 100], [20, 110]])
    assert abs(spread - (20 - 10) / 15) < 1e-9


def test_spread_detects_tier_s_k2_instability():
    """
    Dữ liệu thật tier S, k=2, ghép theo thứ hạng monetary:
      hạng 1 (chi tiêu cao): 904 / 7411 / 7447
      hạng 2:                16342 / 9835 / 9799
    Spread lớn nhất 124,5% → phải vượt ngưỡng 5%.
    """
    sizes = [
        [904, 16342],    # seed 0
        [7411, 9835],    # seed 42
        [7447, 9799],    # seed 123
    ]
    spread = stability_spread(sizes)
    assert abs(spread - 1.245) < 0.005, f"spread={spread}"
    assert spread > TOLERANCE


def test_spread_accepts_tier_s_k4_stability():
    """
    Dữ liệu thật tier S, k=4:
      904/904/904, 6967/6939/6859, 5348/5332/5371, 4055/4043/4112
    Spread lớn nhất 1,7% → phải nằm trong ngưỡng 5%.
    """
    sizes = [
        [904, 6967, 5348, 4055],   # seed 42
        [904, 6939, 5332, 4043],   # seed 0
        [904, 6859, 5371, 4112],   # seed 123
    ]
    spread = stability_spread(sizes)
    assert spread < TOLERANCE, f"spread={spread}"
    assert abs(spread - 0.017) < 0.005, f"spread={spread}"


def test_spread_accepts_tier_m_k2_stability():
    """Tier M, k=2: 79753/79216/80060 và 267365/267902/267058 → ~1,1%."""
    sizes = [
        [79753, 267365],
        [79216, 267902],
        [80060, 267058],
    ]
    spread = stability_spread(sizes)
    assert spread < TOLERANCE, f"spread={spread}"


# =============================================================================
# select_k — cổng chọn k
# =============================================================================

def test_gate_rejects_unstable_high_silhouette():
    """k=2 silhouette cao nhất nhưng bất ổn → cổng phải chọn k=4."""
    rows = [
        {"k": 2, "silhouette": 0.835945, "stability_spread": 1.245},
        {"k": 3, "silhouette": 0.525433, "stability_spread": 0.080},
        {"k": 4, "silhouette": 0.549776, "stability_spread": 0.017},
        {"k": 5, "silhouette": 0.514381, "stability_spread": 0.020},
    ]
    row, excluded = select_k(rows)
    assert row["k"] == 4
    assert 2 in excluded and 3 in excluded


def test_gate_keeps_stable_winner():
    """Khi k cao nhất silhouette cũng ổn định thì không đổi lựa chọn."""
    rows = [
        {"k": 2, "silhouette": 0.580124, "stability_spread": 0.011},
        {"k": 3, "silhouette": 0.491613, "stability_spread": 0.010},
    ]
    row, excluded = select_k(rows)
    assert row["k"] == 2
    assert excluded == []


def test_gate_falls_back_when_nothing_stable():
    """Không k nào đạt ngưỡng → quay về argmax(silhouette), không crash."""
    rows = [
        {"k": 2, "silhouette": 0.40, "stability_spread": 0.90},
        {"k": 3, "silhouette": 0.55, "stability_spread": 0.60},
    ]
    row, excluded = select_k(rows)
    assert row["k"] == 3
    assert excluded == [2, 3]


def test_gate_empty_raises():
    import pytest

    with pytest.raises(ValueError):
        select_k([])
