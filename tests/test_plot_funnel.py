"""
tests/test_plot_funnel.py
Owner: M4 (Analytics KPI/Funnel/Cohort)

Test phần tổng hợp funnel dùng để vẽ biểu đồ trong src/analytics/plots.py.

Bài học từ lỗi thực tế (tier S): biểu đồ funnel tổng từng cộng dồn
`funnel_by_category` — bảng có grain (user_session, category_l1). Một phiên
chạm nhiều danh mục bị đếm nhiều lần, cho ra 905.652 phiên có view trong khi
`kpi_summary.csv` ghi 886.657. Các test dưới đây khoá lại hành vi đúng:
funnel tổng phải lấy từ số liệu grain-phiên, không cộng dồn theo danh mục.

Test thuần pandas (không cần Spark) để chạy được cả ngoài container.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.analytics import plots  # noqa: E402

# =============================================================================
# funnel_steps_from_totals
# =============================================================================

def test_funnel_steps_share_uses_view_as_base():
    steps = plots.funnel_steps_from_totals({
        "sessions_with_view": 886657,
        "sessions_with_cart": 22374,
        "sessions_with_purchase": 18361,
    })

    assert [s["name"] for s in steps] == ["View", "Cart", "Purchase"]
    assert [s["count"] for s in steps] == [886657, 22374, 18361]
    assert steps[0]["share_of_view"] == 1.0
    assert steps[1]["share_of_view"] == pytest.approx(22374 / 886657)
    assert steps[2]["share_of_view"] == pytest.approx(18361 / 886657)


def test_funnel_steps_allows_purchase_greater_than_cart():
    """
    Ba bước funnel KHÔNG lồng nhau: có phiên mua trực tiếp không qua cart.
    (Tier M, danh mục 'unknown': cart_to_purchase = 2.19.)
    Hàm vẽ không được giả định purchase <= cart.
    """
    steps = plots.funnel_steps_from_totals({
        "sessions_with_view": 9243001,
        "sessions_with_cart": 573099,
        "sessions_with_purchase": 629560,
    })
    assert steps[2]["count"] > steps[1]["count"]
    assert steps[2]["share_of_view"] == pytest.approx(629560 / 9243001)


def test_funnel_steps_handles_zero_view():
    steps = plots.funnel_steps_from_totals({
        "sessions_with_view": 0,
        "sessions_with_cart": 0,
        "sessions_with_purchase": 0,
    })
    assert all(s["share_of_view"] == 0.0 for s in steps)


# =============================================================================
# _read_funnel_totals — thứ tự ưu tiên nguồn
# =============================================================================

@pytest.fixture
def temp_results(tmp_path, monkeypatch):
    """Trỏ plots.RESULTS sang thư mục tạm."""
    monkeypatch.setattr(plots, "RESULTS", tmp_path)
    return tmp_path


def _write_dir_csv(base: Path, name: str, df: pd.DataFrame) -> None:
    folder = base / name
    folder.mkdir(parents=True, exist_ok=True)
    df.to_csv(folder / "part-00000-test-c000.csv", index=False)


def test_prefers_funnel_totals_csv(temp_results):
    """Có funnel_totals.csv thì dùng nó, KHÔNG cộng dồn theo danh mục."""
    pd.DataFrame([{
        "total_sessions": 921388,
        "sessions_with_view": 886657,
        "sessions_with_cart": 22374,
        "sessions_with_purchase": 18361,
    }]).to_csv(temp_results / "funnel_totals.csv", index=False)

    # Bảng theo danh mục cố tình lớn hơn (đếm trùng) để bắt lỗi nếu bị dùng
    _write_dir_csv(temp_results, "funnel_by_category", pd.DataFrame([
        {"category_l1": "electronics", "sessions_with_view": 905652,
         "sessions_with_cart": 22378, "sessions_with_purchase": 18378},
    ]))

    totals, source = plots._read_funnel_totals()
    assert source == "funnel_totals.csv"
    assert totals["sessions_with_view"] == 886657
    assert totals["sessions_with_purchase"] == 18361


def test_falls_back_to_kpi_summary(temp_results):
    pd.DataFrame([{
        "total_revenue": 5813952.29,
        "total_sessions": 921388,
        "sessions_with_view": 886657,
        "converted_sessions": 18361,
        "conversion_rate": 0.0207,
        "sessions_with_cart": 22374,
        "cart_abandonment": 0.9812,
        "aov_proxy": 316.65,
    }]).to_csv(temp_results / "kpi_summary.csv", index=False)

    totals, source = plots._read_funnel_totals()
    assert source == "kpi_summary.csv"
    assert totals["sessions_with_view"] == 886657
    # converted_sessions đóng vai sessions_with_purchase
    assert totals["sessions_with_purchase"] == 18361
    assert totals["sessions_with_cart"] == 22374


def test_last_resort_uses_category_sum(temp_results, capsys):
    _write_dir_csv(temp_results, "funnel_by_category", pd.DataFrame([
        {"category_l1": "a", "sessions_with_view": 10,
         "sessions_with_cart": 4, "sessions_with_purchase": 2},
        {"category_l1": "b", "sessions_with_view": 5,
         "sessions_with_cart": 3, "sessions_with_purchase": 1},
    ]))

    totals, source = plots._read_funnel_totals()
    assert "đếm trùng" in source
    assert totals["sessions_with_view"] == 15
    assert "đếm trùng" in capsys.readouterr().out
