"""
src/analytics/kpi.py
Owner: D (Analytics 1)
Reviewer: E

Tính các KPI và phân tích từ gold tables.
Định nghĩa KPI: xem docs/data-dictionary.md và configs/pipeline.yaml.

Tất cả hàm nhận DataFrame Spark và trả về DataFrame hoặc dict.
Không có side-effect (ghi file) — việc ghi do run_analytics.py xử lý.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.logger import get_logger

log = get_logger(__name__)


# =============================================================================
# KPI Summary
# =============================================================================

def compute_kpi_summary(fact_sessions: DataFrame, agg_daily: DataFrame) -> dict:
    """
    Tổng hợp KPI chính.
    Định nghĩa: docs/data-dictionary.md → KPI Definitions
    """
    total_revenue = agg_daily.agg(F.sum("revenue")).collect()[0][0] or 0.0
    total_sessions = fact_sessions.count()
    sessions_with_view = fact_sessions.filter(F.col("view_count") > 0).count()
    converted_sessions = fact_sessions.filter(F.col("converted") == True).count()  # noqa: E712
    sessions_with_cart = fact_sessions.filter(F.col("cart_count") > 0).count()
    cart_abandoned = fact_sessions.filter(
        (F.col("cart_count") > 0) & (F.col("purchase_count") == 0)
    ).count()

    conversion_rate = converted_sessions / sessions_with_view if sessions_with_view > 0 else 0.0
    cart_abandonment = cart_abandoned / sessions_with_cart if sessions_with_cart > 0 else 0.0
    aov = total_revenue / converted_sessions if converted_sessions > 0 else 0.0

    summary = {
        "total_revenue":       round(total_revenue, 2),
        "total_sessions":      total_sessions,
        "sessions_with_view":  sessions_with_view,
        "converted_sessions":  converted_sessions,
        "conversion_rate":     round(conversion_rate, 4),
        "sessions_with_cart":  sessions_with_cart,
        "cart_abandonment":    round(cart_abandonment, 4),
        "aov_proxy":           round(aov, 2),
    }
    log.info(f"KPI summary: {summary}")
    return summary


# =============================================================================
# Time series
# =============================================================================

def revenue_by_day(agg_daily: DataFrame) -> DataFrame:
    """Doanh thu theo ngày."""
    return (
        agg_daily.groupBy("event_date")
                 .agg(
                     F.sum("revenue").alias("revenue"),
                     F.sum("purchases").alias("purchases"),
                     F.sum("sessions").alias("sessions"),
                 )
                 .orderBy("event_date")
    )


def revenue_by_category(agg_daily: DataFrame) -> DataFrame:
    """Doanh thu theo category_l1 (tổng toàn thời gian)."""
    return (
        agg_daily.groupBy("category_l1")
                 .agg(
                     F.sum("revenue").alias("revenue"),
                     F.sum("purchases").alias("purchases"),
                 )
                 .orderBy(F.desc("revenue"))
    )


def hourly_pattern(agg_hourly: DataFrame) -> DataFrame:
    """Heatmap thứ × giờ — trả về DataFrame có thể pivot."""
    return agg_hourly.orderBy("day_of_week", "event_hour")


# =============================================================================
# Funnel
# =============================================================================

def funnel_by_category(agg_funnel: DataFrame) -> DataFrame:
    """
    Thêm tỷ lệ chuyển đổi từng bước.
    view→cart rate, cart→purchase rate, overall conversion.
    """
    return (
        agg_funnel
        .withColumn(
            "view_to_cart_rate",
            F.when(F.col("sessions_with_view") > 0,
                   F.col("sessions_with_cart") / F.col("sessions_with_view"))
             .otherwise(0.0),
        )
        .withColumn(
            "cart_to_purchase_rate",
            F.when(F.col("sessions_with_cart") > 0,
                   F.col("sessions_with_purchase") / F.col("sessions_with_cart"))
             .otherwise(0.0),
        )
        .withColumn(
            "overall_conversion",
            F.when(F.col("sessions_with_view") > 0,
                   F.col("sessions_with_purchase") / F.col("sessions_with_view"))
             .otherwise(0.0),
        )
        .orderBy(F.desc("sessions_with_view"))
    )


# =============================================================================
# DAU (Daily Active Users)
# =============================================================================

def compute_dau(silver_df: DataFrame) -> DataFrame:
    """
    DAU = số user_id khác nhau mỗi ngày.
    So sánh exact COUNT(DISTINCT) với approx_count_distinct.
    """
    return (
        silver_df.groupBy("event_date")
                 .agg(
                     F.countDistinct("user_id").alias("dau_exact"),
                     F.approx_count_distinct("user_id").alias("dau_approx"),
                 )
                 .orderBy("event_date")
    )


# =============================================================================
# Cohort / Retention
# =============================================================================

def compute_cohort_retention(fact_sessions: DataFrame) -> DataFrame:
    """
    Cohort retention theo tuần mua đầu tiên.
    Cần nhiều tháng dữ liệu để có ý nghĩa.

    Trả về DataFrame:
      cohort_week | activity_week | users | retention_rate
    """
    log.info("Tính cohort retention...")

    # Tuần mua đầu tiên của mỗi user
    buyers = fact_sessions.filter(F.col("converted") == True)  # noqa: E712

    first_purchase = (
        buyers.groupBy("user_id")
              .agg(F.min("session_start").alias("first_purchase_ts"))
              .withColumn("cohort_week", F.date_trunc("week", F.col("first_purchase_ts")))
    )

    # Gắn cohort_week vào mọi phiên mua
    sessions_with_cohort = (
        buyers.join(first_purchase.select("user_id", "cohort_week"), on="user_id")
              .withColumn("activity_week", F.date_trunc("week", F.col("session_start")))
              .withColumn(
                  "week_number",
                  (F.datediff(F.col("activity_week"), F.col("cohort_week")) / 7).cast("int"),
              )
    )

    # Đếm users mỗi cohort × tuần
    cohort_size = (
        first_purchase.groupBy("cohort_week")
                      .agg(F.countDistinct("user_id").alias("cohort_size"))
    )

    retention = (
        sessions_with_cohort
        .groupBy("cohort_week", "week_number")
        .agg(F.countDistinct("user_id").alias("active_users"))
        .join(cohort_size, on="cohort_week")
        .withColumn(
            "retention_rate",
            F.col("active_users") / F.col("cohort_size"),
        )
        .orderBy("cohort_week", "week_number")
    )
    return retention
