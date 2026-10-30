"""
src/processing/bronze_to_silver.py
Owner: M2 — luật R1–R9 + DQ report
Reviewer: M1

Bronze → Silver: làm sạch, chuẩn hóa, dedupe, phân loại quarantine.
Ghi DQ report ra results/dq_report_silver.csv (+ bản .md để dán vào báo cáo).

Bất biến bắt buộc:  count(bronze) == count(silver) + count(quarantine)
  → mọi dòng bị loại (kể cả dòng trùng ở R7) đều phải nằm trong quarantine.

Chạy:
    spark-submit src/processing/bronze_to_silver.py --tier M
    spark-submit src/processing/bronze_to_silver.py --tier M --timed   # đo thời gian từng luật
"""
from __future__ import annotations

import argparse
import csv
import time
from functools import reduce
from pathlib import Path
from typing import Callable

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from src.common.config import get_paths, load_config
from src.common.input_manifest import write_silver_manifest
from src.common.logger import get_logger
from src.common.spark_session import get_spark

log = get_logger(__name__)

# Tập event_type hợp lệ — xác nhận lại bằng profiling trước khi dùng
VALID_EVENT_TYPES = {"view", "cart", "purchase", "remove_from_cart"}

# Định dạng event_time của REES46: "2019-10-01 00:00:00 UTC"
TS_FORMAT_WITH_ZONE = "yyyy-MM-dd HH:mm:ss z"
TS_FORMAT_NO_ZONE = "yyyy-MM-dd HH:mm:ss"

# Cột nghiệp vụ dùng để xác định "dòng trùng hoàn toàn" (R7).
# Không dùng source_file / ingest_ts vì đó là metadata của lần ingest.
DEDUPE_KEYS = [
    "event_time", "event_type", "product_id", "category_id",
    "category_code", "brand", "price", "user_id", "user_session",
]

# Thứ tự cột của silver (cột nào không tồn tại sẽ bị bỏ qua)
SILVER_COLUMNS = [
    "event_ts", "event_date", "event_hour",
    "event_type", "product_id", "category_id",
    "category_code", "category_l1", "category_l2", "category_l3", "category_missing",
    "brand", "brand_missing",
    "price", "price_valid",
    "user_id", "user_session", "session_id", "session_generated",
    "out_of_month",
    "source_file", "source_month", "ingest_ts",
]

# Metadata các luật — dùng cho DQ report
# (rule, tên, hành động, loại: "reject" = quarantine, "flag" = chỉ gắn cờ)
RULES = [
    ("R1", "null_key_field",        "quarantine",                                  "reject"),
    ("R2", "invalid_event_type",    "quarantine",                                  "reject"),
    ("R3", "timestamp_parse_error", "quarantine",                                  "reject"),
    ("R4", "negative_price",        "quarantine (<0); flag price_valid=false (=0/null)", "reject+flag"),
    ("R5", "brand_missing",         "fill 'unknown' + flag",                       "flag"),
    ("R6", "category_missing",      "category_l1='unknown' + flag",                "flag"),
    ("R7", "duplicate_row",         "quarantine (dedupe, giữ 1 bản)",              "reject"),
    ("R8", "out_of_month",          "flag",                                        "flag"),
    ("R9", "session_generated",     "sinh session_id bằng gap",                    "flag"),
]

# Cột cờ trên silver tương ứng với từng luật (để đếm "flagged")
FLAG_EXPR = {
    "R4": lambda: ~F.col("price_valid"),
    "R5": lambda: F.col("brand_missing"),
    "R6": lambda: F.col("category_missing"),
    "R8": lambda: F.col("out_of_month"),
    "R9": lambda: F.col("session_generated"),
}


# =============================================================================
# Các luật làm sạch (R1–R9)
# Mỗi luật trả về (df_clean, df_quarantine_slice | None)
# Quarantine slice giữ NGUYÊN các cột bronze + reject_reason.
# =============================================================================

def apply_r1_null_keys(df: DataFrame):
    """R1: event_time, event_type, product_id, user_id không null."""
    mask = (
        F.col("event_time").isNull() |
        F.col("event_type").isNull() |
        F.col("product_id").isNull() |
        F.col("user_id").isNull()
    )
    return (
        df.filter(~mask),
        df.filter(mask).withColumn("reject_reason", F.lit("null_key_field")),
    )


def apply_r2_event_type(df: DataFrame, valid_types: set[str]):
    """R2: event_type (sau khi trim + lower) thuộc tập hợp lệ.

    Dòng sạch được chuẩn hóa event_type về chữ thường.
    Dòng quarantine giữ giá trị gốc để còn điều tra.
    """
    norm = F.lower(F.trim(F.col("event_type")))
    # coalesce(..., True): event_type null (nếu luật được gọi riêng lẻ) → coi là không hợp lệ
    mask = F.coalesce(~norm.isin(list(valid_types)), F.lit(True))
    return (
        df.filter(~mask).withColumn("event_type", norm),
        df.filter(mask).withColumn("reject_reason", F.lit("invalid_event_type")),
    )


def parse_event_time(col: F.Column) -> F.Column:
    """Parse event_time an toàn (ANSI mode): lỗi → null, không làm chết job.

    Thử định dạng có múi giờ trước ("... UTC"), sau đó không múi giờ.
    LƯU Ý: PySpark 3.5 yêu cầu format là Column → phải bọc F.lit().
    """
    raw = F.trim(col)
    return F.coalesce(
        F.try_to_timestamp(raw, F.lit(TS_FORMAT_WITH_ZONE)),
        F.try_to_timestamp(raw, F.lit(TS_FORMAT_NO_ZONE)),
    ).cast(TimestampType())


def apply_r3_parse_timestamp(df: DataFrame):
    """R3: Parse event_time → event_ts (UTC). Lỗi parse → quarantine.

    Giữ lại event_time gốc (bỏ ở bước cuối) để quarantine của các luật sau
    vẫn có đủ cột bronze.
    """
    df = df.withColumn("event_ts", parse_event_time(F.col("event_time")))
    mask = F.col("event_ts").isNull()
    return (
        df.filter(~mask),
        df.filter(mask).withColumn("reject_reason", F.lit("timestamp_parse_error")),
    )


def apply_r4_price(df: DataFrame):
    """R4: price < 0 → quarantine; price == 0 hoặc null → giữ nhưng price_valid=false.

    Chú ý null: (price < 0) với price null cho kết quả null → nếu viết
    filter(price < 0) / filter(~(price < 0)) thì dòng null bị loại ở CẢ HAI
    nhánh và "biến mất" → vỡ bất biến. Vì vậy mask phải luôn true/false.
    """
    mask = F.col("price").isNotNull() & (F.col("price") < 0)
    quarantine = df.filter(mask).withColumn("reject_reason", F.lit("negative_price"))
    clean = df.filter(~mask)
    clean = clean.withColumn("price_valid", F.coalesce(F.col("price") > 0, F.lit(False)))
    return clean, quarantine


def apply_r5_brand(df: DataFrame):
    """R5: brand null/rỗng → 'unknown', gắn cờ brand_missing."""
    missing = F.col("brand").isNull() | (F.trim(F.col("brand")) == "")
    df = df.withColumn("brand_missing", missing)
    df = df.withColumn(
        "brand",
        F.when(missing, F.lit("unknown")).otherwise(F.lower(F.trim(F.col("brand")))),
    )
    return df, None


def apply_r6_category(df: DataFrame):
    """R6: Tách category_code thành l1/l2/l3; thiếu → l1='unknown'.

    Dùng F.get (trả null khi vượt biên) thay cho getItem: với ANSI mode,
    getItem(2) trên mảng 2 phần tử sẽ ném INVALID_ARRAY_INDEX → chết job.
    """
    missing = F.col("category_code").isNull() | (F.trim(F.col("category_code")) == "")
    parts = F.split(F.trim(F.col("category_code")), r"\.")
    df = df.withColumn("category_missing", missing)
    df = df.withColumn(
        "category_l1",
        F.when(missing, F.lit("unknown")).otherwise(F.get(parts, 0)),
    )
    df = df.withColumn("category_l2", F.when(missing, F.lit(None)).otherwise(F.get(parts, 1)))
    df = df.withColumn("category_l3", F.when(missing, F.lit(None)).otherwise(F.get(parts, 2)))
    return df, None


def apply_r7_dedupe(df: DataFrame, persist: bool = False):
    """R7: Dòng trùng hoàn toàn (theo DEDUPE_KEYS) → giữ 1 bản, phần dư vào quarantine.

    Lý do đưa vào quarantine thay vì dropDuplicates(): giữ bất biến
    bronze = silver + quarantine và đếm được số dòng trùng cho DQ report.
    """
    keys = [c for c in DEDUPE_KEYS if c in df.columns]
    order_cols = [c for c in ("ingest_ts", "source_file") if c in df.columns]
    order = [F.col(c) for c in order_cols] or [F.lit(1)]
    w = Window.partitionBy(*keys).orderBy(*order)

    ranked = df.withColumn("_dup_rn", F.row_number().over(w))
    if persist:
        ranked = ranked.persist(StorageLevel.MEMORY_AND_DISK)

    clean = ranked.filter(F.col("_dup_rn") == 1).drop("_dup_rn")
    quarantine = (
        ranked.filter(F.col("_dup_rn") > 1)
        .drop("_dup_rn")
        .withColumn("reject_reason", F.lit("duplicate_row"))
    )
    return clean, quarantine


def apply_r8_out_of_month(df: DataFrame):
    """R8: Sự kiện ngoài tháng của file → gắn cờ (không quarantine).

    source_month rỗng/null (không trích được từ tên file) → không gắn cờ.
    """
    has_month = F.col("source_month").isNotNull() & (F.col("source_month") != "")
    df = df.withColumn(
        "out_of_month",
        F.when(
            has_month,
            F.date_format(F.col("event_ts"), "yyyy-MM") != F.col("source_month"),
        ).otherwise(F.lit(False)),
    )
    return df, None


def apply_r9_session(df: DataFrame, gap_minutes: int = 30):
    """R9: Đảm bảo mọi dòng có session_id.

    - user_session có giá trị → session_id = user_session, session_generated=false
    - user_session null/rỗng → sinh session theo khoảng nghỉ > gap_minutes
      giữa 2 sự kiện liên tiếp của cùng user:  session_id = 'gen_<user_id>_<n>'

    Window chỉ chạy trên phần dòng thiếu session (rất ít) → rẻ.
    """
    missing = F.col("user_session").isNull() | (F.trim(F.col("user_session")) == "")

    has_session = (
        df.filter(~missing)
        .withColumn("session_id", F.col("user_session"))
        .withColumn("session_generated", F.lit(False))
    )

    w = Window.partitionBy("user_id").orderBy("event_ts")
    gap_s = gap_minutes * 60
    prev_ts = F.lag("event_ts").over(w)
    no_session = (
        df.filter(missing)
        .withColumn("_prev_ts", prev_ts)
        .withColumn(
            "_new_session",
            F.when(
                F.col("_prev_ts").isNull()
                | ((F.unix_timestamp("event_ts") - F.unix_timestamp("_prev_ts")) > gap_s),
                1,
            ).otherwise(0),
        )
        .withColumn("_session_seq", F.sum("_new_session").over(w))
        .withColumn(
            "session_id",
            F.concat(
                F.lit("gen_"), F.col("user_id").cast("string"),
                F.lit("_"), F.col("_session_seq").cast("string"),
            ),
        )
        .withColumn("session_generated", F.lit(True))
        .drop("_prev_ts", "_new_session", "_session_seq")
    )
    return has_session.unionByName(no_session), None


# =============================================================================
# Ghép pipeline
# =============================================================================

def enrich_columns(df: DataFrame) -> DataFrame:
    """Thêm các cột dẫn xuất cần thiết (theo session timezone = UTC)."""
    df = df.withColumn("event_date", F.to_date(F.col("event_ts")))
    df = df.withColumn("event_hour", F.hour(F.col("event_ts")))
    return df


def finalize_silver(df: DataFrame) -> DataFrame:
    """Bỏ event_time gốc, sắp xếp cột theo SILVER_COLUMNS."""
    cols = [c for c in SILVER_COLUMNS if c in df.columns]
    return df.select(*cols)


def build_quarantine(parts: list[DataFrame], bronze_cols: list[str]) -> DataFrame:
    """Chuẩn hóa mọi lát quarantine về cùng schema: cột bronze + reject_rule + reject_reason.

    Dùng unionByName (KHÔNG dùng union theo vị trí — các lát có thứ tự/số cột khác nhau).
    """
    cols = list(bronze_cols) + ["reject_rule", "reject_reason"]
    normalized = [p.select(*cols) for p in parts]
    return reduce(lambda a, b: a.unionByName(b), normalized)


def clean_pipeline(
    bronze: DataFrame,
    valid_types: set[str] | None = None,
    gap_minutes: int = 30,
    timed: bool = False,
    persist_dedupe: bool = False,
) -> tuple[DataFrame, DataFrame, dict[str, float]]:
    """Chạy R1→R9 trên bronze. Trả về (silver_df, quarantine_df, timings).

    timed=True: persist + count sau mỗi luật để đo thời gian thực của từng luật
    (tốn thêm 1 lượt quét mỗi luật — chỉ dùng khi cần số liệu cho báo cáo).
    """
    valid_types = valid_types or VALID_EVENT_TYPES
    bronze_cols = bronze.columns

    steps: list[tuple[str, Callable[[DataFrame], tuple]]] = [
        ("R1", apply_r1_null_keys),
        ("R2", lambda d: apply_r2_event_type(d, valid_types)),
        ("R3", apply_r3_parse_timestamp),
        ("R4", apply_r4_price),
        ("R5", apply_r5_brand),
        ("R6", apply_r6_category),
        ("R7", lambda d: apply_r7_dedupe(d, persist=persist_dedupe or timed)),
        ("R8", apply_r8_out_of_month),
        ("R9", lambda d: apply_r9_session(d, gap_minutes)),
    ]

    df = bronze
    q_parts: list[DataFrame] = []
    timings: dict[str, float] = {}

    for rule, fn in steps:
        start = time.perf_counter()
        df, q = fn(df)
        if q is not None:
            q_parts.append(q.withColumn("reject_rule", F.lit(rule)))
        if timed:
            df = df.persist(StorageLevel.MEMORY_AND_DISK)
            n = df.count()
            timings[rule] = round(time.perf_counter() - start, 3)
            log.info(f"  {rule}: {n:,} dòng còn lại | {timings[rule]}s")

    silver = finalize_silver(enrich_columns(df))
    quarantine = build_quarantine(q_parts, bronze_cols)
    return silver, quarantine, timings


# =============================================================================
# DQ report
# =============================================================================

def build_dq_report(
    total_in: int,
    reject_counts: dict[str, int],
    flag_counts: dict[str, int],
    timings: dict[str, float] | None = None,
) -> list[dict]:
    """Tạo bảng DQ (thuần Python — dễ test).

    rows_in/rows_out đi theo dây chuyền: rows_out(Rk) = rows_in(Rk) - rejected(Rk).
    reject_rate_pct tính trên tổng bronze; flag_rate_pct tính trên tổng silver.
    """
    timings = timings or {}
    total_rejected = sum(reject_counts.values())
    total_out = total_in - total_rejected

    rows: list[dict] = []
    current = total_in
    for rule, name, action, _kind in RULES:
        rejected = int(reject_counts.get(rule, 0))
        flagged = int(flag_counts.get(rule, 0))
        rows.append({
            "rule": rule,
            "name": name,
            "action": action,
            "rows_in": current,
            "rows_out": current - rejected,
            "rejected": rejected,
            "reject_rate_pct": round(rejected / total_in * 100, 4) if total_in else 0.0,
            "flagged": flagged,
            "flag_rate_pct": round(flagged / total_out * 100, 4) if total_out else 0.0,
            "time_s": timings.get(rule, ""),
        })
        current -= rejected

    rows.append({
        "rule": "TOTAL",
        "name": "",
        "action": "",
        "rows_in": total_in,
        "rows_out": total_out,
        "rejected": total_rejected,
        "reject_rate_pct": round(total_rejected / total_in * 100, 4) if total_in else 0.0,
        "flagged": "",
        "flag_rate_pct": "",
        "time_s": round(sum(timings.values()), 3) if timings else "",
    })
    return rows


def write_dq_report(rows: list[dict], results_dir: str | Path, tier: str) -> None:
    out_dir = Path(results_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / "dq_report_silver.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    md_path = out_dir / "dq_report_silver.md"
    lines = [
        f"# DQ report — Silver (tier {tier})",
        "",
        "| Luật | Tên | Hành động | Dòng vào | Dòng ra | Bị loại | Tỷ lệ loại | Gắn cờ | Tỷ lệ cờ | Thời gian |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]

    def _fmt_int(v):
        return f"{v:,}" if isinstance(v, int) else str(v)

    def _fmt_pct(v):
        return f"{v:.4f}%" if isinstance(v, float) else str(v)

    def _fmt_time(v):
        return f"{v}s" if v != "" else "—"

    for r in rows:
        bold = r["rule"] == "TOTAL"
        cells = [
            r["rule"], r["name"], r["action"],
            _fmt_int(r["rows_in"]), _fmt_int(r["rows_out"]), _fmt_int(r["rejected"]),
            _fmt_pct(r["reject_rate_pct"]), _fmt_int(r["flagged"]),
            _fmt_pct(r["flag_rate_pct"]), _fmt_time(r["time_s"]),
        ]
        if bold:
            cells = [f"**{c}**" if c not in ("", "—") else c for c in cells]
        lines.append("| " + " | ".join(cells) + " |")

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log.info(f"DQ report → {csv_path} , {md_path}")


def count_flags(silver: DataFrame) -> dict[str, int]:
    """Đếm số dòng gắn cờ cho từng luật trong 1 lượt agg."""
    exprs = [
        F.sum(F.when(expr(), 1).otherwise(0)).alias(rule)
        for rule, expr in FLAG_EXPR.items()
    ]
    row = silver.agg(*exprs).collect()[0]
    return {rule: int(row[rule] or 0) for rule in FLAG_EXPR}


def count_rejects(quarantine: DataFrame) -> dict[str, int]:
    return {
        r["reject_rule"]: int(r["count"])
        for r in quarantine.groupBy("reject_rule").count().collect()
    }


# =============================================================================
# Main
# =============================================================================

def run(tier: str, timed: bool = False) -> None:
    cfg = load_config()
    paths = get_paths(cfg)
    cleaning_cfg = cfg.get("cleaning", {})
    valid_types = set(cleaning_cfg.get("valid_event_types", list(VALID_EVENT_TYPES)))
    gap_min = int(cleaning_cfg.get("session_gap_minutes", 30))

    spark = get_spark(f"silver_{tier}")
    # event_date / event_hour / out_of_month phải tính theo UTC như dữ liệu gốc
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    t0 = time.perf_counter()
    bronze = spark.read.parquet(paths["bronze"])
    total_in = bronze.count()
    log.info(f"Bronze in: {total_in:,} | tier={tier} | timed={timed}")

    silver, quarantine, timings = clean_pipeline(
        bronze, valid_types, gap_min, timed=timed, persist_dedupe=True,
    )

    # Ghi silver: repartition theo event_date → ~1 file/ngày, tránh hàng nghìn file nhỏ
    log.info(f"Ghi silver → {paths['silver']}")
    (
        silver.repartition("event_date")
        .write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(paths["silver"])
    )

    log.info(f"Ghi quarantine → {paths['quarantine']}")
    (
        quarantine.repartition("reject_rule")
        .write.mode("overwrite")
        .partitionBy("reject_rule")
        .parquet(paths["quarantine"])
    )
    spark.catalog.clearCache()

    # Đọc lại từ đĩa để kiểm tra — đếm trên dữ liệu ĐÃ GHI, không phải kế hoạch lazy
    silver_w = spark.read.parquet(paths["silver"])
    quarantine_w = spark.read.parquet(paths["quarantine"])
    total_out = silver_w.count()
    reject_counts = count_rejects(quarantine_w)
    total_q = sum(reject_counts.values())
    flag_counts = count_flags(silver_w)

    rows = build_dq_report(total_in, reject_counts, flag_counts, timings)
    write_dq_report(rows, paths.get("results", "/app/results"), tier)

    elapsed = round(time.perf_counter() - t0, 3)
    ok = total_in == total_out + total_q
    log.info(
        f"Silver out: {total_out:,} | Quarantine: {total_q:,} | "
        f"Bất biến: {total_in:,} == {total_out:,} + {total_q:,} → {'OK' if ok else 'LỖI'} | "
        f"{elapsed}s"
    )
    for rule, n in sorted(reject_counts.items()):
        log.info(f"  quarantine {rule}: {n:,}")

    try:
        from src.common.metrics import RunRecord, write_record
        write_record(RunRecord(
            run_id=f"PIPE_silver_{tier}_{int(time.time())}",
            experiment="PIPE_silver",
            engine="spark",
            engine_version=spark.version,
            tier=tier,
            wall_time_s=elapsed,
            row_count=total_out,
            success=ok,
            notes=f"in={total_in};quarantine={total_q}",
        ))
    except Exception as exc:  # metrics không được làm hỏng pipeline
        log.warning(f"Không ghi được experiment_log: {exc}")

    try:
        write_silver_manifest(paths["silver"], tier)
    except Exception as exc:
        log.warning(f"Không ghi được silver manifest: {exc}")
    spark.stop()
    if not ok:
        raise RuntimeError(f"Bất biến lỗi: {total_in} ≠ {total_out} + {total_q}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze → Silver (R1–R9)")
    parser.add_argument("--tier", default="S", choices=["S", "M", "L", "XL"])
    parser.add_argument("--timed", action="store_true",
                        help="Đo thời gian từng luật (persist + count sau mỗi luật)")
    args = parser.parse_args()
    run(args.tier, timed=args.timed)
