# bigdata-ecom — E-commerce Behavior Analytics trên Apache Spark

> **Môn**: Nhập môn dữ liệu lớn (841450) — Trường Đại học Sài Gòn  
> **Nhóm**: 5 người | **Phạm vi**: Processing → Analytics  
> **Dataset**: REES46 "eCommerce behavior data from multi category store" ([Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data))  
> **Stack**: Apache Spark 3.5 · PySpark · Parquet · Docker Compose · DuckDB · Pandas · Matplotlib

---

## Thành viên

| Mã | Tên | Module chính | Thí nghiệm | Mục báo cáo |
|----|-----|-------------|------------|-------------|
| M1 | Hiếu | Hạ tầng + Docker + Bronze | E2, E7 | Chương 1, 4, Phụ lục |
| M2 | *(điền tên)* | Silver — làm sạch R1–R9, DQ report | E3 | Chương 3, 5 (DQ) |
| M3 | *(điền tên)* | Gold — sessionization, dim/fact | E1, E4 | Chương 5 (proc), 6 (perf) |
| M4 | *(điền tên)* | Analytics — KPI, Funnel, Cohort | Phân tích, biểu đồ | Chương 7 (demo) |
| M5 | *(điền tên)* | Analytics — RFM, KMeans, Baseline | E5, E6 | Chương 2 (lý thuyết), 6 |

📖 **Hướng dẫn chi tiết từng người**: [`docs/phan-cong-nhom.md`](docs/phan-cong-nhom.md)

---

## Kiến trúc pipeline

```
data/raw/*.csv
    │  make bronze TIER=S/M/L/XL
    ▼
data/lake/bronze/          ← Parquet, partition: source_month
    │  make silver
    ▼
data/lake/silver/          ← Parquet, partition: event_date  (đã làm sạch)
data/lake/quarantine/      ← dòng bị loại + reject_reason
    │  make gold
    ▼
data/lake/gold/            ← fact_sessions, dim_product, dim_user,
    │                         agg_daily, agg_hourly, agg_funnel
    │  make analytics
    ▼
results/                   ← kpi_summary.csv, funnel, cluster_profiles...
docs/figures/              ← PNG biểu đồ (make plots)
```

**Bất biến**: `count(bronze) = count(silver) + count(quarantine)`

---

## Yêu cầu môi trường

| Thứ | Phiên bản |
|-----|-----------|
| Docker Desktop / Engine | 24.x+ |
| Docker Compose | v2.x |
| Python (ngoài container) | 3.10 hoặc 3.11 |
| RAM cấp cho Docker | ≥ 6 GB (khuyến nghị 8 GB) |
| Ổ đĩa trống | ≥ 30 GB |

> **Windows**: bật WSL2, mở Docker Desktop → Settings → Resources → Memory ≥ 6 GB.

---

## Bắt đầu nhanh (Quick Start)

### Bước 1 — Clone và cài đặt

```bash
git clone <URL_REPO> bigdata-ecom
cd bigdata-ecom
cp docker/.env.example docker/.env
pip install -r requirements.txt   # chỉ cần cho E5/E6/plots chạy ngoài container
```

### Bước 2 — Khởi động Spark cluster

```bash
make up
# Mở trình duyệt:
#   http://localhost:8080   → Spark Master UI (phải thấy 2 Workers)
#   http://localhost:18080  → History Server
```

### Bước 3 — Tải dataset

```
Tải từ: https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data
Đặt vào: data/raw/2019-Oct.csv   (và 2019-Nov.csv nếu ổ đĩa đủ)
Thư mục data/raw/ bị .gitignore — không lo commit nhầm.
```

### Bước 4 — Chạy pipeline

```bash
# Toàn bộ pipeline bằng 1 lệnh (tier S ≈ 1 triệu dòng, chạy ~15 phút)
make all TIER=S

# Từng bước (linh hoạt hơn)
make profile  TIER=S    # profiling bronze → results/dq_report_bronze_null.csv
make bronze   TIER=M    # CSV → Parquet bronze
make silver   TIER=M    # bronze → silver (làm sạch R1–R9)
make gold     TIER=M    # silver → gold (sessionization, dim/fact, agg)
make analytics TIER=M   # gold → KPI, funnel, RFM, KMeans
make plots              # sinh biểu đồ → docs/figures/
```

### Bước 5 — Chạy thí nghiệm

```bash
# Tất cả E1–E7
make experiments TIER=L

# Từng thí nghiệm (khuyến nghị — dễ debug hơn)
make experiment-e1 TIER=L   # scale data
make experiment-e2 TIER=M   # scale resource
make experiment-e3 TIER=M   # format comparison
make experiment-e4 TIER=M   # tuning (shuffle, AQE, skew, join)
make experiment-e5 TIER=M   # crossover Pandas/DuckDB/Spark
make experiment-e6           # correctness oracle
make experiment-e7           # MapReduce simulation

# Vẽ biểu đồ thí nghiệm
make plot-experiments
```

### Bước 6 — Chạy test

```bash
make test              # tất cả
make test-cleaning     # chỉ test luật làm sạch R1–R9
make test-kpi          # chỉ test định nghĩa KPI
make test-invariants   # chỉ test bất biến pipeline
```

---

## Cấu trúc repo

```
bigdata-ecom/
├── README.md                    # file này
├── Makefile                     # make up | bronze | silver | gold | ...
├── pyproject.toml               # ruff + pytest config
├── requirements.txt             # phiên bản ghim chặt
│
├── docker/
│   ├── docker-compose.yml       # Spark cluster (master + 2 worker + history)
│   ├── spark-defaults.conf      # tham số Spark (AQE, shuffle partitions, ...)
│   └── .env.example             # mẫu biến môi trường
│
├── configs/
│   ├── pipeline.yaml            # đường dẫn, tier, tham số cleaning, analytics
│   └── data-contract.yaml       # schema bronze — thỏa thuận giữa M1 và M2
│
├── src/
│   ├── common/
│   │   ├── spark_session.py     # get_spark() helper
│   │   ├── config.py            # đọc pipeline.yaml
│   │   ├── logger.py            # logging chuẩn
│   │   └── metrics.py           # ghi experiment_log.csv (timed_run context)
│   │
│   ├── prepare_input/           # M1
│   │   ├── main.py              # CSV → bronze (tier S/M/L/XL)
│   │   └── profiling.py         # profiling bronze: null rate, phân phối
│   │
│   ├── processing/              # M2 + M3
│   │   ├── bronze_to_silver.py  # luật R1–R9, quarantine, DQ report
│   │   └── silver_to_gold.py    # sessionization, dim, fact, agg
│   │
│   └── analytics/               # M4 + M5
│       ├── kpi.py               # KPI, funnel, DAU, cohort retention
│       ├── rfm_kmeans.py        # RFM, KMeans, silhouette, cluster profiles
│       ├── run_analytics.py     # entry point: chạy tất cả analytics
│       └── plots.py             # sinh 8 biểu đồ analytics
│
├── experiments/
│   ├── harness.py               # framework đo: 3 lần, ghi CSV tự động
│   ├── e1_scale_data.py         # E1: wall time theo kích thước dữ liệu
│   ├── e2_scale_resource.py     # E2: speedup theo core/executor
│   ├── e3_format.py             # E3: CSV vs Parquet snappy/zstd, partition
│   ├── e4_tuning.py             # E4: shuffle partitions, AQE, join, skew/salting
│   ├── e5_crossover.py          # E5: Pandas vs DuckDB vs Spark
│   ├── e6_correctness.py        # E6: oracle DuckDB vs Spark
│   ├── e7_mapreduce.py          # E7: MapReduce simulation + combiner
│   ├── plot.py                  # biểu đồ thí nghiệm E1–E5
│   └── run_all.py               # chạy tất cả thí nghiệm
│
├── tests/
│   ├── conftest.py              # SparkSession fixture (local[1])
│   ├── test_cleaning_rules.py   # test R1–R6 trên dữ liệu tay
│   ├── test_kpi.py              # test KPI, funnel, cohort
│   └── test_invariants.py       # test bất biến count_in = count_out + count_q
│
├── results/                     # CSV kết quả đo (commit được, trừ *.parquet)
├── data/
│   ├── raw/                     # ← đặt CSV Kaggle ở đây (.gitignore)
│   └── lake/
│       ├── bronze/              # (.gitignore)
│       ├── silver/              # (.gitignore)
│       ├── gold/                # (.gitignore)
│       └── quarantine/          # (.gitignore)
└── docs/
    ├── phan-cong-nhom.md        # hướng dẫn chi tiết từng thành viên
    ├── data-dictionary.md       # schema đầy đủ tất cả bảng + KPI definitions
    ├── dataset-facts.md         # số dòng, dung lượng thực (điền sau khi đo)
    ├── adr/                     # Architectural Decision Records
    │   ├── 001-spark-vs-single-node.md
    │   ├── 002-parquet-format.md
    │   └── 003-storage-no-minio.md
    ├── figures/                 # PNG biểu đồ (sinh từ script, commit được)
    └── log/                     # nhật ký cá nhân theo tuần
        └── log-template.md
```

---

## Kết quả thí nghiệm *(điền sau khi chạy)*

| Thí nghiệm | Mô tả | Kết quả chính |
|------------|-------|---------------|
| E1 — Scale data | Wall time khi tăng từ 1%→100% dữ liệu | *(điền)* |
| E2 — Scale resource | Speedup: local[1]→local[4]→cluster | *(điền)* |
| E3 — Format | CSV vs Parquet snappy vs zstd | *(điền)* |
| E4 — Tuning | Shuffle partitions, AQE, skew salting | *(điền)* |
| E5 — Crossover | Điểm giao Pandas/DuckDB/Spark | *(điền)* |
| E6 — Correctness | Lệch KPI oracle vs Spark | *(điền)* |
| E7 — MapReduce | Số intermediate pairs: combiner giảm X% | *(điền)* |

---

## KPI chính *(điền sau khi chạy analytics)*

| KPI | Giá trị | Ghi chú |
|-----|---------|---------|
| Total Revenue | *(điền)* | Proxy (qty=1) |
| Total Sessions | *(điền)* | |
| Conversion Rate | *(điền)* | Sessions có purchase / sessions có view |
| Cart Abandonment | *(điền)* | |
| AOV Proxy | *(điền)* | Revenue / converted sessions |
| KMeans clusters | *(điền)* | k tốt nhất theo silhouette |

---

## Liên hệ mã nguồn — báo cáo

| Mục báo cáo | File / thư mục |
|-------------|----------------|
| 2. Cơ sở lý thuyết | `docs/adr/`, `experiments/e7_mapreduce.py` |
| 3. Dữ liệu | `docs/dataset-facts.md`, `src/prepare_input/profiling.py` |
| 4. Thiết kế | `docker/`, `configs/`, `docs/adr/` |
| 5. Cài đặt | `src/processing/bronze_to_silver.py`, `silver_to_gold.py` |
| 6. Thực nghiệm | `experiments/`, `results/experiment_log.csv`, `docs/figures/e*.png` |
| 7. Demo kết quả | `results/kpi_summary.csv`, `docs/figures/` |

---

## Troubleshooting

| Lỗi | Nguyên nhân | Cách sửa |
|-----|------------|----------|
| `No such file 2019-Oct.csv` | Chưa tải dataset | Tải vào `data/raw/` |
| `Container spark-master not found` | Cluster chưa chạy | `make up` |
| `Java heap space / OOM` | RAM Docker quá thấp | Tăng RAM lên ≥ 6 GB trong Docker Desktop |
| `AnalysisException: cannot cast` | ANSI mode Spark | Dùng `try_to_timestamp()`, không dùng `to_timestamp()` |
| `Connection refused :7077` | Master chưa sẵn sàng | Đợi 30 giây, kiểm tra `make logs` |
| `pyspark version mismatch` | PySpark local ≠ cluster | Ghim `pyspark==3.5.3` trong `requirements.txt` |
| `Shuffle spill quá nhiều` | RAM executor thiấp | Tăng `SPARK_WORKER_MEMORY` trong `docker/.env` |

---

## Tài liệu tham khảo

1. Apache Spark 3.5 Documentation — https://spark.apache.org/docs/3.5.0/
2. PySpark API Reference — https://spark.apache.org/docs/3.5.0/api/python/
3. REES46 Dataset (Kaggle) — https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data
4. DuckDB Documentation — https://duckdb.org/docs/
5. Yêu cầu đồ án học phần 841450 — tài liệu nhóm
6. Slide Chương 1–8, đề cương chi tiết — tài liệu nhóm
