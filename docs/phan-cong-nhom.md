# Phân Công Nhóm — Hướng Dẫn Chi Tiết A–Z

> Môn: Nhập môn dữ liệu lớn (841450) — Trường Đại học Sài Gòn  
> Nhóm: 5 người | Không có lead — mỗi người đều làm đầy đủ từ code → test → thí nghiệm → báo cáo  
> Dataset: REES46 eCommerce behavior data (Kaggle)  
> Stack: PySpark 3.5, Docker Compose, Parquet, DuckDB, Pandas, Matplotlib

---

## Mục lục

- [Tổng quan phân công](#tổng-quan-phân-công)
- [Kiến trúc pipeline — đọc trước khi bắt đầu](#kiến-trúc-pipeline)
- [Cài đặt môi trường chung](#cài-đặt-môi-trường-chung)
- [M1 — Hạ tầng + Bronze + E2 + E7](#m1--hạ-tầng--bronze--e2--e7)
- [M2 — Silver (làm sạch) + E3](#m2--silver-làm-sạch--e3)
- [M3 — Gold (sessionization) + E1 + E4](#m3--gold-sessionization--e1--e4)
- [M4 — Analytics KPI/Funnel/Cohort](#m4--analytics-kpifunnelcohort)
- [M5 — RFM/KMeans + Baseline + E5 + E6](#m5--rfmkmeans--baseline--e5--e6)
- [Quy trình Git chung](#quy-trình-git-chung)
- [Lịch trình 6 tuần](#lịch-trình-6-tuần)
- [Câu hỏi bảo vệ — mọi người phải trả lời được](#câu-hỏi-bảo-vệ)

---

## Tổng quan phân công

| Người | Module code | Thí nghiệm | Mục báo cáo | Review cho |
|-------|------------|------------|-------------|------------|
| **M1 — Hiếu** | Hạ tầng, Docker, Bronze | E2 (scale resource), E7 (MapReduce) | Chương 1, 4, Phụ lục | Silver (M2) |
| **M2** | Silver: làm sạch R1–R9, DQ report | E3 (định dạng) | Chương 3, 5 (DQ) | Gold (M3) |
| **M3** | Gold: sessionization, dim/fact, agg | E1 (scale data), E4 (tuning) | Chương 5 (processing), 6 (perf) | Bronze (M1) |
| **M4** | Analytics: KPI, funnel, cohort | Phân tích và vẽ biểu đồ | Chương 7 (demo kết quả) | RFM (M5) |
| **M5** | RFM, KMeans, baseline Pandas/DuckDB | E5 (crossover), E6 (correctness) | Chương 2 (lý thuyết), 6 (E5/E6) | KPI (M4) |

**Nguyên tắc**: Mỗi người sở hữu module đầu đến cuối.  
Khi bảo vệ đồ án, **mọi người đều phải giải thích được toàn bộ pipeline**, không chỉ phần mình làm.

---

## Kiến trúc pipeline

Hiểu sơ đồ này trước khi code bất cứ thứ gì:

```
data/raw/*.csv          ← file CSV tải từ Kaggle, đặt ở đây, KHÔNG commit lên git
       │
       │  src/prepare_input/main.py  (M1 làm)
       ▼
data/lake/bronze/       ← Parquet, schema ép kiểu, partition theo source_month
       │                   CHƯA làm sạch, CHƯA dedupe
       │  src/processing/bronze_to_silver.py  (M2 làm)
       ▼
data/lake/silver/       ← Parquet, đã làm sạch, partition theo event_date
data/lake/quarantine/   ← dòng bị loại + lý do (reject_reason)
       │
       │  src/processing/silver_to_gold.py  (M3 làm)
       ▼
data/lake/gold/         ← fact_sessions, dim_product, dim_user,
                           agg_daily, agg_hourly, agg_funnel
       │
       │  src/analytics/run_analytics.py  (M4 + M5 làm)
       ▼
results/                ← CSV kết quả KPI, funnel, cluster_profiles
docs/figures/           ← PNG biểu đồ
```

**Bất biến quan trọng**: `count(bronze) = count(silver) + count(quarantine)`  
Kiểm tra cái này sau mỗi lần chạy silver.

---

## Cài đặt môi trường chung

> **Tất cả 5 người đều làm bước này ở Ngày 0.**

### 1. Cài Docker Desktop

- Windows: tải tại https://www.docker.com/products/docker-desktop
- Bật WSL2 (Docker Desktop sẽ hỏi khi cài)
- Sau khi cài: mở Docker Desktop → Settings → Resources → Memory: đặt ít nhất **6 GB**

Kiểm tra:
```bash
docker --version        # phải thấy Docker version 24.x hoặc mới hơn
docker compose version  # phải thấy Docker Compose version v2.x
```

### 2. Cài Python 3.11 local (để chạy DuckDB/Pandas ngoài container)

```bash
python --version  # phải là 3.10 hoặc 3.11
pip install -r requirements.txt
```

### 3. Clone repo và khởi động cluster

```bash
git clone <URL_REPO_GITHUB> bigdata-ecom
cd bigdata-ecom

# Tạo file .env từ mẫu
cp docker/.env.example docker/.env

# Khởi động Spark cluster (lần đầu tải image ~5 phút)
docker compose -f docker/docker-compose.yml up -d

# Kiểm tra: mở trình duyệt
# http://localhost:8080   → Spark Master UI (phải thấy 2 Workers)
# http://localhost:18080  → History Server
```

### 4. Tải dataset

1. Đăng nhập Kaggle: https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data
2. Tải file `2019-Oct.csv` (và `2019-Nov.csv` nếu ổ đĩa đủ)
3. Đặt vào thư mục `data/raw/` (thư mục này bị `.gitignore`, không lo bị commit lên)

### 5. Smoke test — xác nhận môi trường OK

```bash
# Chạy job Spark đơn giản
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master local[2] /app/src/prepare_input/main.py --tier S

# Nếu thấy "Ghi bronze xong" ở cuối log → OK
# Nếu lỗi → đọc phần Troubleshooting bên dưới
```

**Troubleshooting thường gặp:**

| Lỗi | Nguyên nhân | Cách sửa |
|-----|------------|----------|
| `No such file: data/raw/2019-Oct.csv` | Chưa tải CSV | Tải CSV vào `data/raw/` |
| `Container not found` | Cluster chưa chạy | `docker compose up -d` |
| `Java heap space` | RAM Docker quá thấp | Tăng RAM Docker lên ≥6GB |
| `ANSI mode: cannot cast` | Spark 4 ANSI strict | Dùng `try_to_timestamp()` thay vì `to_timestamp()` |
| `Connection refused :7077` | Master chưa sẵn sàng | Đợi 30 giây rồi thử lại |

---

## M1 — Hạ tầng + Bronze + E2 + E7

> **Hiếu** — Files chính: `docker/`, `src/common/`, `src/prepare_input/`, `experiments/e2_*.py`, `experiments/e7_*.py`

### Kiến thức cần học (ưu tiên học theo thứ tự này)

#### 1. Docker Compose (1–2 tiếng)

Docker Compose cho phép chạy nhiều container cùng lúc bằng 1 file cấu hình.

Các lệnh hay dùng:
```bash
docker compose -f docker/docker-compose.yml up -d      # khởi động nền
docker compose -f docker/docker-compose.yml down       # dừng và xóa container
docker compose -f docker/docker-compose.yml logs -f    # xem log realtime
docker compose -f docker/docker-compose.yml ps         # xem trạng thái

# Vào trong container để chạy lệnh
docker compose -f docker/docker-compose.yml exec spark-master bash
```

Đọc file `docker/docker-compose.yml` — hiểu:
- Service `spark-master`: đóng vai điều phối, nhận job
- Service `spark-worker-1`, `spark-worker-2`: thực thi task
- Service `spark-history`: lưu và xem lại event log
- `volumes`: mount thư mục repo vào `/app` bên trong container

#### 2. Spark Cluster hoạt động thế nào (1 tiếng)

```
spark-submit job.py
       │
       ▼
   Driver (spark-master)
   - Phân tích DAG
   - Chia thành Stages
   - Gửi Task xuống Worker
       │
       ├── spark-worker-1 (chạy Task 1, 2, 3...)
       └── spark-worker-2 (chạy Task 4, 5, 6...)
```

- **Stage**: nhóm các transformation không cần shuffle
- **Task**: 1 đơn vị xử lý trên 1 partition
- **Executor**: JVM process trên worker, chạy các task

Xem Spark UI khi job đang chạy:
- `http://localhost:8080` → xem Job và Stage
- Click vào Stage → xem từng Task, thấy thời gian, shuffle read/write

#### 3. spark-submit (30 phút)

```bash
spark-submit \
  --master spark://spark-master:7077 \   # gửi lên cluster
  --conf spark.app.name=MyJob \
  --conf spark.sql.shuffle.partitions=200 \
  /app/src/prepare_input/main.py --tier M
```

Nếu chạy local (không cần cluster):
```bash
spark-submit \
  --master local[4] \   # dùng 4 core trên máy hiện tại
  /app/src/prepare_input/main.py --tier S
```

#### 4. Đọc file `src/prepare_input/main.py` — hiểu từng bước

```python
# Bước 1: Đọc CSV với schema tường minh
df = spark.read.schema(RAW_SCHEMA).option("header", "true").csv(files)

# Bước 2: Thêm metadata
df = df.withColumn("source_file", F.input_file_name())
df = df.withColumn("ingest_ts", F.current_timestamp())

# Bước 3: Trích source_month từ tên file
df = df.withColumn("source_month", F.regexp_extract(F.col("source_file"), r"(\d{4}-\d{2})", 1))

# Bước 4: Ghi Parquet, partition theo source_month
df.write.mode("overwrite").partitionBy("source_month").parquet(bronze_path)
```

Tại sao dùng schema tường minh (không dùng `inferSchema=True`)?
- `inferSchema` đọc toàn bộ file → chậm
- Có thể suy ra sai kiểu (VD: product_id suy ra là int nhưng thực ra cần long)
- Với ANSI mode, suy ra sai → lỗi ép kiểu ở bước sau

#### 5. Định luật Amdahl (cho E2)

Định luật Amdahl giải thích tại sao tăng gấp đôi core không có nghĩa là nhanh gấp đôi:

```
Speedup(n) = 1 / (s + (1 - s) / n)

Trong đó:
  s = phần tuần tự của chương trình (không song song được)
  n = số core/executor
  
Ví dụ: s = 0.1 (10% code chạy tuần tự)
  n=2:  Speedup = 1 / (0.1 + 0.9/2)  = 1.82×  (hiệu suất 91%)
  n=4:  Speedup = 1 / (0.1 + 0.9/4)  = 3.08×  (hiệu suất 77%)
  n=8:  Speedup = 1 / (0.1 + 0.9/8)  = 4.71×  (hiệu suất 59%)
  n=∞:  Speedup tối đa = 10×          (dù có vô số core)
```

Trong Spark, phần tuần tự bao gồm:
- Driver phân tích plan
- Shuffle: dữ liệu phải gom lại theo key (bottleneck mạng/đĩa)
- Khởi động JVM executor

#### 6. MapReduce và Combiner (cho E7)

```
Input:  [(view, 1), (purchase, 1), (view, 1), (view, 1), (purchase, 1)]

Map phase (mỗi worker xử lý một phần):
  Worker 1: (view,1), (purchase,1), (view,1)
  Worker 2: (view,1), (purchase,1)

--- Shuffle: gom theo key ---

Reduce phase:
  view:     1+1+1 = 3
  purchase: 1+1   = 2
```

Combiner (tối ưu, giảm shuffle):
```
Không combiner:  Worker 1 gửi 3 cặp qua mạng
Có combiner:     Worker 1 tự tổng trước → gửi (view,2), (purchase,1)
                 → tiết kiệm 1 cặp, tiết kiệm hơn nhiều ở dữ liệu lớn
```

**Quan trọng**: Combiner KHÔNG bắt buộc phải chạy. Spark/Hadoop có thể bỏ qua.  
→ Code phải đúng cả khi không có combiner.

---

### Việc làm từng tuần

#### Ngày 0–3: Setup

- [ ] Cài Docker, chạy cluster, smoke test thành công
- [ ] Tải dataset, đếm số dòng, điền vào `docs/dataset-facts.md`
- [ ] Init git repo, push lên GitHub, bật branch protection cho `main`
- [ ] Tạo GitHub Projects (Backlog / Doing / Review / Done), thêm issue cho mọi người
- [ ] Báo RAM/ổ đĩa cho nhóm

#### Tuần 1 (W1): Bronze tier S chạy được

```bash
# Chạy prepare_input
make bronze TIER=S

# Kiểm tra output
docker compose exec spark-master python - <<'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.read.parquet("/app/data/lake/bronze/events")
print(f"Số dòng bronze tier S: {df.count():,}")
df.printSchema()
df.show(3)
spark.stop()
EOF
```

- [ ] Bronze tier S ghi thành công vào `data/lake/bronze/`
- [ ] Schema đúng 11 cột (9 gốc + source_file + ingest_ts)
- [ ] Commit: `feat(bronze): implement prepare_input with tier S/M/L/XL support`

#### Tuần 2 (W2): CI + Harness

- [ ] Kiểm tra CI tự chạy khi push PR (`.github/workflows/ci.yml`)
- [ ] Hiểu `src/common/metrics.py` — cách ghi `experiment_log.csv`
- [ ] Đọc `experiments/harness.py` — hiểu `timed_run` context manager

#### Tuần 3 (W3): Chạy E2

```bash
# Chạy sau khi M2 có silver tier M
docker compose exec spark-master \
  spark-submit /app/experiments/e2_scale_resource.py --tier M
```

Sau khi chạy:
1. Mở `results/experiment_log.csv`
2. Lọc `experiment` bắt đầu bằng `E2_`
3. Tính speedup = `wall_time_E2_local_1 / wall_time_E2_cấu_hình_n`
4. Vẽ biểu đồ: `python experiments/plot.py --experiments E2`
5. **Chụp ảnh Spark UI** cho `local[1]` và `local[4]` → lưu vào `docs/figures/`
6. Ghi nhật ký: `docs/log/hieu-w3.md`

#### Tuần 3: Chạy E7

```bash
python experiments/e7_mapreduce.py
```

So sánh:
- Số intermediate pairs không có combiner
- Số intermediate pairs có combiner  
- Tính `% giảm = (không_combiner - có_combiner) / không_combiner × 100`

#### Tuần 4 (W4): Báo cáo

Chịu trách nhiệm viết:
- **Chương 1** (~2 trang): Giới thiệu, bối cảnh, phạm vi, 3 câu hỏi kỹ thuật
- **Chương 4** (~4 trang): Thiết kế hệ thống — sơ đồ kiến trúc, bảng công nghệ + lý do, ADR
- **Phụ lục**: Hướng dẫn chạy chi tiết, cấu trúc repo, bảng đóng góp từ Git

Sơ đồ kiến trúc: dùng Mermaid hoặc draw.io (https://draw.io)
```
Ví dụ Mermaid:
graph LR
    A[CSV thô] -->|prepare_input| B[Bronze Parquet]
    B -->|bronze_to_silver| C[Silver Parquet]
    C -->|silver_to_gold| D[Gold Parquet]
    D -->|run_analytics| E[KPI / RFM / Funnel]
```

---

## M2 — Silver (làm sạch) + E3

> Files chính: `src/processing/bronze_to_silver.py`, `src/prepare_input/profiling.py`, `experiments/e3_format.py`, `tests/test_cleaning_rules.py`

### Kiến thức cần học

#### 1. PySpark DataFrame API — các hàm dùng cho làm sạch

```python
from pyspark.sql import functions as F

# Kiểm tra null
df.filter(F.col("user_id").isNull())

# Thay thế null
F.coalesce(F.col("brand"), F.lit("unknown"))

# Điều kiện
F.when(F.col("price") < 0, True).otherwise(False)

# Parse timestamp AN TOÀN (ANSI mode)
# KHÔNG dùng: F.to_timestamp(...)  → có thể raise lỗi với giá trị sai
# NÊN dùng:   F.try_to_timestamp(...) → trả null nếu lỗi parse
F.try_to_timestamp(F.col("event_time"), "yyyy-MM-dd HH:mm:ss z")

# Tách chuỗi theo dấu chấm
F.split(F.col("category_code"), r"\.")
F.split(F.col("category_code"), r"\.").getItem(0)  # lấy phần tử đầu tiên

# Regex extract
F.regexp_extract(F.col("source_file"), r"(\d{4}-\d{2})", 1)

# Đọc log kết quả
df.count()       # đếm số dòng (action → trigger tính toán)
df.show(10)      # hiển thị 10 dòng đầu
df.printSchema() # in cấu trúc schema
```

#### 2. ANSI mode trong Spark 4 — quan trọng!

Spark 4.x mặc định bật `spark.sql.ansi.enabled = true`.

Ý nghĩa: ép kiểu sai **báo lỗi** thay vì trả `null` như các bản trước.

```python
# Ví dụ: event_time = "abc" (không phải timestamp)

# Spark 3 (ANSI tắt):
F.to_timestamp("abc")  # → trả null, job vẫn chạy

# Spark 4 (ANSI bật):
F.to_timestamp("abc")  # → raise AnalysisException, job chết!

# Cách xử lý đúng:
F.try_to_timestamp(F.col("event_time"), "yyyy-MM-dd HH:mm:ss z")
# → null nếu không parse được, không chết job
# → row này sẽ vào quarantine (R3)
```

#### 3. Các luật làm sạch R1–R9 — hiểu mục đích mỗi luật

Mở file `src/processing/bronze_to_silver.py` và đọc từng hàm `apply_r*`:

| Luật | Kiểm tra | Xử lý |
|------|---------|-------|
| R1 | Null trong cột khóa (event_time, event_type, product_id, user_id) | → quarantine |
| R2 | event_type không nằm trong tập hợp lệ | → quarantine |
| R3 | Không parse được timestamp | → quarantine |
| R4 | price < 0 | → quarantine; price = 0 → giữ + flag |
| R5 | brand null | → thay "unknown" + flag brand_missing |
| R6 | category_code null | → l1="unknown"; có → tách l1/l2/l3 |
| R7 | Dòng trùng hoàn toàn | → dedupe |
| R8 | Sự kiện ngoài tháng của file | → flag out_of_month |
| R9 | user_session null | → TODO sinh session bằng gap 30 phút |

#### 4. DQ Report (Data Quality Report)

DQ report là bảng chứng minh pipeline xử lý đúng. **Đây là yêu cầu bắt buộc của đề.**

Format mẫu:

| Luật | Tên | Dòng vào | Dòng ra | Bị loại | Tỷ lệ lỗi | Thời gian |
|------|-----|----------|---------|---------|-----------|-----------|
| R1 | null_key_field | 42,448,764 | 42,447,012 | 1,752 | 0.004% | 12.3s |
| R2 | invalid_event_type | 42,447,012 | 42,445,800 | 1,212 | 0.003% | 8.1s |
| ... | ... | ... | ... | ... | ... | ... |
| **Tổng** | | **42,448,764** | **42,000,000** | **448,764** | **1.06%** | |

#### 5. Parquet — hiểu để làm E3

Parquet là định dạng lưu trữ dạng **cột** (columnar), ngược với CSV là dạng **hàng**.

```
CSV (hàng):           Parquet (cột):
event_time,event_type,price     event_time  | event_type | price
2019-10-01,view,199.0           2019-10-01  | view       | 199.0
2019-10-01,purchase,299.0       2019-10-01  | purchase   | 299.0
...                             ...         | ...        | ...
```

Lợi ích:
1. **Column pruning**: `SELECT price FROM ...` → chỉ đọc cột price, bỏ qua event_type
2. **Compression tốt hơn**: cùng loại dữ liệu → nén hiệu quả hơn
3. **Predicate pushdown**: filter điều kiện đẩy xuống tầng đọc

Partition pruning:
```
data/lake/silver/events/
  event_date=2019-10-01/   ← chỉ đọc folder này khi filter event_date='2019-10-01'
  event_date=2019-10-02/
  ...
```

Codec nén:
- `snappy`: nhanh, nén ~2-3× so với CSV, decode nhanh → phù hợp cho dữ liệu đọc nhiều
- `zstd`: chậm hơn khi ghi, nén ~3-4×, decode chậm hơn một chút → phù hợp cho lưu trữ lâu dài

---

### Việc làm từng tuần

#### Tuần 1 (W1): Profiling

```bash
# Sau khi M1 có bronze tier S
make bronze TIER=S   # chạy trước nếu chưa có

docker compose exec spark-master \
  spark-submit /app/src/prepare_input/profiling.py --tier S
```

**Ghi lại vào `docs/log/m2-w1.md`:**
1. event_type có những giá trị nào? Có giá trị lạ không?
2. Null rate của brand: bao nhiêu %?
3. Null rate của category_code: bao nhiêu %?
4. price: giá trị nhỏ nhất, lớn nhất, có âm không?
5. event_time trông như thế nào? (VD: `"2019-10-01 00:00:00 UTC"`)

Cập nhật `configs/pipeline.yaml` nếu profiling cho thấy giá trị thực khác giả định.

#### Tuần 2 (W2): Silver tier M

```bash
make silver TIER=M
```

Kiểm tra sau khi chạy:
```bash
docker compose exec spark-master python - <<'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[2]").getOrCreate()

bronze = spark.read.parquet("/app/data/lake/bronze/events")
silver = spark.read.parquet("/app/data/lake/silver/events")
quarantine = spark.read.parquet("/app/data/lake/quarantine/events")

b = bronze.count()
s = silver.count()
q = quarantine.count()

print(f"Bronze:     {b:,}")
print(f"Silver:     {s:,}")
print(f"Quarantine: {q:,}")
print(f"Bất biến:   {b} == {s} + {q} → {'OK' if b == s + q else 'LỖI'}")

# Xem các lý do quarantine
quarantine.groupBy("reject_reason").count().orderBy("count", ascending=False).show()
spark.stop()
EOF
```

Viết DQ report vào `results/dq_report_silver.csv` (M2 tự viết code cho cái này).

#### Tuần 2: Chạy tests

```bash
make test
# Hoặc cụ thể hơn:
docker compose exec spark-master \
  python -m pytest tests/test_cleaning_rules.py -v
```

Nếu test fail: đọc message lỗi, sửa code, commit, push.  
**Không sửa test để pass** trừ khi logic trong test thực sự sai.

#### Tuần 3 (W3): E3 — So sánh định dạng

```bash
docker compose exec spark-master \
  spark-submit /app/experiments/e3_format.py --tier M
```

E3 sẽ:
1. Ghi dữ liệu ra 5 định dạng (vào `results/e3_tmp/`)
2. Chạy 3 truy vấn × 5 định dạng × 3 lần mỗi truy vấn
3. Ghi tất cả vào `results/experiment_log.csv`

Sau khi chạy:
```bash
python experiments/plot.py --experiments E3
# Xem docs/figures/e3_format_time.png
```

Phân tích cần viết trong báo cáo:
- Parquet nhanh hơn CSV bao nhiêu lần ở Q2 (tổng hợp 1 cột)?
- Parquet có partition nhanh hơn không có partition bao nhiêu lần ở Q3 (filter ngày)?
- Dung lượng: CSV vs snappy vs zstd (ghi vào bảng)

#### Tuần 4 (W4): Báo cáo

Chịu trách nhiệm viết:
- **Chương 3** (~4 trang): Nguồn dữ liệu, schema, số dòng thực, đặc trưng Big Data, data contract, ví dụ bản ghi
- **Chương 5 phần DQ** (~2 trang): DQ report (bảng luật R1–R9), ví dụ dòng trước/sau làm sạch

---

## M3 — Gold (sessionization) + E1 + E4

> Files chính: `src/processing/silver_to_gold.py`, `experiments/e1_scale_data.py`, `experiments/e4_tuning.py`

### Kiến thức cần học

#### 1. Sessionization — khái niệm và cài đặt

Sessionization = nhóm các sự kiện của cùng user thành "phiên mua sắm".

Dataset REES46 đã có cột `user_session`, nhưng ta cần:
1. Dùng `user_session` gốc làm session key chính
2. Tự sinh session bằng "gap" 30 phút để so sánh

Sinh session bằng gap (R9):
```python
from pyspark.sql import Window
import pyspark.sql.functions as F

w = Window.partitionBy("user_id").orderBy("event_ts")

# Thời gian kể từ sự kiện trước
df = df.withColumn(
    "prev_ts",
    F.lag("event_ts").over(w)
)
df = df.withColumn(
    "gap_minutes",
    (F.unix_timestamp("event_ts") - F.unix_timestamp("prev_ts")) / 60
)
# Sự kiện đầu tiên hoặc sau gap > 30 phút → bắt đầu session mới
df = df.withColumn(
    "new_session_flag",
    F.when(
        F.col("prev_ts").isNull() | (F.col("gap_minutes") > 30),
        1
    ).otherwise(0)
)
# Session ID = tổng cộng dồn số lần bắt đầu session mới
df = df.withColumn(
    "gap_session_id",
    F.sum("new_session_flag").over(w)
)
```

#### 2. Narrow vs Wide Transformation — hiểu để đọc Spark UI

```
Narrow transformation (không shuffle):
  filter(), select(), map(), withColumn()
  → mỗi partition đầu ra chỉ cần 1 partition đầu vào
  → không tốn mạng/đĩa trung gian

Wide transformation (cần shuffle):
  groupBy(), join(), distinct(), orderBy()
  → mỗi partition đầu ra cần dữ liệu từ NHIỀU partition đầu vào
  → phải shuffle: ghi dữ liệu trung gian ra đĩa, đọc lại theo key
```

Trong Spark UI → Stages:
- Ranh giới stage = chỗ có shuffle
- Mỗi stage = tập hợp các narrow transformation
- Xem "Input Size" và "Shuffle Write" mỗi stage

#### 3. Data Skew và Salting (E4)

**Skew** = phân phối dữ liệu không đều theo key.

Ví dụ thực tế: product_id = 1515966223509580337 có 10 triệu sự kiện,  
trong khi hầu hết product_id chỉ có vài chục sự kiện.

```
Không skew:          Có skew:
Task 1: 10s          Task 1: 10s
Task 2: 10s          Task 2: 10s
Task 3: 10s          Task 3: 347s  ← hot key, kéo cả stage
Task 4: 10s          Task 4: 10s
Stage: 10s           Stage: 347s  ← chờ task 3
```

Phát hiện skew trong Spark UI:
- Stages → click vào stage có groupBy
- Xem bảng Task Summary → cột Duration
- Nếu max >> median → có skew

Xử lý bằng Salting:
```python
# Thêm cột salt ngẫu nhiên (0 đến 19)
df = df.withColumn("salt", (F.rand() * 20).cast("int"))

# Group lần 1: (product_id, salt) → tổng cục bộ
partial = df.groupBy("product_id", "salt").agg(F.sum("price").alias("partial"))

# Group lần 2: product_id → tổng toàn cục
result = partial.groupBy("product_id").agg(F.sum("partial").alias("total"))

# Key "hot" bị chia thành 20 task nhỏ → không còn 1 task siêu nặng
```

#### 4. AQE (Adaptive Query Execution)

AQE = Spark tự điều chỉnh kế hoạch lúc chạy dựa trên thống kê thực tế.

3 tính năng chính:
1. **Coalesce partitions**: sau shuffle nếu nhiều partition rỗng → tự gộp lại
2. **Skew join handling**: phát hiện partition quá lớn → tự chia nhỏ
3. **Switch join strategy**: nếu sau shuffle một bảng nhỏ hơn dự kiến → đổi sang broadcast

```python
# Bật AQE (mặc định trong spark-defaults.conf)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Tắt AQE để thí nghiệm (E4 T2)
spark.conf.set("spark.sql.adaptive.enabled", "false")
```

#### 5. Broadcast Join vs Sort-Merge Join

```
Sort-Merge Join (mặc định khi cả 2 bảng lớn):
  → Sort cả 2 bảng theo join key
  → Merge: lần lượt so sánh
  → Tốn shuffle cho cả 2 bảng

Broadcast Join (khi 1 bảng nhỏ):
  → Gửi bảng nhỏ (dim_product) tới TẤT CẢ executor
  → Mỗi executor tự join local → KHÔNG cần shuffle bảng lớn
  
Khi nào dùng broadcast:
  → Bảng nhỏ < spark.sql.autoBroadcastJoinThreshold (mặc định 10MB)
  → Hoặc dùng hint: F.broadcast(dim_product)
```

---

### Việc làm từng tuần

#### Tuần 1 (W1): Hiểu gold và chạy tier S

```bash
# Cần silver tier S trước
make silver TIER=S
make gold TIER=S
```

Kiểm tra:
```bash
docker compose exec spark-master python - <<'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[2]").getOrCreate()

fact = spark.read.parquet("/app/data/lake/gold/fact_sessions")
print(f"Tổng phiên: {fact.count():,}")
fact.printSchema()
fact.describe(["revenue", "view_count", "purchase_count"]).show()

# Kiểm tra grain: mỗi (user_id, user_session) chỉ xuất hiện 1 lần
dup = fact.groupBy("user_id","user_session").count().filter("count > 1").count()
print(f"Duplicate sessions: {dup} (phải = 0)")
spark.stop()
EOF
```

#### Tuần 2 (W2): Hoàn thiện session gap + Gold tier M

Thêm session gap vào `silver_to_gold.py`:
```python
# Trong hàm build_fact_sessions(), sau khi đọc silver:
# Bước 1: Tính gap_session_id theo window
# Bước 2: Group theo (user_id, gap_session_id) để có fact_gap_sessions
# Bước 3: So sánh count với fact_sessions (dùng user_session gốc)
```

Ghi kết quả so sánh vào nhật ký:
```
fact_sessions (dùng user_session gốc): X phiên
fact_gap_sessions (sinh bằng gap 30 phút): Y phiên
Chênh lệch: (Y - X) / X × 100 = ?%
```

#### Tuần 3 (W3): E1 và E4

**Chạy E1** (sau khi có silver tier L):
```bash
docker compose exec spark-master \
  spark-submit /app/experiments/e1_scale_data.py --tier L --runs 3
```

**Trong lúc E1 chạy**: giữ Spark UI mở tại `http://localhost:8080`
- Chụp ảnh tab "Stages" khi đang chạy fraction 100%
- Xem "Shuffle Write" tăng tỷ lệ thuận với data không

**Chạy E4**:
```bash
docker compose exec spark-master \
  spark-submit /app/experiments/e4_tuning.py --tier M
```

Sau T4 (skew):
- Mở History Server: `http://localhost:18080`
- Tìm run `E4_T4_no_salt` → vào Stages → xem task time distribution
- So sánh với `E4_T4_salt20`
- Chụp ảnh cả 2 → lưu vào `docs/figures/e4_task_skew_before.png` và `e4_task_skew_after.png`

#### Tuần 4 (W4): Báo cáo

- **Chương 5 phần processing** (~2 trang): sessionization, dim/fact grain, agg tables
- **Chương 6 phần hiệu năng** (~3 trang): E1 (đường cong thời gian–data), E4 (bảng tuning, ảnh task distribution)

---

## M4 — Analytics KPI/Funnel/Cohort

> Files chính: `src/analytics/kpi.py`, `src/analytics/run_analytics.py`, `tests/test_kpi.py`

### Kiến thức cần học

#### 1. Định nghĩa KPI — đọc thuộc lòng trước khi code

```
Revenue = SUM(price) WHERE event_type = 'purchase' AND price > 0
         Lưu ý: KHÔNG có quantity → đây là PROXY, phải nói rõ trong báo cáo

Sessions = COUNT(DISTINCT user_session)

Conversion rate = Số phiên có ít nhất 1 purchase / Số phiên có ít nhất 1 view
                 (KHÔNG chia cho tổng số phiên)

Cart abandonment = Phiên có cart nhưng không purchase / Phiên có cart

AOV proxy = Revenue / Số phiên đã purchase
            (Average Order Value — proxy vì không có order_id thật)

DAU = COUNT(DISTINCT user_id) GROUP BY event_date
```

Tại sao conversion rate chia cho "phiên có view" chứ không phải tất cả phiên?
- Có phiên chỉ có purchase, không có view (bất thường nhưng tồn tại trong data)
- Công thức chuẩn của e-commerce: "Trong số người đã xem hàng, bao nhiêu % mua?"

#### 2. Spark SQL vs DataFrame API

Cả hai đều cho kết quả như nhau, chọn cái nào dễ đọc hơn:

```python
# Cách 1: DataFrame API
from pyspark.sql import functions as F

result = (
    df.filter(F.col("event_type") == "purchase")
      .groupBy("category_l1")
      .agg(F.sum("price").alias("revenue"))
      .orderBy(F.desc("revenue"))
)

# Cách 2: Spark SQL (dễ đọc hơn với người quen SQL)
df.createOrReplaceTempView("silver")
result = spark.sql("""
    SELECT category_l1, SUM(price) AS revenue
    FROM silver
    WHERE event_type = 'purchase'
    GROUP BY category_l1
    ORDER BY revenue DESC
""")
```

#### 3. Window Functions — dùng cho cohort

```python
from pyspark.sql import Window
import pyspark.sql.functions as F

# Tuần đầu tiên mua của mỗi user
w_user = Window.partitionBy("user_id")
df = df.withColumn(
    "first_purchase_week",
    F.date_trunc("week", F.min("session_start").over(w_user))
)
```

#### 4. Vẽ biểu đồ đúng với Matplotlib

Nguyên tắc:
- **Line chart**: dữ liệu theo thời gian (doanh thu theo ngày)
- **Bar chart**: so sánh các nhóm (doanh thu theo category)
- **Heatmap**: ma trận 2 chiều (thứ × giờ)
- **Stacked bar**: funnel steps

Luôn có: trục x/y có label + đơn vị, title, legend nếu có nhiều series.

```python
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

# Đọc kết quả từ results/
df = pd.read_csv("results/revenue_by_day/part-00000-*.csv")
df["event_date"] = pd.to_datetime(df["event_date"])

fig, ax = plt.subplots(figsize=(12, 5))
ax.plot(df["event_date"], df["revenue"] / 1e6, marker=".")
ax.set_xlabel("Ngày")
ax.set_ylabel("Doanh thu (triệu USD)")
ax.set_title("Doanh thu theo ngày — Tháng 10/2019")
ax.grid(True, linestyle="--", alpha=0.4)
fig.savefig("docs/figures/revenue_timeseries.png", dpi=150, bbox_inches="tight")
plt.close(fig)
```

#### 5. Cohort analysis — hiểu khái niệm

Cohort = nhóm user bắt đầu mua hàng trong cùng một tuần.

Ví dụ:
```
Cohort 2019-10-07 (100 users mua lần đầu tuần này):
  Tuần 0 (2019-10-07): 100 users (100% — luôn là 100%)
  Tuần 1 (2019-10-14): 30 users  (30% retention)
  Tuần 2 (2019-10-21): 15 users  (15% retention)
  Tuần 3 (2019-10-28): 8 users   (8% retention)
```

**Hạn chế quan trọng**: Nếu chỉ có 1–2 tháng data, cohort tuần cuối sẽ chỉ thấy tuần 0 và tuần 1. Phải ghi rõ hạn chế này trong báo cáo.

---

### Việc làm từng tuần

#### Tuần 1 (W1): Chạy analytics tier S và đọc hiểu code

```bash
make all TIER=S    # chạy toàn bộ pipeline
make analytics TIER=S
```

Kiểm tra `results/kpi_summary.csv` — các con số có hợp lý không?

```bash
cat results/kpi_summary.csv
# Kiểm tra:
# - conversion_rate: thường 1–5% trong e-commerce, nếu 50% thì có bug
# - total_revenue: phải dương
# - aov_proxy: thường $50–$500
```

Đọc kỹ `src/analytics/kpi.py`:
- Tại sao `compute_conversion_rate` dùng `sessions_with_view` chứ không phải `total_sessions`?
- Tại sao `cohort_retention` chỉ dùng phiên `converted = True`?

#### Tuần 2 (W2): Bổ sung visualization

Tạo file `src/analytics/plots.py`:

```python
# src/analytics/plots.py
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

RESULTS = Path("/app/results")
FIGURES = Path("/app/docs/figures")

def plot_revenue_timeseries():
    """Doanh thu theo ngày."""
    # Đọc results/revenue_by_day/ (là thư mục chứa part-*.csv)
    import glob
    files = glob.glob(str(RESULTS / "revenue_by_day" / "*.csv"))
    df = pd.concat([pd.read_csv(f) for f in files])
    df["event_date"] = pd.to_datetime(df["event_date"])
    df = df.sort_values("event_date")
    
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(df["event_date"], df["revenue"] / 1e6, color="steelblue")
    ax.set_xlabel("Ngày")
    ax.set_ylabel("Doanh thu (triệu USD)")
    ax.set_title("Doanh thu theo ngày")
    ax.grid(True, linestyle="--", alpha=0.4)
    FIGURES.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIGURES / "revenue_timeseries.png", dpi=150, bbox_inches="tight")
    plt.close()

def plot_funnel():
    """Funnel chart theo category_l1 top 10."""
    import glob
    files = glob.glob(str(RESULTS / "funnel_by_category" / "*.csv"))
    df = pd.concat([pd.read_csv(f) for f in files])
    top10 = df.nlargest(10, "sessions_with_view")
    
    x = range(len(top10))
    fig, ax = plt.subplots(figsize=(12, 5))
    width = 0.25
    ax.bar([i - width for i in x], top10["sessions_with_view"],  width, label="View",     color="#4c72b0")
    ax.bar([i         for i in x], top10["sessions_with_cart"],   width, label="Cart",     color="#dd8452")
    ax.bar([i + width for i in x], top10["sessions_with_purchase"],width, label="Purchase",color="#55a868")
    ax.set_xticks(list(x))
    ax.set_xticklabels(top10["category_l1"], rotation=30, ha="right")
    ax.set_ylabel("Số phiên")
    ax.set_title("Funnel View → Cart → Purchase theo danh mục (Top 10)")
    ax.legend()
    fig.savefig(FIGURES / "funnel_by_category.png", dpi=150, bbox_inches="tight")
    plt.close()

if __name__ == "__main__":
    plot_revenue_timeseries()
    plot_funnel()
    print("Biểu đồ đã lưu vào docs/figures/")
```

#### Tuần 3 (W3): Analytics tier L + Cohort

```bash
make analytics TIER=L
python src/analytics/plots.py
```

Kiểm tra cohort có đủ tuần không:
- Nếu chỉ có dữ liệu tháng 10 → cohort chỉ thấy 4 tuần → ghi rõ hạn chế
- Nếu có tháng 10 + 11 → cohort thấy 8 tuần → có ý nghĩa hơn

#### Tuần 4 (W4): Báo cáo

- **Chương 7** (~3 trang): Demo kết quả — ảnh biểu đồ KPI, funnel, cohort + giải thích nghiệp vụ
  - Category nào có conversion rate cao nhất?
  - Giờ nào và thứ nào có doanh thu cao nhất?
  - Funnel drop-off nhiều nhất ở bước nào?

---

## M5 — RFM/KMeans + Baseline + E5 + E6

> Files chính: `src/analytics/rfm_kmeans.py`, `experiments/e5_crossover.py`, `experiments/e6_correctness.py`

### Kiến thức cần học

#### 1. RFM Analysis

RFM là phương pháp phân khúc khách hàng dựa trên 3 chiều:

```
R = Recency   : Mua gần đây không? (ngày kể từ lần mua cuối)
                Nhỏ = tốt (mới mua gần đây)

F = Frequency : Mua bao nhiêu lần? (số phiên có purchase)
                Lớn = tốt

M = Monetary  : Chi bao nhiêu? (tổng doanh thu)
                Lớn = tốt
```

Ví dụ phân khúc sau KMeans:
| Cụm | Tên nghiệp vụ | Recency | Frequency | Monetary |
|-----|--------------|---------|-----------|---------|
| 0 | Khách trung thành | Thấp (mới mua) | Cao | Cao |
| 1 | Khách ngủ đông | Cao (lâu không mua) | Trung bình | Trung bình |
| 2 | Khách mới | Thấp (mới mua) | Thấp | Thấp |
| 3 | Khách VIP | Thấp | Rất cao | Rất cao |

**Quan trọng**: RFM chỉ áp dụng cho user có ít nhất 1 purchase.  
User chỉ view/cart nhưng không mua → không có trong RFM.

#### 2. Tiền xử lý cho KMeans

**Vấn đề 1: Phân phối skew**

RFM thường có phân phối lệch (skew) — vài user mua rất nhiều lần/rất nhiều tiền:
```python
# KHÔNG nên dùng giá trị raw vì outlier kéo cluster
# NÊN dùng log transform:
df = df.withColumn("log_recency",   F.log1p(F.col("recency")))
df = df.withColumn("log_frequency", F.log1p(F.col("frequency")))
df = df.withColumn("log_monetary",  F.log1p(F.col("monetary")))
# log1p(x) = log(1 + x) → tránh log(0) khi x=0
```

**Vấn đề 2: Đơn vị khác nhau**

- Recency đơn vị là ngày (0–365)
- Frequency đơn vị là lần (1–100)
- Monetary đơn vị là USD (0–10000)

→ StandardScaler: đưa mỗi chiều về trung bình=0, độ lệch=1:
```python
from pyspark.ml.feature import StandardScaler, VectorAssembler

assembler = VectorAssembler(
    inputCols=["log_recency","log_frequency","log_monetary"],
    outputCol="features_raw"
)
scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                        withStd=True, withMean=True)
```

#### 3. Chọn k — Elbow + Silhouette

**Elbow method**: Vẽ inertia (tổng khoảng cách bình phương tới centroid) theo k.
Tìm điểm mà inertia giảm chậm lại → đó là k tốt.

```python
# Thêm vào rfm_kmeans.py để log inertia
model = km.fit(rfm_scaled)
print(f"k={k}, inertia={model.summary.trainingCost:.2f}")
```

**Silhouette score** (-1 đến +1):
- Gần +1: điểm nằm rất "trong" cluster của nó, xa cluster khác → tốt
- Gần 0: điểm nằm gần ranh giới giữa 2 cluster
- Gần -1: điểm bị phân loại sai cluster

```python
from pyspark.ml.evaluation import ClusteringEvaluator
evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
score = evaluator.evaluate(predictions)
```

**Kiểm tra ổn định** (quan trọng!):
Chạy KMeans với nhiều seed khác nhau (42, 0, 123). Nếu cluster profiles tương tự nhau → thuật toán ổn định với data này.

#### 4. Pandas chunk và DuckDB (cho E5)

**Pandas đọc Parquet theo chunk** (tránh hết RAM):
```python
import pyarrow.parquet as pq

parquet = pq.ParquetFile("sample.parquet")
for batch in parquet.iter_batches(batch_size=65536,
                                  columns=["event_type", "category_l1", "price"]):
    df_chunk = batch.to_pandas()
    # xử lý và cộng dồn kết quả của batch...
```

**DuckDB** — query thẳng Parquet không cần load:
```python
import duckdb

con = duckdb.connect()
result = con.execute("""
    SELECT category_l1, SUM(price) AS revenue
    FROM read_parquet('data/lake/silver/events/**/*.parquet')
    WHERE event_type = 'purchase'
    GROUP BY category_l1
    ORDER BY revenue DESC
""").df()  # .df() trả về pandas DataFrame
```

DuckDB nhanh nhất ở tier S mức 100% và tier M mức 5–50% trong kết quả E5 đã đo; Spark Local nhanh nhất ở tier M mức 100%. Phép đo không tính thời gian khởi tạo SparkSession, nên không dùng JVM startup để giải thích trực tiếp số liệu trên hình.

#### 5. Oracle và E6 — đảm bảo kết quả đúng

Oracle = bản tham chiếu độc lập tính bằng DuckDB (đơn giản, ít bug).

Quy trình E6:
1. Tính 3 KPI bằng DuckDB (oracle)
2. Đọc kết quả Spark từ `results/kpi_summary.csv`
3. So sánh: nếu lệch > 0.1% → tìm nguyên nhân

```python
import duckdb, pandas as pd

# Oracle
con = duckdb.connect()
oracle_revenue = con.execute("""
    SELECT ROUND(SUM(price), 2)
    FROM read_parquet('data/lake/silver/events/**/*.parquet')
    WHERE event_type = 'purchase' AND price > 0
""").fetchone()[0]

# Spark result
spark_kpi = pd.read_csv("results/kpi_summary.csv").iloc[0]
spark_revenue = spark_kpi["total_revenue"]

diff_pct = abs(oracle_revenue - spark_revenue) / oracle_revenue * 100
print(f"Oracle: {oracle_revenue}, Spark: {spark_revenue}, Diff: {diff_pct:.4f}%")
# Phải < 0.1% để coi là đúng
```

---

### Việc làm từng tuần

#### Tuần 1 (W1): Làm quen DuckDB và rfm_kmeans.py

```bash
# Cài local (ngoài container)
pip install duckdb pyarrow pandas

# Test DuckDB trên bronze tier S
python - <<'EOF'
import duckdb
con = duckdb.connect()
result = con.execute("""
    SELECT COUNT(*) FROM read_parquet('data/lake/bronze/events/**/*.parquet')
""").fetchone()
print(f"Số dòng bronze: {result[0]:,}")
EOF
```

Đọc kỹ `src/analytics/rfm_kmeans.py` — hiểu từng bước.

#### Tuần 2 (W2): Chạy RFM + KMeans

```bash
make analytics TIER=M PIPELINE_CONFIG=/app/configs/pipeline-m5-tier-m.yaml
```

Kiểm tra cluster profiles:
```python
import pandas as pd, glob
files = glob.glob("results/tier_m/cluster_profiles/*.csv")
df = pd.concat([pd.read_csv(f) for f in files])
print(df.sort_values("median_monetary", ascending=False))
# Đặt tên cho từng cụm dựa trên profile
```

Kiểm tra ổn định:
```bash
# Chạy 3 lần với seed khác nhau
# Sửa pipeline.yaml: kmeans_seed: 0 → 42 → 123
# Xem cluster_profiles có tương tự không
```

#### Tuần 3 (W3): E5 và E6

```bash
# E5: Chạy bốn engine trên cùng cấu hình tier M
make experiment-e5 TIER=M PIPELINE_CONFIG=/app/configs/pipeline-m5-tier-m.yaml

# E6 đã kiểm chứng trên S và M; chạy lại sau Gold và analytics của tier tương ứng
make experiment-e6 TIER=M PIPELINE_CONFIG=/app/configs/pipeline-m5-tier-m.yaml
```

Ghi lại:
- Ở fraction nào Spark bắt đầu nhanh hơn DuckDB?
- Pandas có bị OOM (hết RAM) ở fraction nào không?
- E6 có lệch không? Nếu lệch → tìm nguyên nhân và commit fix

```bash
make plot-experiments
# Xem docs/figures/e5_crossover.png
```

#### Tuần 4 (W4): Báo cáo

- **Chương 2** (~3 trang): Lý thuyết — Spark RDD/DataFrame, lazy evaluation, shuffle, KMeans, silhouette coefficient
- **Chương 6 phần E5/E6** (~2 trang): Bảng crossover (engine × fraction × time), phân tích điểm giao, bảng đối chiếu oracle

Vẽ elbow + silhouette chart:
```python
# Thêm vào rfm_kmeans.py: log inertia và silhouette mỗi k
# Vẽ 2 subplots
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
ax1.plot(ks, inertias, marker="o")
ax1.set_title("Elbow Method")
ax1.set_xlabel("k")
ax1.set_ylabel("Inertia")

ax2.plot(ks, silhouettes, marker="s", color="orange")
ax2.set_title("Silhouette Score")
ax2.set_xlabel("k")
ax2.set_ylabel("Silhouette")
```

---

## Quy trình Git chung

> **Mọi người đều theo quy trình này.**

### Workflow mỗi tính năng

```bash
# 1. Cập nhật main trước khi làm việc mới
git checkout main
git pull

# 2. Tạo nhánh mới với tên mô tả
git checkout -b feat/m2-silver-r7-dedupe
#                  ↑     ↑      ↑
#               loại  người  việc

# 3. Làm việc, commit thường xuyên (mỗi bước nhỏ 1 commit)
git add src/processing/bronze_to_silver.py
git commit -m "feat(silver): implement R7 dedupe with dropDuplicates"

git add tests/test_cleaning_rules.py
git commit -m "test(silver): add test for R7 dedupe logic"

# 4. Push lên GitHub
git push -u origin feat/m2-silver-r7-dedupe

# 5. Tạo Pull Request trên GitHub
# → Điền title + description
# → Assign reviewer (người review module của mình)
# → Đợi review trước khi merge
```

### Mẫu commit message

```
feat(silver): implement R7 dedupe with sensitivity analysis
fix(gold): correct session duration for same-second events
test(kpi): add edge case for zero-purchase sessions
docs(adr): add ADR-004 dedupe decision with KPI impact
exp(e3): add format comparison results tier M
refactor(common): extract parquet write helper
```

Format: `type(scope): mô tả ngắn`
- `feat`: tính năng mới
- `fix`: sửa bug
- `test`: thêm/sửa test
- `docs`: tài liệu
- `exp`: kết quả thí nghiệm
- `refactor`: refactor không thay đổi behavior

### Mẫu Pull Request

```markdown
## Mục tiêu
Implement luật R7 (dedupe) trong bronze_to_silver.py

## Cách kiểm tra
```bash
make test
# test_cleaning_rules.py::test_r7_dedupe PASSED
```

## Số liệu (nếu có)
- Tier S: 1,234 dòng trùng (0.12%)
- KPI revenue thay đổi: -0.08% (sensitivity analysis)

## Ảnh hưởng đến contract
Không thay đổi schema silver

## Checklist
- [x] Có test
- [x] Có nhật ký (docs/log/m2-w2.md)
- [x] CI xanh
```

### Ghi nhật ký hằng ngày (3 dòng, vào `docs/log/<tên>-w<n>.md`)

```markdown
[2026-10-14]
Làm: chạy E3 tier M, Parquet snappy nhanh hơn CSV 3.2× ở Q2
Gặp: Q3 không dùng partition pruning vì filter trên cột tính toán (không phải partition key)
Cần: M3 xác nhận gold/agg_daily có event_date đúng format để test E3
```

---

## Lịch trình 6 tuần

```
         M1          M2          M3          M4          M5
W1   Cluster+CI   Profiling   Gold S     Analytics S  DuckDB test
     Bronze S     Bronze prof  Gold grain  KPI đọc     RFM đọc
     
W2   Bronze M     Silver M     Gold M      Biểu đồ     RFM+KMeans M
     E7 draft     DQ report    Session gap  Funnel     KMeans test
     
W3   E2 chạy      E3 chạy      E1+E4 chạy  Cohort      E5+E6 chạy
     Báo cáo C1   Báo cáo C3   Báo cáo C5  Báo cáo C7  Báo cáo C2
     
W4   ─────────────────── CODE FREEZE ───────────────────────────
     Báo cáo C4   Báo cáo C5   Báo cáo C6  Slide demo   C6 E5/E6
     Phụ lục      DQ section   Perf section Analytics   Lý thuyết
     
W5   README test  Review C3    Review C5   Review C7    Review C2
     CI kiểm tra  Video demo   Slide prep   Demo run     Crossover
     
W6   ─── Buffer: sửa theo phản hồi, đóng gói, nộp, bảo vệ ────
```

**Gate check cuối mỗi tuần (10 phút, mọi người cùng làm)**:
```bash
make all TIER=S   # phải xanh
make test         # phải xanh
# Nếu fail → tuần sau ưu tiên sửa gate, không làm tính năng mới
```

---

## Câu hỏi bảo vệ

> Đề yêu cầu **mọi thành viên** giải thích được toàn bộ pipeline.  
> Học câu trả lời dựa trên số liệu đo thực của nhóm, không học vẹt.

### Câu hỏi về kiến trúc

**"Tại sao không dùng Pandas?"**
> Trình bày đúng E5 đã đo: tier S, Pandas chunk nhanh nhất ở 5–50%, DuckDB nhanh nhất ở 100%; tier M, DuckDB nhanh nhất đến 50%, Spark Local nhanh nhất ở 100%. Điểm giao DuckDB/Spark Local chỉ được khoanh giữa 50% và 100% tier M trong phép đo truy vấn đã chuẩn bị mẫu. Chưa đo tier L/XL hoặc chứng minh Pandas hết RAM; nêu mục tiêu dùng Spark cho pipeline phân tán và giới hạn của benchmark hiện tại.

**"Cluster Docker của nhóm có thực sự phân tán không?"**
> Nói thẳng: Không. Tất cả container chạy trên cùng 1 máy vật lý, chia sẻ CPU/RAM/đĩa. E2 đo speedup nhưng không phản ánh tăng tốc phân tán thật sự. Đây là hạn chế, đã nêu trong mục 8 báo cáo.

**"Tại sao dùng Parquet thay vì CSV?"**
> Nêu 3 lợi ích: column pruning, partition pruning, nén tốt hơn. Dẫn số liệu từ E3: Parquet snappy nhanh hơn CSV [X]× ở Q2, kích thước nhỏ hơn [Y]%.

### Câu hỏi về Spark

**"Shuffle là gì, tại sao đắt?"**
> Shuffle = phân phối lại dữ liệu theo key qua mạng/đĩa để thực hiện groupBy/join. Đắt vì: (1) ghi dữ liệu trung gian ra đĩa, (2) đọc lại theo key mới, (3) network I/O giữa các executor. Chỉ ra shuffle bytes trong Spark UI của E1/E4.

**"Narrow vs wide transformation khác nhau thế nào?"**
> Narrow: filter, select, map — mỗi partition đầu ra thường chỉ cần một partition đầu vào. Wide: tổng hợp theo key có thể cần dữ liệu từ nhiều partition và tạo shuffle. Join có nhiều kế hoạch vật lý; broadcast join có thể tránh shuffle ở một phía.

**"Skew là gì, nhóm xử lý thế nào?"**
> Skew = một số key có lượng dữ liệu lớn hơn nhiều. Ví dụ product_id hot. Biểu hiện: 1 task chạy rất lâu trong Spark UI kéo cả stage. Nhóm xử lý bằng salting (E4 T4): thêm cột salt ngẫu nhiên vào key, group 2 lần. Nêu kết quả đo: task max giảm từ Xs xuống Ys.

**"Hiệu suất song song thấp hơn lý thuyết vì sao?"**
> Định luật Amdahl: phần tuần tự (driver planning, shuffle overhead) giới hạn speedup. Thêm vào đó: cluster Docker trên 1 máy → worker cạnh tranh CPU/RAM vật lý. E2 đo speedup thực tế [X]× với local[4], trong khi lý thuyết Amdahl dự đoán tối đa [Y]×.

### Câu hỏi về dữ liệu

**"Dedupe quyết định thế nào?"**
> Nêu quy tắc dedupe trong mã làm sạch và số dòng bị loại đã đo. E6 hiện chỉ đối chiếu ba KPI trên Silver sau làm sạch; chưa có sensitivity analysis trước/sau dedupe nên không được viện dẫn E6 để khẳng định phần trăm tác động doanh thu hoặc nguyên nhân tạo dòng trùng.

**"Doanh thu tính ra sao khi không có số lượng?"**
> Revenue = SUM(price) WHERE event_type='purchase'. Giả định số lượng = 1 vì dataset không có cột quantity. Đây là proxy, không phải revenue thật. AOV cũng là proxy. Đã ghi rõ giả định này trong báo cáo mục 3 và 8.

**"Kết quả đúng xác nhận bằng cách nào?"**
> E6 trên tier S và M tính ba KPI bằng DuckDB từ cùng Silver rồi so sánh với đầu ra Spark: cả ba đều đạt ngưỡng 0.1% ở mỗi tier. Conversion rate lệch 0.0392% (S) và 0.0177% (M) do Spark lưu bốn chữ số thập phân. Bất biến số dòng được đối chiếu riêng: S `1,060,438 = 1,060,392 + 46`, M `42,448,764 = 42,418,544 + 30,220`. Unit test kiểm tra các trường hợp mẫu; E6 chỉ xác nhận ba KPI đã nêu.

**"KMeans chọn k bằng cách nào?"**
> Pipeline thử $k=2..10$, ở mỗi $k$ chọn nghiệm có inertia thấp nhất trong ba seed rồi so sánh silhouette. Tier S có silhouette lớn nhất 0.835945 tại $k=2$, nhưng quy mô hai nhóm thay đổi mạnh giữa các seed; $k=4$ có profile tổng hợp gần nhau hơn dù silhouette chỉ 0.549776. Tier M có silhouette lớn nhất 0.580124 tại $k=2$, chia 79.098 và 268.020 người; ba lần fit riêng có quy mô gần nhau. Đường elbow không cho điểm uốn duy nhất rõ ràng; chưa kiểm tra mức trùng khớp thành viên cụm hay kiểm định nhãn nghiệp vụ.

---

*Tài liệu này được cập nhật khi có thay đổi. Mỗi người ghi nhật ký tiến độ vào `docs/log/<tên>-w<tuần>.md`.*
