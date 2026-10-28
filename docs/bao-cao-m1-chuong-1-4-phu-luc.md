# BÁO CÁO DỰ ÁN DỮ LIỆU LỚN: HỆ THỐNG PHÂN TÍCH HÀNH VI KHÁCH HÀNG E-COMMERCE
## PHẦN ĐẢM NHẬN: M1 — HẠ TẦNG, INGESTION (BRONZE) & THÍ NGHIỆM TÀI NGUYÊN (E2, E7)
**Sinh viên thực hiện**: M1 (Hiếu)  
**Môn học**: Nhập môn dữ liệu lớn (841450) — Trường Đại học Sài Gòn  
**Dataset**: REES46 eCommerce behavior data from multi category store (Kaggle)

---

# CHƯƠNG 1: GIỚI THIỆU VÀ BỐI CẢNH DỰ ÁN

## 1.1 Bối cảnh bài toán và Tập dữ liệu

Trong kỷ nguyên số, các nền tảng thương mại điện tử (e-Commerce) ghi nhận hàng chục triệu tương tác (clickstream events) mỗi ngày từ người dùng, bao gồm xem sản phẩm (`view`), thêm vào giỏ hàng (`cart`), xóa khỏi giỏ (`remove_from_cart`) và thanh toán (`purchase`). Việc khai thác khối dữ liệu khổng lồ này giúp doanh nghiệp tối ưu trải nghiệm khách hàng, xây dựng phễu chuyển đổi (conversion funnel), phân khúc khách hàng (RFM Segmentation) và đề xuất sản phẩm cá nhân hóa.

Tuy nhiên, xử lý luồng dữ liệu clickstream đặt ra các thách thức lớn về mặt kỹ thuật:
- **Khối lượng dữ liệu (Volume)**: File thô tháng 10/2019 (`2019-Oct.csv`) có hơn 42.4 triệu dòng (~5.67 GB thô), và cả hai tháng 10–11 vượt 109.9 triệu dòng (~14.4 GB). Dữ liệu này vượt quá khả năng lưu trữ trên bộ nhớ RAM của các công cụ xử lý đơn luồng truyền thống như Pandas trên máy cá nhân phổ thông (thường gây lỗi `MemoryError` hoặc OOM).
- **Chất lượng dữ liệu (Veracity)**: Dữ liệu clickstream thô từ thực tế chứa nhiều nhiễu: bản ghi trùng lặp (duplicates), giá trị `null` ở các trường danh mục/thương hiệu, sự kiện bất thường (`price <= 0`), định dạng thời gian cần chuẩn hóa UTC.
- **Tốc độ và Chi phí tính toán (Velocity & Performance)**: Các phép tính gộp nhóm (`groupBy`), kết nối bảng (`join`), và phân tách phiên người dùng (sessionization) đòi hỏi xáo trộn dữ liệu (Shuffle) trên mạng và đĩa, dẫn đến nghẽn thắt cổ chai (bottleneck).

Nhóm lựa chọn tập dữ liệu thực tế **REES46 eCommerce behavior data from multi-category store**, ghi nhận toàn bộ hành vi người dùng trong tháng 10 và tháng 11 năm 2019. Mỗi sự kiện bao gồm 9 trường thông tin cơ bản: `event_time`, `event_type`, `product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`, `user_session`.

## 1.2 Mục tiêu và Phạm vi dự án

### Mục tiêu kỹ thuật:
1. Xây dựng một **Data Lakehouse** hoàn chỉnh theo kiến trúc Medallion (Bronze → Silver → Gold) trên nền tảng **Apache Spark 3.5**, triển khai bằng **Docker Compose** với 1 Master và 2 Workers.
2. Thiết kế cơ chế phân loại dữ liệu lỗi sang bảng cách ly (**Quarantine**) kèm mã lỗi rõ ràng (`reject_reason`), bảo toàn định luật bất biến dữ liệu: `count(Bronze) = count(Silver) + count(Quarantine)`.
3. Đo lường, kiểm chứng thực nghiệm các nguyên lý phân tán dữ liệu: Định luật Amdahl về khả năng tăng tốc khi mở rộng tài nguyên (Thí nghiệm E2), và bản chất xáo trộn dữ liệu của mô hình MapReduce kinh điển có/không có Combiner (Thí nghiệm E7).
4. Phân tích kinh doanh: Tính toán các KPI thương mại điện tử (Doanh thu, AOV, Tỷ lệ chuyển đổi), phân tích phễu hành vi, phân khúc khách hàng RFM kết hợp học máy không giám sát K-Means.

### Phạm vi dự án:
- Phân tầng xử lý chia thành các Tier dữ liệu để phục vụ kiểm thử và thí nghiệm:
  - **Tier S (Small)**: Lấy mẫu 2.5% từ tháng 10 (~1 triệu dòng) — phục vụ phát triển nhanh, unit test CI.
  - **Tier M (Medium)**: Toàn bộ tháng 10/2019 (~42.4 triệu dòng, 5.67 GB).
  - **Tier L (Large)**: Hai tháng 10 và 11/2019 (~109.9 triệu dòng, 14.4 GB).
  - **Tier XL (Extra Large)**: Dữ liệu synthetic khuếch đại (amplify) 5× có biến đổi ID nhằm kiểm thử ngưỡng chịu tải và cơ chế disk-spill của Spark.

## 1.3 Ba câu hỏi kỹ thuật cốt lõi (Technical Research Questions)

Để định hướng các thí nghiệm khoa học, M1 cùng nhóm xác lập 3 câu hỏi kỹ thuật:

### Câu hỏi 1: Giới hạn mở rộng tài nguyên (Resource Scalability) theo Định luật Amdahl
> *"Khi tăng số lượng CPU cores/Workers từ 1 lên 2, 4 cores hoặc phân cụm, tốc độ xử lý tác vụ phân tích clickstream (Aggregation & GroupBy) tăng theo tỷ lệ nào? Yếu tố nào đóng vai trò điểm nghẽn tuần tự (sequential bottleneck) ngăn cản speedup tuyến tính?"*
- **Mục tiêu**: Kiểm chứng Định luật Amdahl: $Speedup(n) = \frac{1}{s + \frac{1-s}{n}}$, phân tích tỷ lệ phần tuần tự $s$ (driver DAG generation, metadata scanning, I/O disk) và tác động của Shuffle I/O.

### Câu hỏi 2: Chi phí Shuffle trong mô hình MapReduce và vai trò của Combiner
> *"Trong bài toán tính tổng doanh thu theo ngành hàng cấp 1 (`category_l1`), cơ chế tổng hợp cục bộ (In-mapper Combiner) giúp giảm thiểu bao nhiêu phần trăm số cặp khóa-giá trị trung gian trước pha Shuffle, và tác động ra sao đến thời gian hoàn thành tác vụ so với cơ chế không có Combiner?"*
- **Mục tiêu**: Mô phỏng giải thuật MapReduce nguyên bản trên Python/PyArrow để thấy rõ số lượng bản ghi trung gian (intermediate records) phát sinh qua mạng/đĩa, liên hệ trực tiếp với cơ chế Map-side Aggregation của Spark DataFrame.

### Câu hỏi 3: Đảm bảo toàn vẹn dữ liệu qua cơ chế Data Contract & Quarantine
> *"Làm thế nào để pipeline xử lý dữ liệu lớn không bị gián đoạn (crash) bởi dữ liệu bẩn (dirty data) theo chế độ ANSI-strict của Spark 4, đồng thời vẫn bảo toàn 100% dữ liệu để phục vụ kiểm toán (Data Auditability)?"*
- **Mục tiêu**: Thiết kế schema tường minh (explicit schema), kỹ thuật `try_to_timestamp`, và bảng cách ly Quarantine lưu trữ các bản ghi vi phạm luật R1–R4 mà không làm mất mát thông tin.

---

# CHƯƠNG 4: THIẾT KẾ HỆ THỐNG VÀ KIẾN TRÚC PIPELINE

## 4.1 Sơ đồ kiến trúc tổng thể

Hệ thống được thiết kế theo kiến trúc Lakehouse hiện đại phân tầng Medallion Architecture, toàn bộ vận hành trong môi trường container hóa (Docker):

```mermaid
graph TD
    subgraph Storage ["Tầng Lưu Trữ (Local Lakehouse File System)"]
        Raw["data/raw/*.csv<br/>(Dữ liệu thô từ Kaggle)"]
        Bronze["data/lake/bronze/events<br/>(Parquet, partition by source_month)"]
        Silver["data/lake/silver/events<br/>(Parquet sạch, partition by event_date)"]
        Quarantine["data/lake/quarantine/events<br/>(Parquet lỗi + reject_reason)"]
        Gold["data/lake/gold/<br/>(fact_sessions, dim_product, dim_user, agg_*)"]
        Results["results/ & docs/figures/<br/>(CSV báo cáo & biểu đồ)"]
    end

    subgraph Compute ["Tầng Tính Toán (Apache Spark Standalone Cluster)"]
        Master["spark-master<br/>(Driver / Cluster Manager: 8080, 7077)"]
        Worker1["spark-worker-1<br/>(2 Cores, 2GB RAM)"]
        Worker2["spark-worker-2<br/>(2 Cores, 2GB RAM)"]
        History["spark-history<br/>(Event Logs UI: 18080)"]
    end

    Raw -->|src/prepare_input/main.py (M1)| Bronze
    Bronze -->|src/processing/bronze_to_silver.py (M2)| Silver
    Bronze -->|Bản ghi vi phạm R1-R4| Quarantine
    Silver -->|src/processing/silver_to_gold.py (M3)| Gold
    Gold -->|src/analytics/run_analytics.py (M4, M5)| Results

    Master --- Worker1
    Master --- Worker2
    Master -.-> History
```

### Chi tiết phân bổ cấu hình Docker:
- **Network**: `docker_default` (Bridge network nội bộ cho phép Master và các Worker phân giải DNS lẫn nhau).
- **Volume Mount**: Cấu hình Bind Mount toàn bộ repository `./` vào container tại `/app`, đảm bảo tính thống nhất dữ liệu giữa host Windows và hệ thống Linux trong container.
- **Log Persist**: Named volume `spark-events` mount vào `/opt/spark-events` để Spark History Server đọc lại DAG và metrics sau khi job kết thúc.

## 4.2 Bảng lựa chọn công nghệ và Luận cứ kỹ thuật

| Thành phần | Công nghệ lựa chọn | Lý do kỹ thuật & Đánh giá thay thế |
|------------|-------------------|------------------------------------|
| **Compute Core** | **Apache Spark 3.5.3 (PySpark)** | - Hỗ trợ xử lý phân tán dữ liệu lớn vượt ngưỡng RAM máy chủ vật lý qua cơ chế Spill to Disk.<br/>- Trình tối ưu hóa Catalyst Optimizer và bộ sinh mã Tungsten.<br/>- Thay thế Pandas (bị lỗi OOM ở Tier L/XL). |
| **Containerization** | **Docker & Docker Compose v2** | - Đóng gói môi trường Spark Standalone (1 Master + 2 Workers) đồng nhất giữa 5 thành viên trong nhóm.<br/>- Độc lập với hệ điều hành của host (Windows/WSL2/macOS/Linux). |
| **Storage Format** | **Apache Parquet (Snappy)** | - Lưu trữ dạng cột (Columnar format) hỗ trợ Column Pruning (chỉ đọc các cột cần thiết).<br/>- Nén Snappy cân bằng giữa tỷ lệ nén (~3.5× so với CSV) và tốc độ giải nén cao.<br/>- Tích hợp sẵn Metadata (min/max stats per row group) hỗ trợ Data Skipping. |
| **Data Partitioning** | **source_month & event_date** | - Tầng Bronze phân vùng theo `source_month` (đúng cấu trúc nhập liệu định kỳ).<br/>- Tầng Silver phân vùng theo `event_date` giúp truy vấn báo cáo theo ngày không phải scan toàn bộ dataset (Partition Pruning). |
| **Baseline & Validation** | **DuckDB & Pandas** | - Dùng DuckDB làm "Ground Truth" (Oracle) chạy trên Tier S/M để đối chiếu kết quả KPI với Spark (E6), đảm bảo tính đúng đắn tuyệt đối của mã nguồn. |
| **Quản lý thực thi** | **GNU Make (Makefile)** | - Cung cấp giao diện dòng lệnh tiêu chuẩn (`make bronze`, `make silver`, `make test`, `make all`) giúp đơn giản hóa quy trình tương tác với cluster. |

## 4.3 Thiết kế Phân tầng Dữ liệu Medallion

### 1. Tầng Bronze (`data/lake/bronze/events`)
- **Nhiệm vụ**: Lưu trữ bản sao trung thực của dữ liệu thô ở định dạng chuẩn Parquet.
- **Quy tắc**: Ép kiểu schema tường minh theo `RAW_SCHEMA`, tuyệt đối **không dùng `inferSchema=True`** để tránh 2 lần quét dữ liệu và lỗi suy diễn sai kiểu.
- **Làm giàu trường kỹ thuật**:
  - `source_file`: Đường dẫn file CSV nguồn (dùng `input_file_name()`).
  - `ingest_ts`: Dấu thời gian hệ thống nạp dữ liệu vào hồ (`current_timestamp()`).
  - `source_month`: Trích xuất từ tên file (`2019-10`) phục vụ partitioning.

### 2. Tầng Silver (`data/lake/silver/events`) & Bảng Quarantine
- **Nhiệm vụ**: Làm sạch dữ liệu, áp dụng 9 luật nghiệp vụ (R1–R9).
- **Bất biến cách ly**:
  $$\text{Dòng vào (Bronze)} = \text{Dòng sạch (Silver)} + \text{Dòng cách ly (Quarantine)}$$
- Khi bản ghi vi phạm luật R1 (null key fields: `event_time`, `user_id`, `product_id`, `event_type`), R2 (loại sự kiện không hợp lệ), R3 (timestamp lỗi), hoặc R4 (`price < 0`), bản ghi sẽ được định tuyến sang bảng `quarantine` với trường `reject_reason` để đội ngũ kỹ thuật phân tích nguyên nhân gốc.

### 3. Tầng Gold (`data/lake/gold/`)
- **Nhiệm vụ**: Phục vụ phân tích nghiệp vụ và máy học (Business-level aggregates).
- Bao gồm các bảng chiều và bảng sự kiện tổng hợp: `fact_sessions`, `dim_product`, `dim_user`, `agg_daily`, `agg_hourly`, `agg_funnel`.

## 4.4 Các Quyết định Kiến trúc Trọng yếu (ADR Summary)

### ADR 001: Sử dụng Apache Spark Standalone thay vì xử lý Single-node (DuckDB/Pandas)
- **Bối cảnh**: Tập dữ liệu 2 tháng đạt gần 110 triệu dòng. Pandas xử lý tốt ở quy mô <1 triệu dòng nhưng đổ vỡ khi dữ liệu vượt RAM.
- **Quyết định**: Chọn PySpark trên Docker cluster làm nền tảng xử lý dữ liệu chính, sử dụng DuckDB làm bộ kiểm chứng tính đúng đắn (Validation Oracle).
- **Hệ quả**: Chấp nhận overhead khởi tạo JVM và shuffle ở quy mô nhỏ, nhưng đảm bảo khả năng mở rộng (scalability) không giới hạn lên quy mô hàng trăm GB.

### ADR 002: Chuẩn hóa định dạng lưu trữ nội bộ bằng Apache Parquet Snappy
- **Bối cảnh**: File CSV tốn dung lượng đĩa (14.4 GB cho 2 tháng), không lưu trữ thông tin kiểu dữ liệu và bắt buộc phải scan toàn bộ dòng khi truy vấn dù chỉ cần 2–3 cột.
- **Quyết định**: Chuyển đổi toàn bộ dữ liệu CSV thô sang Parquet Snappy ngay tại tầng Bronze.
- **Hệ quả**: Tiết kiệm ~65% dung lượng đĩa, tốc độ truy vấn ở các bước phân tích sau tăng từ 3× đến 8× nhờ Column Pruning và Dictionary Encoding.

### ADR 003: Lưu trữ Data Lake trên Hệ thống Tệp Cục bộ qua Volume Mount (thay vì MinIO)
- **Bối cảnh**: Đề bài ban đầu cân nhắc sử dụng MinIO để giả lập AWS S3 Object Storage.
- **Quyết định**: Lưu trữ trực tiếp trên Local File System của host máy tính và mount vào container tại `/app/data/lake`.
- **Lý do**:
  1. Loại bỏ chi phí mạng (Network overhead) và giao thức HTTP/S3 multipart upload trên máy cá nhân của sinh viên.
  2. Giảm thiểu tài nguyên tiêu hao (RAM/CPU) của container MinIO, nhường tài nguyên tối đa cho Spark Worker JVM.
  3. Dễ dàng kiểm tra, inspect dữ liệu trực tiếp từ máy host bằng các công cụ như VS Code, DuckDB CLI.

---

# PHỤ LỤC: HƯỚNG DẪN VẬN HÀNH VÀ MA TRẬN ĐÓNG GÓP

## P.1 Hướng dẫn cài đặt và vận hành hệ thống

### 1. Chuẩn bị môi trường
- Cài đặt **Docker Desktop** (bật WSL2 trên Windows, cấp phát RAM tối thiểu 6GB).
- Tải file dữ liệu Kaggle `2019-Oct.csv` và đặt vào thư mục `data/raw/2019-Oct.csv`.

### 2. Khởi động Cụm Spark Cluster
```bash
# Khởi động cluster ngầm
docker compose -f docker/docker-compose.yml up -d

# Kiểm tra trạng thái các container (phải thấy master, worker-1, worker-2, history)
docker compose -f docker/docker-compose.yml ps

# Truy cập giao diện trực quan:
# - Spark Master UI:  http://localhost:8080 (Kiểm tra 2 Workers đã kết nối)
# - History Server:   http://localhost:18080
```

### 3. Thực thi Pipeline xử lý dữ liệu
```bash
# Chạy Ingestion tầng Bronze (Tier S - Lấy mẫu nhanh)
make bronze TIER=S

# Chạy Ingestion tầng Bronze với dữ liệu đầy đủ (Tier M - Tháng 10)
make bronze TIER=M

# Chạy làm sạch tầng Silver
make silver TIER=M

# Chạy toàn bộ pipeline kiểm thử
make all TIER=S
```

### 4. Chạy Thí nghiệm đo lường hiệu năng
```bash
# Chạy thí nghiệm E2 (Mở rộng tài nguyên Cores/Workers)
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit /app/experiments/e2_scale_resource.py --tier M

# Chạy thí nghiệm E7 (MapReduce mô phỏng Combiner)
docker compose -f docker/docker-compose.yml exec spark-master \
  python3 /app/experiments/e7_mapreduce.py

# Xem bảng kết quả đo lường thời gian
cat results/experiment_log.csv
```

## P.2 Cấu trúc thư mục dự án

```
bigdata-ecom/
├── configs/
│   ├── data-contract.yaml          # Hợp đồng dữ liệu, schema, quy tắc R1-R9
│   └── pipeline.yaml               # Tham số cấu hình đường dẫn và các Tier
├── docker/
│   ├── docker-compose.yml          # Định nghĩa cụm Spark 1 Master + 2 Workers
│   └── spark-defaults.conf         # Cấu hình tối ưu bộ nhớ, AQE, shuffle
├── docs/
│   ├── adr/                        # Các bản ghi quyết định kiến trúc (001-003)
│   ├── dataset-facts.md            # Bảng số liệu đo đạc thực tế của tập dữ liệu
│   ├── phan-cong-nhom.md           # Kế hoạch phân công và checklist chi tiết
│   └── bao-cao-m1-chuong-1-4.md    # Tài liệu báo cáo phần M1
├── experiments/
│   ├── harness.py                  # Framework đo thời gian 3 lần lặp (median)
│   ├── e2_scale_resource.py        # Thí nghiệm E2 (Amdahl Speedup)
│   └── e7_mapreduce.py             # Thí nghiệm E7 (MapReduce Combiner)
├── src/
│   ├── common/                     # Module dùng chung (Config, Logger, Metrics)
│   ├── prepare_input/              # [M1 phụ trách] Ingestion Bronze & Profiling
│   ├── processing/                 # Làm sạch Silver và tổng hợp Gold
│   └── analytics/                  # KPI, RFM, K-Means Clustering
├── tests/                          # Bộ kiểm thử tự động pytest
├── Makefile                        # Tự động hóa các tác vụ với Make
└── README.md                       # Hướng dẫn tổng quan dự án
```

## P.3 Bảng tổng hợp đóng góp của M1 (Hiếu)

| Hạng mục | Trách nhiệm | File triển khai chính | Trạng thái |
|----------|-------------|-----------------------|------------|
| **Hạ tầng Docker** | Thiết lập cụm Spark 3.5 Standalone (Master, 2 Workers, History Server) | `docker/docker-compose.yml`, `docker/spark-defaults.conf`, `Makefile` | Hoàn thành |
| **Ingestion Bronze** | Đọc CSV thô, trích xuất metadata, mapping tên file linh hoạt (`2019-Oct` & `2019-10`), phân vùng Parquet | `src/prepare_input/main.py`, `src/prepare_input/profiling.py` | Hoàn thành |
| **Thí nghiệm E2** | Đo tốc độ thực thi với các cấu hình CPU: `local[1]`, `local[2]`, `local[4]`, cluster 2 workers | `experiments/e2_scale_resource.py` | Sẵn sàng chạy |
| **Thí nghiệm E7** | Mô phỏng thuật toán MapReduce kinh điển, so sánh cặp intermediate có/không Combiner | `experiments/e7_mapreduce.py` | Sẵn sàng chạy |
| **Kiểm thử tự động** | Viết unit test cho schema Bronze, cơ chế sampling Tier S và amplify Tier XL | `tests/test_prepare_input.py`, `tests/test_invariants.py` | Hoàn thành |
| **Báo cáo đồ án** | Biên soạn Chương 1 (Bối cảnh, 3 câu hỏi kỹ thuật), Chương 4 (Kiến trúc, ADR), Phụ lục kỹ thuật | `docs/bao-cao-m1-chuong-1-4-phu-luc.md` | Hoàn thành |
