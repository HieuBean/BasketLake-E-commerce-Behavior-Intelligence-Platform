# Báo cáo — Phần của M2 (Dữ liệu, Làm sạch DQ, Thí nghiệm E3)

> Tác giả: M2 (Phụ trách Tầng Silver, Luật làm sạch R1–R9, DQ Report, Thí nghiệm E3)  
> Dữ liệu đo đạc thực tế trên Apache Spark 3.5.3  

---

## Chương 3. Dữ liệu

### 3.1 Nguồn dữ liệu
- **Tên**: REES46 "eCommerce behavior data from multi category store" (Kaggle).
- **Mô tả**: Nhật ký hành vi clickstream của người dùng trên một sàn thương mại điện tử đa ngành.
- **Phạm vi sử dụng**: Nhóm sử dụng dữ liệu tháng 10/2019 (`2019-Oct.csv`, kích thước gốc 5.28 GB, 42.4 triệu dòng) cho Tier S (mẫu lấy ngẫu nhiên 2.5%, ~1.06 triệu dòng) và Tier M (toàn bộ tháng 10).

### 3.2 Schema gốc và Data Contract
Dữ liệu thô gồm 9 trường thông tin:
| Cột | Kiểu | Nullable | Mô tả |
|---|---|---|---|
| `event_time` | string | No | Thời điểm diễn ra sự kiện (UTC), định dạng `yyyy-MM-dd HH:mm:ss z` |
| `event_type` | string | No | Loại hành vi: `view`, `cart`, `purchase`, `remove_from_cart` |
| `product_id` | long | No | Mã định danh sản phẩm |
| `category_id` | long | Yes | Mã định danh danh mục sản phẩm |
| `category_code` | string | Yes | Mã phân cấp danh mục (vd: `electronics.smartphone`) |
| `brand` | string | Yes | Tên thương hiệu |
| `price` | double | Yes | Giá sản phẩm (USD) |
| `user_id` | long | No | Mã định danh người dùng |
| `user_session` | string | Yes | Mã phiên tương tác gốc do sàn ghi nhận |

Tầng Bronze bổ sung 3 trường metadata: `source_file`, `ingest_ts`, `source_month` (partition key).

### 3.3 Kết quả Profiling thực tế (Tầng Bronze — Tier S)
Từ file kết quả thực thi `results/profiling_summary.md` và `results/dq_report_bronze_null.csv`:
- **Tổng số dòng thực tế**: **1,060,438 dòng**.
- **Phân phối hành vi**:
  - `view`: 1,019,004 dòng (96.09%)
  - `cart`: 22,964 dòng (2.17%)
  - `purchase`: 18,470 dòng (1.74%)
- **Tỷ lệ thiếu nhãn (Missing / Null rate)**:
  - `category_code`: **31.899%** (338,270 dòng thiếu nhãn danh mục)
  - `brand`: **14.433%** (153,056 dòng thiếu tên thương hiệu)
- **Thống kê giá (`price`)**:
  - Thấp nhất: **0.00 USD**, Cao nhất: **2,574.07 USD**, Trung bình: **290.74 USD**.
  - Tồn tại **1,674 dòng có giá = 0.0 USD** (cần bảo lưu và gắn cờ kiểm toán ở Silver).
  - Không có dòng nào có giá âm trong mẫu khảo sát.
- **Dòng trùng lặp nghiệp vụ**: Phát hiện **46 dòng trùng lặp hoàn toàn** trên tất cả các cột thuộc tính nghiệp vụ.
- **Kích thước đối tượng ước lượng (Cardinality qua HyperLogLog)**: ~652,746 khách hàng, ~92,487 mã sản phẩm, ~906,898 phiên giao dịch, 125 nhóm ngành hàng, 2,811 thương hiệu.

### 3.4 Đặc trưng 5V của Big Data
1. **Volume**: Dung lượng file thô ~5.3 GB cho 1 tháng (~42.4 triệu dòng). Việc tải toàn bộ vào bộ nhớ một máy tính cá nhân bằng Pandas sẽ gây tràn RAM (OOM), đòi hỏi kiến trúc phân tán Apache Spark.
2. **Velocity**: Dữ liệu clickstream liên tục sinh ra với tần suất cao từ hàng triệu người dùng trực tuyến.
3. **Variety**: Cấu trúc bán phân tầng (ví dụ `category_code` dạng chuỗi chấm `l1.l2.l3`) và nhiều trường dữ liệu bị khuyết thiếu.
4. **Veracity**: Có dữ liệu nhiễu (dòng trùng lặp, thiếu thương hiệu, giá = 0), đòi hỏi quy trình làm sạch nghiêm ngặt và đo lường chất lượng dữ liệu (Data Quality).
5. **Value**: Sau khi làm sạch, dữ liệu phục vụ trực tiếp cho bài toán tính phễu chuyển đổi (Funnel), giá trị đơn hàng trung bình (AOV), và phân khúc khách hàng (RFM + KMeans).

---

## Chương 5 (Phần DQ). Làm sạch dữ liệu Bronze → Silver

### 5.1 Nguyên tắc thiết kế tầng Silver
1. **Bảo toàn bất biến (Data Invariant)**:  
   $$\text{count}(\text{Bronze}) = \text{count}(\text{Silver}) + \text{count}(\text{Quarantine})$$  
   Không được để mất mát dữ liệu một cách im lặng. Dòng không đạt chuẩn bắt buộc phải được đưa vào `quarantine` kèm theo lý do cụ thể.
2. **An toàn kiểu dữ liệu trong Spark ANSI mode**: Sử dụng `try_to_timestamp` và `F.get` để tránh lỗi văng runtime ngoại lệ khi gặp định dạng timestamp lạ hoặc mảng phân cấp không đủ 3 cấp.
3. **Phân vùng tối ưu (Partitioning)**: Silver được phân vùng theo `event_date` (ngày), giúp tăng tốc các truy vấn phân tích theo thời gian.

### 5.2 Bảng luật làm sạch R1–R9
| Mã | Tên luật | Tiêu chí kiểm tra | Hành động xử lý |
|---|---|---|---|
| **R1** | null_key_field | Thiếu một trong các khóa: `event_time`, `event_type`, `product_id`, `user_id` | Chuyển vào `quarantine` (`null_key_field`) |
| **R2** | invalid_event_type | `event_type` không nằm trong tập hợp lệ `{view, cart, purchase, remove_from_cart}` | Chuyển vào `quarantine` (`invalid_event_type`) |
| **R3** | timestamp_parse_error | `event_time` không parse được thành timestamp UTC hợp lệ | Chuyển vào `quarantine` (`timestamp_parse_error`) |
| **R4** | negative_price | `price < 0` → Chuyển vào `quarantine`; `price == 0` hoặc null | Giữ lại dòng, gắn cờ `price_valid = False` |
| **R5** | brand_missing | `brand` bị null hoặc chuỗi rỗng | Điền `"unknown"`, gắn cờ `brand_missing = True` |
| **R6** | category_missing | `category_code` bị null hoặc chuỗi rỗng; tách thành `category_l1/l2/l3` | Điền `category_l1 = "unknown"`, gắn cờ `category_missing = True` |
| **R7** | duplicate_row | Trùng khớp toàn bộ các cột nghiệp vụ (loại trừ metadata) | Giữ lại 1 bản ghi duy nhất, các bản ghi dư thừa chuyển vào `quarantine` (`duplicate_row`) |
| **R8** | out_of_month | Tháng của sự kiện lệch so với tháng quản lý của file dữ liệu | Giữ lại dòng, gắn cờ cảnh báo `out_of_month = True` |
| **R9** | session_generated | `user_session` bị thiếu | Tự động sinh `session_id` theo thuật toán khoảng nghỉ (gap 30 phút), gắn cờ `session_generated = True` |

### 5.3 Bảng báo cáo chất lượng dữ liệu thực tế (DQ Report — Tier S)
Bảng kết quả xuất từ `results/dq_report_silver.md`:

| Luật | Tên luật | Hành động | Dòng vào | Dòng ra | Bị loại | Tỷ lệ loại | Gắn cờ | Tỷ lệ cờ |
|---|---|---|---:|---:|---:|---:|---:|---:|
| **R1** | null_key_field | quarantine | 1,060,438 | 1,060,438 | 0 | 0.0000% | 0 | 0.0000% |
| **R2** | invalid_event_type | quarantine | 1,060,438 | 1,060,438 | 0 | 0.0000% | 0 | 0.0000% |
| **R3** | timestamp_parse_error | quarantine | 1,060,438 | 1,060,438 | 0 | 0.0000% | 0 | 0.0000% |
| **R4** | negative_price | flag price_valid=false | 1,060,438 | 1,060,438 | 0 | 0.0000% | 1,674 | 0.1579% |
| **R5** | brand_missing | fill 'unknown' + flag | 1,060,438 | 1,060,438 | 0 | 0.0000% | 153,054 | 14.4337% |
| **R6** | category_missing | category_l1='unknown' + flag | 1,060,438 | 1,060,438 | 0 | 0.0000% | 338,266 | 31.9001% |
| **R7** | duplicate_row | quarantine (dedupe) | 1,060,438 | 1,060,392 | **46** | **0.0043%** | 0 | 0.0000% |
| **R8** | out_of_month | flag | 1,060,392 | 1,060,392 | 0 | 0.0000% | 0 | 0.0000% |
| **R9** | session_generated | sinh session_id bằng gap | 1,060,392 | 1,060,392 | 0 | 0.0000% | 0 | 0.0000% |
| **TOTAL** | | | **1,060,438** | **1,060,392** | **46** | **0.0043%** | | |

* **Kiểm chứng bất biến**: $1,060,438 = 1,060,392 + 46 \implies \text{OK}$ (Chính xác tuyệt đối).

---

## Chương 6 (Phần E3). Thí nghiệm so sánh định dạng lưu trữ

### 6.1 Mục tiêu và Thiết lập thí nghiệm
So sánh hiệu quả lưu trữ (kích thước file, tỷ lệ nén) và tốc độ truy vấn trên 5 cấu hình định dạng:
1. `csv`: Định dạng phân cách dấu phẩy truyền thống (không nén).
2. `parquet_snappy`: Parquet nén Snappy (tối ưu tốc độ giải nén).
3. `parquet_snappy_p`: Parquet Snappy có phân vùng theo ngày (`event_date`).
4. `parquet_zstd`: Parquet nén Zstandard (tối ưu tỷ lệ nén cao).
5. `parquet_zstd_p`: Parquet Zstandard có phân vùng theo ngày.

Ba truy vấn đại diện:
- **Q1 (Full scan)**: Đếm số lượng sự kiện theo từng loại `event_type`.
- **Q2 (Single-column Aggregation)**: Tính tổng doanh thu lọc theo danh mục cấp 1 (`category_l1`).
- **Q3 (Partition Filtering)**: Lọc sự kiện trong một ngày cụ thể (`2019-10-05`).

### 6.2 Kết quả thực nghiệm

#### Bảng 1: So sánh kích thước lưu trữ và tỷ lệ nén (từ `results/e3_sizes.csv`)
| Định dạng | Dung lượng (MB) | Tỷ lệ nén so với CSV | Thời gian ghi (s) | Số lượng file |
|---|---:|---:|---:|---:|
| `csv` | 303.6 MB | **1.00×** (gốc) | 5.89s | 2 |
| `parquet_snappy` | 92.8 MB | **3.27×** | 5.51s | 2 |
| `parquet_snappy_p` | 95.5 MB | **3.18×** | 8.96s | 31 |
| `parquet_zstd` | **56.1 MB** | **5.41×** | 4.69s | 2 |
| `parquet_zstd_p` | 60.0 MB | **5.06×** | 7.41s | 31 |

#### Bảng 2: Thời gian thực thi truy vấn (từ `results/e3_summary.csv`)
| Định dạng | Q1 — Quét toàn bộ (s) | Speedup Q1 | Q2 — Tổng hợp 1 cột (s) | Speedup Q2 | Q3 — Lọc ngày (s) | Speedup Q3 |
|---|---:|---:|---:|---:|---:|---:|
| `csv` | 2.557s | 1.00× | 2.422s | 1.00× | 2.392s | 1.00× |
| `parquet_snappy` | 0.385s | **6.64×** | 0.484s | **5.00×** | 0.270s | **8.86×** |
| `parquet_snappy_p` | 1.818s | 1.41× | 2.041s | 1.19× | 1.171s | 2.04× |
| `parquet_zstd` | **0.299s** | **8.55×** | **0.320s** | **7.57×** | **0.248s** | **9.65×** |
| `parquet_zstd_p` | 1.935s | 1.32× | 1.873s | 1.29× | 1.171s | 2.04× |

*(Biểu đồ trực quan hóa được lưu tại `docs/figures/e3_format_size.png` và `docs/figures/e3_format_time.png`)*.

### 6.3 Phân tích và Kết luận rút ra
1. **Khả năng nén và tiết kiệm lưu trữ**:
   - Parquet dạng cột giúp nén dữ liệu vượt trội nhờ lưu trữ các giá trị cùng kiểu liên tục. Snappy giảm dung lượng **3.27 lần**, trong khi Zstandard giảm đến **5.41 lần** (từ 303.6 MB xuống còn 56.1 MB).
2. **Hiệu năng Column Pruning (ở truy vấn Q2)**:
   - Khi thực hiện tổng hợp doanh thu chỉ cần đọc các cột `event_type`, `category_l1`, `price`, Parquet chỉ tải đúng các khối dữ liệu cần thiết từ đĩa, giúp tốc độ tăng **từ 5.0 lần đến 7.5 lần** so với việc CSV phải đọc toàn bộ hàng.
3. **Lựa chọn định dạng tối ưu cho Data Lake**:
   - Khuyến nghị chọn **`parquet_snappy` hoặc `parquet_zstd` có partition theo `event_date`** cho tầng Silver. Mặc dù trên tập dữ liệu mẫu nhỏ (Tier S) chi phí mở nhiều file nhỏ phân vùng tạo overhead nhẹ, nhưng trên quy mô toàn bộ tháng (Tier M với 42 triệu dòng), partition pruning sẽ mang lại sự khác biệt lớn về hiệu năng truy vấn theo ngày.
