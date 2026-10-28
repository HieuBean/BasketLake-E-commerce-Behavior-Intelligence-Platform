# Nhật Ký Tuần 1 (W1) — Thiết Lập Hạ Tầng & Ingestion Bronze

**Thành viên**: Hiếu (M1)  
**Vai trò**: Hạ tầng Docker, Bronze Ingestion, Thí nghiệm E2 & E7  
**Thời gian**: Tuần 1

---

## [QĐ] Cấu hình Docker Compose cho cụm Spark 3.5 Standalone

**Vấn đề**:
Khởi tạo cụm Spark gồm 1 Master, 2 Workers và 1 History Server. Cần mount dữ liệu và tệp cấu hình đồng nhất giữa host và container.

**Lựa chọn xem xét**:
1. Dùng image `bitnami/spark:3.5.3`
2. Dùng image chính thức `apache/spark:3.5.3`

**Quyết định**:
Chọn `apache/spark:3.5.3` vì Bitnami đã chuyển chính sách phân phối một số image Spark cũ sang kho lưu trữ riêng, trong khi image chính thức của Apache Spark hoạt động ổn định và có sẵn trên Docker Hub.

**Chi tiết cấu hình**:
- `spark-master`: cổng UI 8080, cluster port 7077.
- `spark-worker-1` & `spark-worker-2`: mỗi worker cấp 2 Cores, 2GB RAM.
- `spark-history`: cổng UI 18080.
- Volume: bind mount `../:/app` để chia sẻ toàn bộ code và data lake.

---

## [QĐ] Chuẩn hóa tên file CSV nguồn và trích xuất source_month

**Vấn đề**:
File tải từ Kaggle mang tên `2019-Oct.csv` trong khi pipeline ban đầu cấu hình tên định dạng số `2019-10.csv`.

**Quyết định**:
Nâng cấp hàm `read_csv` trong `src/prepare_input/main.py`:
1. Tự động kiểm tra file tồn tại ở cả 2 định dạng: `2019-Oct.csv` và `2019-10.csv`.
2. Trích xuất tên tháng (kể cả chữ viết tắt `Oct`, `Nov` hoặc số `10`, `11`) và chuẩn hóa về dạng `YYYY-MM` (ví dụ `2019-10`) làm cột phân vùng `source_month`.

---

## [Checklist hoàn thành W1]
- [x] Soạn thảo và kiểm tra `docker-compose.yml`, `spark-defaults.conf`.
- [x] Viết logic nạp dữ liệu tầng Bronze với schema tường minh (không dùng inferSchema).
- [x] Hỗ trợ sampling Tier S (fraction 0.025) và amplify Tier XL.
- [x] Tạo unit test `tests/test_prepare_input.py`.
