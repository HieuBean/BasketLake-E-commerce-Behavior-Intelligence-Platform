# ADR-001: Spark vs công cụ một máy (Pandas / DuckDB)

Trạng thái: **chấp nhận** | Ngày: 07/10/2026 | Người quyết: Nhóm (Ngày 0)

## Bối cảnh

Dataset REES46 có nhiều tháng, mỗi tháng ~9 GB CSV (~42 triệu dòng).
Môn học yêu cầu minh chứng nhu cầu Big Data. Cần chọn engine xử lý chính.

## Các lựa chọn

| | Pandas | DuckDB | Spark |
|---|---|---|---|
| Giới hạn RAM | Hết RAM với file lớn | Vượt RAM tốt | Spill ra đĩa |
| Tốc độ nhỏ | Nhanh | Rất nhanh | Chi phí khởi động cao |
| Phân tán | Không | Không | Có (cluster) |
| Môn học | Baseline | Baseline mạnh | **Trọng tâm** |

## Quyết định

Dùng **Apache Spark 4.1.x** (PySpark) làm engine chính.
Pandas (chunk) và DuckDB là baseline để chứng minh/phản biện điểm giao (E5).

## Lý do

1. Trọng tâm môn học (Chương 5), MLlib tích hợp sẵn.
2. DuckDB mạnh ở cỡ một máy — nêu thẳng điều này ở E5 và mục 8 báo cáo.
3. Spark cần thiết khi dữ liệu vượt RAM một máy (tier XL) hoặc cần xử lý phân tán thật.

## Hệ quả

- Cần Docker Compose để đảm bảo môi trường đồng nhất.
- E5 phải đo trung thực điểm giao; không "vẽ" kết quả đẹp nếu DuckDB thắng.
- Mọi job phải chạy được cả `local[*]` và cluster (tham số `spark.master` cấu hình được).
