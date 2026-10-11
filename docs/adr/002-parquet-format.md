# ADR-002: Định dạng lưu trữ — Parquet (snappy)

Trạng thái: **chấp nhận** | Ngày: 07/10/2026 | Người quyết: Nhóm (Ngày 0)

## Bối cảnh

Cần lưu trữ bronze, silver, gold với hiệu quả đọc/ghi và hỗ trợ partitioning.

## Các lựa chọn

| | CSV | JSON | Parquet |
|---|---|---|---|
| Nén | Kém | Kém | Tốt (snappy/zstd) |
| Đọc cột | Không | Không | Có (column pruning) |
| Predicate pushdown | Không | Không | Có |
| Schema | Không ép | Không ép | Có |

## Quyết định

Dùng **Parquet + snappy** cho tất cả tầng bronze/silver/gold.
So sánh thêm với zstd ở E3.

## Lý do

1. Column pruning giảm I/O đáng kể khi truy vấn vài cột.
2. Partition pruning tránh đọc dữ liệu ngoài khoảng thời gian lọc.
3. Thống nhất với bài giảng Chương 4.

## Hệ quả

- Cần PyArrow để đọc/ghi Parquet ngoài Spark (baseline DuckDB/Pandas ở E5).
- E3 đo và báo cáo dung lượng, thời gian truy vấn của từng định dạng/codec.
