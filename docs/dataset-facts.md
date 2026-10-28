# Dataset Facts

> **Quan trọng**: Chỉ điền số liệu khi đã đo thực. Không ước lượng.
> Owner: B | Cập nhật: *(điền khi đo xong)*

## Nguồn

- Tên: REES46 "eCommerce behavior data from multi category store"
- URL: <https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data>
- Giấy phép: Chỉ dùng cho mục đích phi thương mại — ghi nguồn trong báo cáo
- Lưu ý: REES46 còn dataset khác (mỹ phẩm, ~9M dòng) — tải đúng bản "multi category store"

## Số liệu thực tế *(điền bằng script, không điền tay)*

| File | Số dòng | Dung lượng CSV | Dung lượng Parquet (snappy) |
|------|---------|----------------|-----------------------------|
| 2019-Oct.csv | 42,448,763 | 5,406.0 MB (5.28 GB) | ~1.6 GB (ước tính) |
| 2019-Nov.csv | 67,501,979 | 9,000.0 MB (8.79 GB) | ~2.7 GB (ước tính) |
| **Tổng** | 109,950,742 | 14,406.0 MB (14.07 GB) | ~4.3 GB (ước tính) |

## Script đếm

```bash
# Chạy trong container hoặc môi trường có Python + PyArrow
python - <<'EOF'
import os, glob
for f in sorted(glob.glob("data/raw/*.csv")):
    lines = sum(1 for _ in open(f)) - 1  # trừ header
    size_mb = os.path.getsize(f) / 1024 / 1024
    print(f"{f}: {lines:,} dòng | {size_mb:.1f} MB")
EOF
```

## Schema gốc (9 cột)

```
event_time, event_type, product_id, category_id, category_code,
brand, price, user_id, user_session
```

Kiểu dữ liệu thực tế xác nhận bằng `src/prepare_input/profiling.py`.

## Đặc điểm đã biết

- Có dòng trùng hoàn toàn (quyết định dedupe: xem ADR + R7 trong bronze_to_silver.py)
- Nhiều dòng thiếu `brand` và `category_code`
- Giá trị `price = 0` tồn tại (gắn cờ, không quarantine)
- Không có `order_id`, không có `quantity`

## Cấu hình máy chạy thí nghiệm *(điền ở Ngày 0)*

| Thành viên | CPU | RAM | Ổ đĩa trống | OS |
|-----------|-----|-----|-------------|-----|
| Hiếu (A) | *(điền)* | *(điền)* | *(điền)* | *(điền)* |
| B | *(điền)* | *(điền)* | *(điền)* | *(điền)* |
| C | *(điền)* | *(điền)* | *(điền)* | *(điền)* |
| D | *(điền)* | *(điền)* | *(điền)* | *(điền)* |
| E | *(điền)* | *(điền)* | *(điền)* | *(điền)* |
