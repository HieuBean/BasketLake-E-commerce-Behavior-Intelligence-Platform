# Data Dictionary

> Phiên bản: 1 | Owner: B | Cập nhật: 07/10/2026

## Bronze — `bronze_events`

| Cột | Kiểu | Nullable | Mô tả |
|-----|------|----------|-------|
| event_time | string | No | Chuỗi gốc từ CSV, chưa parse |
| event_type | string | No | view / cart / purchase / remove_from_cart |
| product_id | long | No | ID sản phẩm |
| category_id | long | Yes | ID danh mục |
| category_code | string | Yes | Mã danh mục phân cấp (vd: electronics.smartphone) |
| brand | string | Yes | Thương hiệu |
| price | double | Yes | Giá đơn vị (USD) |
| user_id | long | No | ID người dùng |
| user_session | string | Yes | ID phiên gốc từ dataset |
| source_file | string | No | Tên file CSV gốc |
| ingest_ts | timestamp | No | Thời điểm ingest vào bronze |
| source_month | string | No | Partition key — YYYY-MM |

**Bất biến bronze**: chưa làm sạch, chưa dedupe. Schema ép kiểu nhưng giá trị giữ nguyên.

---

## Silver — `silver_events`

| Cột | Kiểu | Nullable | Mô tả |
|-----|------|----------|-------|
| event_ts | timestamp | No | Timestamp UTC đã parse từ event_time |
| event_date | date | No | Ngày của event_ts |
| event_hour | int | No | Giờ trong ngày (0–23) |
| event_type | string | No | Như bronze |
| product_id | long | No | Như bronze |
| category_id | long | Yes | Như bronze |
| category_l1 | string | No | Cấp 1 của category_code (hoặc "unknown") |
| category_l2 | string | Yes | Cấp 2 |
| category_l3 | string | Yes | Cấp 3 |
| brand | string | No | Như bronze; null → "unknown" |
| brand_missing | boolean | No | True nếu brand gốc là null |
| price | double | Yes | Như bronze |
| price_valid | boolean | No | True nếu price > 0 |
| user_id | long | No | Như bronze |
| user_session | string | Yes | Như bronze |
| out_of_month | boolean | No | True nếu event nằm ngoài tháng của file |
| source_file | string | No | Như bronze |
| source_month | string | No | Như bronze |

**Partition**: `event_date`

---

## Quarantine — `quarantine_events`

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| *(tất cả cột bronze)* | — | Dòng gốc |
| reject_reason | string | Lý do bị loại (null_key_field, invalid_event_type, timestamp_parse_error, negative_price) |

**Bất biến**: `count(bronze) = count(silver) + count(quarantine)`

---

## Gold

### `fact_sessions`

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| user_id | long | |
| user_session | string | Session key |
| session_start | timestamp | Sự kiện đầu tiên của phiên |
| session_end | timestamp | Sự kiện cuối cùng của phiên |
| last_purchase_ts | timestamp | Lần purchase cuối trong phiên; null nếu phiên không mua |
| session_duration_s | long | Thời lượng phiên (giây) |
| event_count | long | Tổng số sự kiện |
| view_count | long | Số view |
| cart_count | long | Số cart |
| purchase_count | long | Số purchase |
| revenue | double | Tổng giá purchase (giả định qty=1) |
| distinct_products | long | Số sản phẩm khác nhau trong phiên |
| category_l1 | string | Danh mục chính của phiên |
| converted | boolean | True nếu có ≥1 purchase |

### `dim_product`

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| product_id | long | PK |
| category_id | long | |
| category_l1/l2/l3 | string | |
| brand | string | |
| price_min/max/avg | double | Thống kê giá |

### `dim_user`

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| user_id | long | PK |
| first_seen | timestamp | |
| last_seen | timestamp | |
| session_count | long | Số phiên |

### `agg_daily`

Grain: `event_date × category_l1`

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| event_date | date | |
| category_l1 | string | |
| views | long | |
| carts | long | |
| purchases | long | |
| revenue | double | |
| unique_users | long | approx_count_distinct khuyến nghị cho scale |
| sessions | long | |

### `agg_hourly`

Grain: `day_of_week × event_hour`

### `agg_funnel`

Grain: `category_l1`

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| category_l1 | string | |
| sessions_with_view | long | Phiên có ≥1 view |
| sessions_with_cart | long | Phiên có ≥1 cart |
| sessions_with_purchase | long | Phiên có ≥1 purchase |

### `user_segments` (Analytics output)

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| user_id | long | |
| recency | int | Số ngày từ `last_purchase_ts` gần nhất đến `as_of_date` |
| frequency | long | Số phiên mua |
| monetary | double | Tổng doanh thu |
| cluster | int | Nhãn cụm KMeans (0-indexed) |

---

## KPI Definitions

| KPI | Định nghĩa chính xác |
|-----|----------------------|
| Revenue | `SUM(price) WHERE event_type='purchase' AND price > 0` |
| Sessions | Số cặp `(user_id, user_session)` khác nhau, cùng grain với `fact_sessions` |
| Conversion rate | `sessions có purchase / sessions có view` |
| Cart abandonment | `sessions có cart nhưng không purchase / sessions có cart` |
| AOV proxy | `Revenue / sessions_converted` (giả định qty=1) |
| DAU | `COUNT(DISTINCT user_id) GROUP BY event_date` |
| RFM as_of | Xem `configs/pipeline.yaml → analytics.rfm_as_of` |

> **Lưu ý quan trọng**: Dataset không có `order_id` và không có số lượng.
> Revenue = proxy, AOV = proxy. Ghi rõ trong báo cáo mục 8.
