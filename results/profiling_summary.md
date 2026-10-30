# Profiling bronze — tier S

- Tổng số dòng: **1,060,438**
- User ≈ 652,746 | Product ≈ 92,487 | Session ≈ 906,898 | Brand ≈ 2,811 | Category code ≈ 125

## 1. event_type

| event_type | count | % |
|---|---:|---:|
| view | 1,019,004 | 96.0927% |
| cart | 22,964 | 2.1655% |
| purchase | 18,470 | 1.7417% |

Giá trị lạ (ngoài ['cart', 'purchase', 'remove_from_cart', 'view']): không có

## 2–3. Null rate

| Cột | Kiểu | Null | Rỗng | Null rate |
|---|---|---:|---:|---:|
| event_time | string | 0 | 0 | 0.0000% |
| event_type | string | 0 | 0 | 0.0000% |
| product_id | bigint | 0 | 0 | 0.0000% |
| category_id | bigint | 0 | 0 | 0.0000% |
| category_code | string | 338,270 | 0 | 31.8991% |
| brand | string | 153,056 | 0 | 14.4333% |
| price | double | 0 | 0 | 0.0000% |
| user_id | bigint | 0 | 0 | 0.0000% |
| user_session | string | 0 | 0 | 0.0000% |
| source_file | string | 0 | 0 | 0.0000% |
| ingest_ts | timestamp | 0 | 0 | 0.0000% |
| source_month | string | 0 | 0 | 0.0000% |

## 4. price

| Chỉ số | Giá trị |
|---|---:|
| price_min | 0.0 |
| price_max | 2574.07 |
| price_avg | 290.7372859139385 |
| price_p25 | 66.15 |
| price_p50 | 163.62 |
| price_p75 | 358.57 |
| price_p99 | 1741.34 |
| count_negative | 0 |
| count_zero | 1674 |
| count_null | 0 |

## 5. event_time

Mẫu: `2019-10-30 05:50:01 UTC`, `2019-10-30 05:50:01 UTC`, `2019-10-30 05:50:01 UTC`, `2019-10-30 05:50:03 UTC`, `2019-10-30 05:50:04 UTC`

- Không parse được: 0 (0.0000%)
- Khoảng thời gian: 2019-10-01 00:00:18 → 2019-10-31 23:59:19 (UTC)
- Ngoài tháng của file (R8): 0 (0.0000%)

## 6. Dòng trùng hoàn toàn (R7)

- 46 dòng (0.0043%)
