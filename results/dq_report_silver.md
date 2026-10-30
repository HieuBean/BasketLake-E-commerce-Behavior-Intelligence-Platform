# DQ report — Silver (tier S)

| Luật | Tên | Hành động | Dòng vào | Dòng ra | Bị loại | Tỷ lệ loại | Gắn cờ | Tỷ lệ cờ | Thời gian |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| R1 | null_key_field | quarantine | 1,060,438 | 1,060,438 | 0 | 0.0000% | 0 | 0.0000% | — |
| R2 | invalid_event_type | quarantine | 1,060,438 | 1,060,438 | 0 | 0.0000% | 0 | 0.0000% | — |
| R3 | timestamp_parse_error | quarantine | 1,060,438 | 1,060,438 | 0 | 0.0000% | 0 | 0.0000% | — |
| R4 | negative_price | quarantine (<0); flag price_valid=false (=0/null) | 1,060,438 | 1,060,438 | 0 | 0.0000% | 1,674 | 0.1579% | — |
| R5 | brand_missing | fill 'unknown' + flag | 1,060,438 | 1,060,438 | 0 | 0.0000% | 153,054 | 14.4337% | — |
| R6 | category_missing | category_l1='unknown' + flag | 1,060,438 | 1,060,438 | 0 | 0.0000% | 338,266 | 31.9001% | — |
| R7 | duplicate_row | quarantine (dedupe, giữ 1 bản) | 1,060,438 | 1,060,392 | 46 | 0.0043% | 0 | 0.0000% | — |
| R8 | out_of_month | flag | 1,060,392 | 1,060,392 | 0 | 0.0000% | 0 | 0.0000% | — |
| R9 | session_generated | sinh session_id bằng gap | 1,060,392 | 1,060,392 | 0 | 0.0000% | 0 | 0.0000% | — |
| **TOTAL** |  |  | **1,060,438** | **1,060,392** | **46** | **0.0043%** |  |  | — |
