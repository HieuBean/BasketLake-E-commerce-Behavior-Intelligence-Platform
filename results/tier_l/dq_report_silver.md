# DQ report — Silver (tier L)

| Luật | Tên | Hành động | Dòng vào | Dòng ra | Bị loại | Tỷ lệ loại | Gắn cờ | Tỷ lệ cờ | Thời gian |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| R1 | null_key_field | quarantine | 109,950,743 | 109,950,743 | 0 | 0.0000% | 0 | 0.0000% | — |
| R2 | invalid_event_type | quarantine | 109,950,743 | 109,950,743 | 0 | 0.0000% | 0 | 0.0000% | — |
| R3 | timestamp_parse_error | quarantine | 109,950,743 | 109,950,743 | 0 | 0.0000% | 0 | 0.0000% | — |
| R4 | negative_price | quarantine (<0); flag price_valid=false (=0/null) | 109,950,743 | 109,950,743 | 0 | 0.0000% | 256,657 | 0.2337% | — |
| R5 | brand_missing | fill 'unknown' + flag | 109,950,743 | 109,950,743 | 0 | 0.0000% | 15,321,303 | 13.9513% | — |
| R6 | category_missing | category_l1='unknown' + flag | 109,950,743 | 109,950,743 | 0 | 0.0000% | 35,383,685 | 32.2197% | — |
| R7 | duplicate_row | quarantine (dedupe, giữ 1 bản) | 109,950,743 | 109,820,004 | 130,739 | 0.1189% | 0 | 0.0000% | — |
| R8 | out_of_month | flag | 109,820,004 | 109,820,004 | 0 | 0.0000% | 0 | 0.0000% | — |
| R9 | session_generated | sinh session_id bằng gap | 109,820,004 | 109,820,004 | 0 | 0.0000% | 12 | 0.0000% | — |
| **TOTAL** |  |  | **109,950,743** | **109,820,004** | **130,739** | **0.1189%** |  |  | — |
