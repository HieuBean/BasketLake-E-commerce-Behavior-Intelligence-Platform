# Tier L full DQ — PASS

Authoritative evidence: dq.json; all checks use full Tier L.

| Check | Result |
|---|---|
| row_accounting | PASS |
| immutable_input_counts | PASS |
| quarantine_schema | PASS |
| silver_schema | PASS |
| all_required_columns | PASS |
| R1_R4_independent_reject_counts | PASS |
| reject_rule_values_and_priority | PASS |
| required_nulls | PASS |
| R2_event_type | PASS |
| R3_UTC | PASS |
| R4_price | PASS |
| R5_brand | PASS |
| R6_category | PASS |
| R8_month | PASS |
| R9_flag | PASS |
| R9_original | PASS |
| R9_generated_gap | PASS |
| both_source_months | PASS |
| both_event_months | PASS |
| canonical_source_timestamp | PASS |
| R7_metadata_tie_proof | PASS |
| full_normalized_payload_multiset_conservation | PASS |
| R7_full_key_unique_survivor_and_n_minus_1 | PASS |
| purchase_revenue_reconciliation | PASS |
| E1_workload_actual_aggregates | PASS |
| output_commits | PASS |

Row counts: {"bronze": 109950743, "silver": 109820004, "quarantine": 130739}
Purchase/revenue: {"bronze": {"purchases": 1659788, "revenue": 505152392.76999843}, "silver": {"purchases": 1659703, "revenue": 505126293.4400004}, "quarantine": {"purchases": 85, "revenue": 26099.33}}

No E1 benchmark executed. Original Silver data was not rewritten.
