# =============================================================================
# run.ps1 - PowerShell Runner thay thế Make cho Windows
# Sử dụng: .\run.ps1 <lệnh> [TIER]
# Ví dụ:   .\run.ps1 bronze S
#          .\run.ps1 silver S
#          .\run.ps1 gold S
#          .\run.ps1 test
# =============================================================================

param (
    [Parameter(Position=0, Mandatory=$false)]
    [string]$Command = "help",

    [Parameter(Position=1, Mandatory=$false)]
    [string]$Tier = "S"
)

$COMPOSE = "docker compose -f docker/docker-compose.yml"
$SPARK_SUBMIT = "$COMPOSE exec spark-master /opt/spark/bin/spark-submit"
$PYSPARK_PATH = "/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"

switch ($Command.ToLower()) {
    "up" {
        Invoke-Expression "$COMPOSE up -d"
        Write-Host "`nSpark Master UI : http://localhost:8080" -ForegroundColor Cyan
        Write-Host "History Server  : http://localhost:18080`n" -ForegroundColor Cyan
    }
    "down" {
        Invoke-Expression "$COMPOSE down"
    }
    "logs" {
        Invoke-Expression "$COMPOSE logs -f"
    }
    "ps" {
        Invoke-Expression "$COMPOSE ps"
    }
    "profile" {
        Write-Host "==> Chạy Profiling Bronze (Tier $Tier)..." -ForegroundColor Green
        Invoke-Expression "$SPARK_SUBMIT --master spark://spark-master:7077 --conf spark.app.name=profiling /app/src/prepare_input/profiling.py --tier $Tier"
    }
    "bronze" {
        Write-Host "==> Chạy Bronze Ingestion (Tier $Tier)..." -ForegroundColor Green
        Invoke-Expression "$SPARK_SUBMIT --master spark://spark-master:7077 --conf spark.app.name=prepare_input /app/src/prepare_input/main.py --tier $Tier"
    }
    "silver" {
        Write-Host "==> Chạy Bronze -> Silver (Tier $Tier)..." -ForegroundColor Green
        Invoke-Expression "$SPARK_SUBMIT --master spark://spark-master:7077 --conf spark.app.name=bronze_to_silver /app/src/processing/bronze_to_silver.py --tier $Tier"
    }
    "gold" {
        Write-Host "==> Chạy Silver -> Gold (Tier $Tier)..." -ForegroundColor Green
        Invoke-Expression "$SPARK_SUBMIT --master spark://spark-master:7077 --conf spark.app.name=silver_to_gold /app/src/processing/silver_to_gold.py --tier $Tier"
    }
    "analytics" {
        Write-Host "==> Chạy Analytics KPI / Funnel / RFM (Tier $Tier)..." -ForegroundColor Green
        Invoke-Expression "$SPARK_SUBMIT --master spark://spark-master:7077 --conf spark.app.name=analytics /app/src/analytics/run_analytics.py --tier $Tier"
    }
    "plots" {
        Write-Host "==> Sinh biểu đồ Analytics (Tier $Tier)..." -ForegroundColor Green
        Invoke-Expression "$COMPOSE exec spark-master python3 /app/src/analytics/plots.py --tier $Tier"
    }
    "all" {
        Write-Host "==> Chạy toàn bộ Pipeline (Tier $Tier)..." -ForegroundColor Yellow
        & $PSCommandPath bronze $Tier
        & $PSCommandPath silver $Tier
        & $PSCommandPath gold $Tier
        & $PSCommandPath analytics $Tier
        & $PSCommandPath plots $Tier
    }
    "e1" {
        Write-Host "==> Chạy Thí nghiệm E1 (Scale Data)..." -ForegroundColor Green
        Invoke-Expression "$SPARK_SUBMIT /app/experiments/e1_scale_data.py --tier $Tier"
    }
    "e2" {
        Write-Host "==> Chạy Thí nghiệm E2 (Amdahl Speedup)..." -ForegroundColor Green
        Invoke-Expression "$SPARK_SUBMIT /app/experiments/e2_scale_resource.py --tier $Tier"
    }
    "e3" {
        Write-Host "==> Chạy Thí nghiệm E3 (Format comparison: Parquet vs CSV)..." -ForegroundColor Green
        Invoke-Expression "$SPARK_SUBMIT /app/experiments/e3_format.py --tier $Tier"
    }
    "e4" {
        Write-Host "==> Chạy Thí nghiệm E4 (Tuning & Salting)..." -ForegroundColor Green
        Invoke-Expression "$SPARK_SUBMIT /app/experiments/e4_tuning.py --tier $Tier"
    }
    "e5" {
        Write-Host "==> Chạy Thí nghiệm E5 (Crossover Pandas/DuckDB/Spark)..." -ForegroundColor Green
        Invoke-Expression "$SPARK_SUBMIT --master local[*] /app/experiments/e5_crossover.py --tier $Tier"
    }
    "e6" {
        Write-Host "==> Chạy Thí nghiệm E6 (Correctness Oracle)..." -ForegroundColor Green
        Invoke-Expression "$COMPOSE exec spark-master python3 /app/experiments/e6_correctness.py --tier $Tier"
    }
    "e7" {
        Write-Host "==> Chạy Thí nghiệm E7 (MapReduce Combiner)..." -ForegroundColor Green
        Invoke-Expression "$COMPOSE exec spark-master python3 /app/experiments/e7_mapreduce.py"
    }
    "plot-experiments" {
        Write-Host "==> Sinh biểu đồ thí nghiệm..." -ForegroundColor Green
        Invoke-Expression "$COMPOSE exec spark-master python3 /app/experiments/plot.py"
    }
    "test" {
        Write-Host "==> Chạy toàn bộ Unit Tests..." -ForegroundColor Green
        Invoke-Expression "$COMPOSE exec -e PYTHONPATH=$PYSPARK_PATH spark-master python3 -m pytest tests/ -v --tb=short"
    }
    "test-cleaning" {
        Write-Host "==> Chạy Test tầng Silver (R1-R9)..." -ForegroundColor Green
        Invoke-Expression "$COMPOSE exec -e PYTHONPATH=$PYSPARK_PATH spark-master python3 -m pytest tests/test_cleaning_rules.py -v"
    }
    "lint" {
        Write-Host "==> Chạy Ruff Linter..." -ForegroundColor Green
        Invoke-Expression "$COMPOSE exec spark-master python3 -m ruff check src/ experiments/ tests/"
    }
    default {
        Write-Host @"
====================================================================
bigdata-ecom PowerShell Helper (Dành cho Windows)
====================================================================
Quản lý Cluster:
  .\run.ps1 up                Khởi động Spark cluster
  .\run.ps1 down              Dừng Spark cluster
  .\run.ps1 ps                Xem trạng thái các container
  .\run.ps1 logs              Xem logs cluster realtime

Pipeline chính:
  .\run.ps1 bronze [TIER]     CSV thô -> Parquet Bronze (mặc định S)
  .\run.ps1 silver [TIER]     Bronze -> Silver (làm sạch, DQ report)
  .\run.ps1 gold [TIER]       Silver -> Gold (sessionization, dims)
  .\run.ps1 analytics [TIER]  Gold -> KPI, Funnel, RFM
  .\run.ps1 plots [TIER]      Sinh biểu đồ kết quả
  .\run.ps1 all [TIER]        Chạy toàn bộ pipeline (Bronze -> Plots)

Thí nghiệm (E1 - E7):
  .\run.ps1 e1 [TIER]         E1: Scale Data
  .\run.ps1 e2 [TIER]         E2: Scale Resources (Amdahl)
  .\run.ps1 e3 [TIER]         E3: So sánh định dạng lưu trữ
  .\run.ps1 e4 [TIER]         E4: Tuning & Salting
  .\run.ps1 e5 [TIER]         E5: Crossover Pandas / DuckDB / Spark
  .\run.ps1 e6 [TIER]         E6: Kiểm chứng tính đúng đắn với DuckDB
  .\run.ps1 e7                E7: Mô phỏng MapReduce Combiner
  .\run.ps1 plot-experiments  Vẽ biểu đồ các thí nghiệm

Kiểm thử:
  .\run.ps1 test              Chạy toàn bộ pytest
  .\run.ps1 test-cleaning     Chạy test tầng Silver (R1-R9)
  .\run.ps1 lint              Kiểm tra ruff lint

Các Tier: S (1M dòng lấy mẫu), M (toàn bộ T10), L (T10+T11), XL (amplify)
====================================================================
"@ -ForegroundColor Cyan
    }
}
