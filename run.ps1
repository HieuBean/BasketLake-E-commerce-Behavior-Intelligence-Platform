# =============================================================================
# run.ps1 - PowerShell Runner thay thế Make cho Windows
# Sử dụng: .\run.ps1 <lệnh> [TIER]
# Ví dụ:   .\run.ps1 bronze S
#          .\run.ps1 silver S
#          .\run.ps1 gold S
# =============================================================================

param (
    [Parameter(Position=0, Mandatory=$false)]
    [string]$Command = "help",

    [Parameter(Position=1, Mandatory=$false)]
    [string]$Tier = "S"
)

$COMPOSE = "docker compose -f docker/docker-compose.yml"
$SPARK_SUBMIT = "$COMPOSE exec spark-master /opt/spark/bin/spark-submit"

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
        Write-Host "==> Chạy Analytics KPI / Funnel (Tier $Tier)..." -ForegroundColor Green
        Invoke-Expression "$SPARK_SUBMIT --master spark://spark-master:7077 --conf spark.app.name=analytics /app/src/analytics/run_analytics.py --tier $Tier"
    }
    "e2" {
        Write-Host "==> Chạy Thí nghiệm E2 (Amdahl Speedup)..." -ForegroundColor Green
        Invoke-Expression "$SPARK_SUBMIT /app/experiments/e2_scale_resource.py --tier $Tier"
    }
    "e7" {
        Write-Host "==> Chạy Thí nghiệm E7 (MapReduce Combiner)..." -ForegroundColor Green
        Invoke-Expression "$COMPOSE exec spark-master python3 /app/experiments/e7_mapreduce.py"
    }
    "all" {
        Write-Host "==> Chạy toàn bộ Pipeline (Tier $Tier)..." -ForegroundColor Yellow
        & $PSCommandPath bronze $Tier
        & $PSCommandPath silver $Tier
        & $PSCommandPath gold $Tier
        & $PSCommandPath analytics $Tier
    }
    default {
        Write-Host @"
====================================================================
bigdata-ecom PowerShell Helper (Dành cho Windows)
====================================================================
Cách dùng:
  .\run.ps1 up                Khởi động Spark cluster
  .\run.ps1 down              Dừng Spark cluster
  .\run.ps1 ps                Xem trạng thái các container
  .\run.ps1 logs              Xem logs cluster realtime

  .\run.ps1 bronze [TIER]     CSV thô -> Parquet Bronze (mặc định S)
  .\run.ps1 silver [TIER]     Bronze -> Silver (làm sạch, DQ report)
  .\run.ps1 gold [TIER]       Silver -> Gold (sessionization, dims)
  .\run.ps1 analytics [TIER]  Gold -> KPI, Funnel, RFM
  .\run.ps1 all [TIER]        Chạy toàn bộ pipeline (Bronze -> Analytics)

  .\run.ps1 e2 [TIER]         Thí nghiệm E2 (Scale resources)
  .\run.ps1 e7                Thí nghiệm E7 (MapReduce Combiner)
  .\run.ps1 profile [TIER]    Profiling dữ liệu Bronze

Các Tier: S (1M dòng lấy mẫu), M (toàn bộ T10), L (T10+T11), XL (amplify)
====================================================================
"@ -ForegroundColor Cyan
    }
}
