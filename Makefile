# =============================================================================
# bigdata-ecom Makefile
# Dùng: make <target> [TIER=S|M|L|XL]
# =============================================================================

TIER         ?= S
PIPELINE_CONFIG ?= /app/configs/pipeline.yaml
E5_CONFIG    ?= $(PIPELINE_CONFIG)
COMPOSE      := docker compose -f docker/docker-compose.yml
SPARK_SUBMIT := $(COMPOSE) exec -e CONFIG_PATH=$(PIPELINE_CONFIG) spark-master spark-submit
PYSPARK_PATH := /app:/opt/bitnami/spark/python/lib/pyspark.zip:/opt/bitnami/spark/python/lib/py4j-0.10.9.7-src.zip

.PHONY: help up down logs profile bronze silver gold analytics plots all \
        experiments test lint clean

help:
	@echo ""
	@echo "Targets:"
	@echo "  make up                   Khởi động Spark cluster"
	@echo "  make down                 Dừng cluster"
	@echo "  make profile  TIER=S      Profiling bronze (null rate, phân phối)"
	@echo "  make bronze   TIER=S      CSV -> Parquet bronze"
	@echo "  make silver   TIER=S      bronze -> silver (làm sạch)"
	@echo "  make gold     TIER=S      silver -> gold (sessionization, dim/fact)"
	@echo "  make analytics TIER=S     gold -> KPI, funnel, RFM, KMeans"
	@echo "  make plots                Sinh biểu đồ từ results/ -> docs/figures/"
	@echo "  make all      TIER=S      Chạy toàn bộ pipeline (bronze -> analytics)"
	@echo "  make experiments          Chạy E1-E7"
	@echo "  make experiment-e5 TIER=M E5_CONFIG=/app/configs/pipeline-m5-tier-m.yaml"
	@echo "  make bronze TIER=M PIPELINE_CONFIG=/app/configs/pipeline-m5-tier-m.yaml"
	@echo "  make test                 pytest trên tier S"
	@echo "  make lint                 ruff check toàn bộ src/, experiments/, tests/"
	@echo "  make clean                Xoá silver/gold/quarantine (giữ raw, bronze)"
	@echo ""
	@echo "Tier: S (sample ~1M dòng)  M (1 tháng)  L (nhiều tháng)  XL (synthetic)"
	@echo ""

# ---------- Cluster ----------

up:
	$(COMPOSE) up -d
	@echo ""
	@echo "Spark Master UI : http://localhost:8080"
	@echo "History Server  : http://localhost:18080"
	@echo ""

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

# ---------- Pipeline ----------

profile:
	$(SPARK_SUBMIT) \
		--master spark://spark-master:7077 \
		--conf spark.app.name=profiling \
		/app/src/prepare_input/profiling.py --tier $(TIER)

bronze:
	$(SPARK_SUBMIT) \
		--master spark://spark-master:7077 \
		--conf spark.app.name=prepare_input \
		/app/src/prepare_input/main.py --tier $(TIER)

silver:
	$(SPARK_SUBMIT) \
		--master spark://spark-master:7077 \
		--conf spark.app.name=bronze_to_silver \
		/app/src/processing/bronze_to_silver.py --tier $(TIER)

gold:
	$(SPARK_SUBMIT) \
		--master spark://spark-master:7077 \
		--conf spark.app.name=silver_to_gold \
		/app/src/processing/silver_to_gold.py --tier $(TIER)

analytics:
	$(SPARK_SUBMIT) \
		--master spark://spark-master:7077 \
		--conf spark.app.name=analytics \
		/app/src/analytics/run_analytics.py --tier $(TIER)

plots:
	$(COMPOSE) exec -e CONFIG_PATH=$(PIPELINE_CONFIG) spark-master bash -c \
		"cd /app && python src/analytics/plots.py --tier $(TIER)"

all: bronze silver gold analytics plots
	@echo ""
	@echo "Pipeline hoàn thành (TIER=$(TIER))"
	@echo "Biểu đồ: docs/figures/"
	@echo "KPI:     cấu hình $(PIPELINE_CONFIG) → paths.results/kpi_summary.csv"
	@echo ""

# ---------- Thí nghiệm ----------

experiments:
	$(SPARK_SUBMIT) \
		--master spark://spark-master:7077 \
		/app/experiments/run_all.py --tier $(TIER)

# Chạy thí nghiệm đơn lẻ: make experiment-e1 TIER=L
experiment-e1:
	$(SPARK_SUBMIT) --master spark://spark-master:7077 \
		/app/experiments/e1_scale_data.py --tier $(TIER)

experiment-e2:
	$(SPARK_SUBMIT) --master spark://spark-master:7077 \
		/app/experiments/e2_scale_resource.py --tier $(TIER)

experiment-e3:
	$(SPARK_SUBMIT) --master spark://spark-master:7077 \
		/app/experiments/e3_format.py --tier $(TIER)

experiment-e4:
	$(SPARK_SUBMIT) --master spark://spark-master:7077 \
		/app/experiments/e4_tuning.py --tier $(TIER)

experiment-e5:
	$(COMPOSE) exec -e CONFIG_PATH=$(E5_CONFIG) spark-master spark-submit \
		--master local[*] /app/experiments/e5_crossover.py --tier $(TIER)

experiment-e6:
	$(COMPOSE) exec -e CONFIG_PATH=$(PIPELINE_CONFIG) spark-master bash -c \
		"cd /app && python experiments/e6_correctness.py --tier $(TIER)"

experiment-e7:
	$(COMPOSE) exec spark-master bash -c \
		"cd /app && python experiments/e7_mapreduce.py"

plot-experiments:
	$(COMPOSE) exec spark-master bash -c \
		"cd /app && python experiments/plot.py"

# ---------- Test & Lint ----------

test:
	$(COMPOSE) exec -e PYTHONPATH=$(PYSPARK_PATH) spark-master bash -c \
		"cd /app && python -m pytest tests/ -v --tb=short"

test-cleaning:
	$(COMPOSE) exec -e PYTHONPATH=$(PYSPARK_PATH) spark-master bash -c \
		"cd /app && python -m pytest tests/test_cleaning_rules.py -v"

test-kpi:
	$(COMPOSE) exec -e PYTHONPATH=$(PYSPARK_PATH) spark-master bash -c \
		"cd /app && python -m pytest tests/test_kpi.py -v"

test-invariants:
	$(COMPOSE) exec -e PYTHONPATH=$(PYSPARK_PATH) spark-master bash -c \
		"cd /app && python -m pytest tests/test_invariants.py -v"

lint:
	$(COMPOSE) exec spark-master bash -c \
		"cd /app && python -m ruff check src/ experiments/ tests/"

# ---------- Dọn dẹp ----------

clean:
	@echo "Xoá silver, gold, quarantine, results/tmp (giữ raw và bronze)..."
	rm -rf data/lake/silver/* data/lake/gold/* data/lake/quarantine/*
	rm -rf results/tmp/ results/e3_tmp/
	@echo "Xong."

clean-all:
	@echo "CẢNH BÁO: Xoá toàn bộ data/lake/ kể cả bronze!"
	@echo "Chạy 'make bronze TIER=M' để tạo lại."
	rm -rf data/lake/bronze/* data/lake/silver/* data/lake/gold/* \
	       data/lake/quarantine/* results/tmp/ results/e3_tmp/
