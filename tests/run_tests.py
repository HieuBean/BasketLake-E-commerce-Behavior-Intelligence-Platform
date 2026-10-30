"""
tests/run_tests.py
Runner cho test_cleaning_rules.py và test_invariants.py
Chạy trực tiếp qua spark-submit trong container:
    spark-submit tests/run_tests.py
"""
import sys
from pyspark.sql import SparkSession
import tests.test_cleaning_rules as tcr
import tests.test_invariants as ti

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("test_runner")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.ansi.enabled", "true")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

modules = [("test_cleaning_rules", tcr), ("test_invariants", ti)]
passed = 0
failed = 0

print("=" * 60)
print("BẮT ĐẦU CHẠY KIỂM THỬ TẦNG SILVER (M2)")
print("=" * 60)

for mod_name, mod in modules:
    print(f"\n--- {mod_name} ---")
    for name in dir(mod):
        if name.startswith("test_"):
            fn = getattr(mod, name)
            try:
                if fn.__code__.co_argcount >= 1:
                    fn(spark)
                else:
                    fn()
                print(f"  [PASS] {name}")
                passed += 1
            except Exception as e:
                print(f"  [FAIL] {name}: {e}")
                failed += 1

print("\n" + "=" * 60)
print(f"KẾT QUẢ: {passed} passed, {failed} failed")
print("=" * 60)

spark.stop()
if failed > 0:
    sys.exit(1)
