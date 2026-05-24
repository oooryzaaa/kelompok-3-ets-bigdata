import os
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['hadoop.home.dir'] = "C:\\hadoop"

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

builder = SparkSession.builder.appName("CekData") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")

spark = configure_spark_with_delta_pip(
    builder, extra_packages=["io.delta:delta-spark_2.12:3.3.0"]
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

SILVER_PATH = "lakehouse_data/silver/saham_api"

# 1. Bronze vs Silver
print("=" * 60)
print("1. BRONZE vs SILVER")
print("=" * 60)
bronze = spark.read.parquet("lakehouse_data/bronze/saham_api")
silver = spark.read.format("delta").load(SILVER_PATH)
print(f"Bronze - rows: {bronze.count()}, columns: {len(bronze.columns)}")
print(f"Silver - rows: {silver.count()}, columns: {len(silver.columns)}")
print("\nKolom baru di Silver (tidak ada di Bronze):")
new_cols = set(silver.columns) - set(bronze.columns)
print(new_cols)

# 2. Isi Silver sekarang
print("\n" + "=" * 60)
print("2. ISI SILVER SEKARANG")
print("=" * 60)
silver.select("ticker", "harga", "return_pct", "jam", "is_outlier", "z_score").show(10)

# 3. Semua versi Time Travel
print("=" * 60)
print("3. VERSI TIME TRAVEL")
print("=" * 60)
dt = DeltaTable.forPath(spark, SILVER_PATH)
dt.history().select("version", "timestamp", "operation").show(truncate=False)

print("Version 0:")
spark.read.format("delta").option("versionAsOf", 0).load(SILVER_PATH) \
    .select("ticker", "harga", "return_pct", "is_outlier").show(5)

print("Version 1 (post-UPDATE):")
spark.read.format("delta").option("versionAsOf", 1).load(SILVER_PATH) \
    .select("ticker", "harga", "return_pct", "is_outlier").show(5)

print("Version 2 (post-DELETE, ticker GOTO dihapus):")
spark.read.format("delta").option("versionAsOf", 2).load(SILVER_PATH) \
    .select("ticker", "harga", "return_pct", "is_outlier").show(5)

spark.stop()
