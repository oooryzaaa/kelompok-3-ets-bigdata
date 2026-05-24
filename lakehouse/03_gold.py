import os
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['hadoop.home.dir'] = "C:\\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, stddev, count, round as spark_round,
    current_timestamp, lit
)
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder.appName("Gold-SahamMeter") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")

spark = configure_spark_with_delta_pip(
    builder, extra_packages=["io.delta:delta-spark_2.12:3.3.0"]
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Gold Layer SparkSession aktif")

# ============================================================
# BACA SILVER LAYER
# ============================================================
print("\nMembaca Silver layer...")
silver_api = spark.read.format("delta").load("lakehouse_data/silver/saham_api")
print(f"Silver API: {silver_api.count()} rows")

# ============================================================
# TABEL GOLD 1 — Return per Saham
# Reproduksi Analisis ETS: saham mana yang naik/turun paling banyak
# ============================================================
print("\n--- Membuat Gold 1: Return per Saham ---")

gold_return = silver_api \
    .groupBy("ticker") \
    .agg(
        spark_round(avg("harga"), 2).alias("avg_harga"),
        spark_round(avg("return_pct"), 4).alias("avg_return_pct"),
        spark_round(avg("open"), 2).alias("avg_open"),
    ) \
    .withColumn("label",
        col("avg_return_pct").cast("string")
    ) \
    .withColumn("_created_at", current_timestamp()) \
    .orderBy(col("avg_return_pct").desc())

print("Preview Gold 1 - Return per Saham:")
gold_return.show(truncate=False)

gold_return.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("lakehouse_data/gold/return_saham")
print("✅ Gold 1 tersimpan ke lakehouse_data/gold/return_saham")

# ============================================================
# TABEL GOLD 2 — Volatilitas per Saham
# Reproduksi Analisis ETS: saham mana yang paling fluktuatif
# ============================================================
print("\n--- Membuat Gold 2: Volatilitas per Saham ---")

gold_volatilitas = silver_api \
    .groupBy("ticker") \
    .agg(
        spark_round(stddev("harga"), 4).alias("volatilitas"),
        spark_round(avg("price_range"), 2).alias("avg_price_range"),
        count("ticker").alias("jumlah_data")
    ) \
    .withColumn("kategori_risiko",
        col("volatilitas").cast("double")
    ) \
    .withColumn("_created_at", current_timestamp()) \
    .orderBy(col("volatilitas").desc())

print("Preview Gold 2 - Volatilitas per Saham:")
gold_volatilitas.show(truncate=False)

gold_volatilitas.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("lakehouse_data/gold/volatilitas_saham")
print("✅ Gold 2 tersimpan ke lakehouse_data/gold/volatilitas_saham")

# ============================================================
# EXPORT KE JSON UNTUK FLASK DASHBOARD
# ============================================================
print("\n--- Export ke JSON untuk Dashboard ---")

os.makedirs("dashboard/data", exist_ok=True)

# Export return saham
return_list = [row.asDict() for row in gold_return.collect()]
import json
with open("dashboard/data/gold_return.json", "w") as f:
    json.dump(return_list, f, ensure_ascii=False, indent=2, default=str)
print("✅ Ekspor gold_return.json selesai")

# Export volatilitas saham
volatilitas_list = [row.asDict() for row in gold_volatilitas.collect()]
with open("dashboard/data/gold_volatilitas.json", "w") as f:
    json.dump(volatilitas_list, f, ensure_ascii=False, indent=2, default=str)
print("✅ Ekspor gold_volatilitas.json selesai")

print("\n🎉 Gold Layer selesai! Dashboard siap membaca data baru.")
spark.stop()