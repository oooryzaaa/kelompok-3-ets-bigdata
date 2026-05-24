import os
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['hadoop.home.dir'] = "C:\\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, to_timestamp, hour, dayofweek,
    avg, stddev, abs as spark_abs,
    when, lit, current_timestamp,
    round as spark_round
)
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder.appName("Silver-SahamMeter") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")

spark = configure_spark_with_delta_pip(
    builder, extra_packages=["io.delta:delta-spark_2.12:3.3.0"]
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("SparkSession + Delta Lake ready")

print("\nReading Bronze layer...")
api_df = spark.read.parquet("lakehouse_data/bronze/saham_api")
rss_df = spark.read.parquet("lakehouse_data/bronze/saham_rss")
print(f"Bronze API : {api_df.count()} rows")
print(f"Bronze RSS : {rss_df.count()} rows")

# --- CLEANING API ---
print("\n--- Cleaning API ---")

step1 = api_df.dropDuplicates(["ticker", "timestamp"])
print(f"T1 - Remove duplicates     : {api_df.count()} -> {step1.count()} rows")

step2 = step1.filter(
    col("ticker").isNotNull() &
    col("harga").isNotNull()
)
print(f"T2 - Drop NULL             : {step1.count()} -> {step2.count()} rows")

step3 = step2.filter(
    (col("harga") > 0) &
    (col("open") > 0) &
    (col("high") >= col("low")) &
    (col("volume") >= 0)
)
print(f"T3 - Filter invalid values : {step2.count()} -> {step3.count()} rows")

step4 = step3 \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("jam", hour(col("timestamp"))) \
    .withColumn("hari_minggu", dayofweek(col("timestamp"))) \
    .withColumn("return_pct",
        spark_round((col("harga") - col("open")) / col("open") * 100, 4)) \
    .withColumn("price_range", col("high") - col("low")) \
    .withColumn("_cleaned_at", current_timestamp())
print(f"T4 - Cast timestamp + feature engineering : done")

print("\nDetecting price outliers via Z-Score...")
window_ticker = Window.partitionBy("ticker")
silver_api = step4 \
    .withColumn("mean_harga", avg("harga").over(window_ticker)) \
    .withColumn("std_harga", stddev("harga").over(window_ticker)) \
    .withColumn("z_score",
        when(col("std_harga") > 0,
             (col("harga") - col("mean_harga")) / col("std_harga")
        ).otherwise(lit(0.0))) \
    .withColumn("is_outlier",
        when(spark_abs(col("z_score")) > 2, True).otherwise(False))

outlier_count = silver_api.filter(col("is_outlier") == True).count()
print(f"Outliers flagged (Z-Score > 2) : {outlier_count} rows")

# --- CLEANING RSS ---
print("\n--- Cleaning RSS ---")

rss1 = rss_df.dropDuplicates(["id"])
print(f"T1 - Remove duplicates : {rss_df.count()} -> {rss1.count()} rows")

silver_rss = rss1.filter(
    col("judul").isNotNull() &
    col("id").isNotNull()
).withColumn("waktu_terbit", to_timestamp(col("waktu_terbit"), "EEE, dd MMM yyyy HH:mm:ss Z")) \
 .withColumn("jam", hour(col("waktu_terbit"))) \
 .withColumn("_cleaned_at", current_timestamp())
print(f"T2 - Filter NULL + cast timestamp : {rss1.count()} -> {silver_rss.count()} rows")

# --- SAVE TO DELTA ---
print("\nSaving to Silver Delta Lake...")

silver_api.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("lakehouse_data/silver/saham_api")

silver_rss.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("lakehouse_data/silver/saham_rss")

print("Silver API saved to lakehouse_data/silver/saham_api")
print("Silver RSS saved to lakehouse_data/silver/saham_rss")

print("\nSample Silver API (5 rows):")
silver_api.select("ticker", "harga", "return_pct", "jam", "is_outlier", "z_score").show(5)

print("\nSilver layer complete.")
spark.stop()
