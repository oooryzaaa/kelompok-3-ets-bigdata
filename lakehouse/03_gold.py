import os
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['hadoop.home.dir'] = "C:\\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, avg, sum as spark_sum, count, max as spark_max, min as spark_min,
    round as spark_round, to_date, rank, desc, asc
)
from delta import configure_spark_with_delta_pip

# =============================================
# SETUP SPARK + DELTA LAKE
# =============================================
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
print("SparkSession + Delta Lake ready")

# =============================================
# BACA SILVER LAYER
# =============================================
print("\nReading Silver layer (Delta format)...")
silver_api = spark.read.format("delta").load("lakehouse_data/silver/saham_api")
silver_rss = spark.read.format("delta").load("lakehouse_data/silver/saham_rss")
print(f"Silver API : {silver_api.count()} rows")
print(f"Silver RSS : {silver_rss.count()} rows")

# =============================================
# GOLD TABLE 1: RINGKASAN PER TICKER
# Reproduksi dari ETS: rata-rata harga, total volume, avg return
# =============================================
print("\n--- Building Gold Table 1: Summary Per Ticker ---")

gold_summary = silver_api.groupBy("ticker").agg(
    spark_round(avg("harga"), 2).alias("avg_harga"),
    spark_round(spark_min("harga"), 2).alias("min_harga"),
    spark_round(spark_max("harga"), 2).alias("max_harga"),
    spark_round(avg("open"), 2).alias("avg_open"),
    spark_round(avg("high"), 2).alias("avg_high"),
    spark_round(avg("low"), 2).alias("avg_low"),
    spark_round(spark_sum("volume"), 0).alias("total_volume"),
    spark_round(avg("volume"), 0).alias("avg_volume"),
    spark_round(avg("return_pct"), 4).alias("avg_return_pct"),
    spark_round(avg("price_range"), 2).alias("avg_price_range"),
    count("ticker").alias("jumlah_data_points"),
    spark_sum(col("is_outlier").cast("int")).alias("jumlah_outlier")
).orderBy(desc("avg_harga"))

print("Gold Table 1 - Summary Per Ticker:")
gold_summary.show(truncate=False)

gold_summary.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("lakehouse_data/gold/summary_per_ticker")
print("Saved: lakehouse_data/gold/summary_per_ticker")

# =============================================
# GOLD TABLE 2: TOP MOVER (RETURN TERTINGGI & TERENDAH)
# Reproduksi dari ETS: ticker terbaik dan terburuk per hari
# =============================================
print("\n--- Building Gold Table 2: Top Mover Per Hari ---")

silver_with_date = silver_api.withColumn("tanggal", to_date(col("timestamp")))

window_by_date = Window.partitionBy("tanggal").orderBy(desc("return_pct"))
window_by_date_asc = Window.partitionBy("tanggal").orderBy(asc("return_pct"))

ranked_top = silver_with_date.withColumn("rank_return", rank().over(window_by_date))
ranked_bottom = silver_with_date.withColumn("rank_return_asc", rank().over(window_by_date_asc))

top_gainer = ranked_top.filter(col("rank_return") == 1).select(
    col("tanggal"),
    col("ticker").alias("top_gainer_ticker"),
    spark_round(col("return_pct"), 4).alias("top_gainer_return_pct"),
    spark_round(col("harga"), 2).alias("top_gainer_harga")
)

top_loser = ranked_bottom.filter(col("rank_return_asc") == 1).select(
    col("tanggal"),
    col("ticker").alias("top_loser_ticker"),
    spark_round(col("return_pct"), 4).alias("top_loser_return_pct"),
    spark_round(col("harga"), 2).alias("top_loser_harga")
)

gold_top_mover = top_gainer.join(top_loser, on="tanggal", how="outer").orderBy("tanggal")

print("Gold Table 2 - Top Mover Per Hari:")
gold_top_mover.show(truncate=False)

gold_top_mover.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("lakehouse_data/gold/top_mover_harian")
print("Saved: lakehouse_data/gold/top_mover_harian")

# =============================================
# RINGKASAN GOLD LAYER
# =============================================
print("\n" + "=" * 60)
print("GOLD LAYER SUMMARY")
print("=" * 60)

g1 = spark.read.format("delta").load("lakehouse_data/gold/summary_per_ticker")
g2 = spark.read.format("delta").load("lakehouse_data/gold/top_mover_harian")

print(f"gold/summary_per_ticker  : {g1.count()} rows, {len(g1.columns)} columns")
print(f"gold/top_mover_harian    : {g2.count()} rows, {len(g2.columns)} columns")
print("\nGold layer complete. Flask dashboard bisa baca dari path di atas.")

spark.stop()