import os
import json
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['hadoop.home.dir'] = "C:\\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, avg, sum as spark_sum, count, max as spark_max, min as spark_min,
    round as spark_round, to_date, rank, desc, asc,
    lag, when
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

silver_api = spark.read.format("delta").load(
    "lakehouse_data/silver/saham_api"
)

silver_rss = spark.read.format("delta").load(
    "lakehouse_data/silver/saham_rss"
)

print(f"Silver API : {silver_api.count()} rows")
print(f"Silver RSS : {silver_rss.count()} rows")

# =============================================
# GOLD TABLE 1: RINGKASAN PER TICKER
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
# GOLD TABLE 2: TOP MOVER PER HARI
# =============================================
print("\n--- Building Gold Table 2: Top Mover Per Hari ---")

silver_with_date = silver_api.withColumn(
    "tanggal",
    to_date(col("timestamp"))
)

window_by_date = Window.partitionBy("tanggal").orderBy(
    desc("return_pct")
)

window_by_date_asc = Window.partitionBy("tanggal").orderBy(
    asc("return_pct")
)

ranked_top = silver_with_date.withColumn(
    "rank_return",
    rank().over(window_by_date)
)

ranked_bottom = silver_with_date.withColumn(
    "rank_return_asc",
    rank().over(window_by_date_asc)
)

top_gainer = ranked_top.filter(
    col("rank_return") == 1
).select(
    col("tanggal"),
    col("ticker").alias("top_gainer_ticker"),
    spark_round(col("return_pct"), 4).alias("top_gainer_return_pct"),
    spark_round(col("harga"), 2).alias("top_gainer_harga")
)

top_loser = ranked_bottom.filter(
    col("rank_return_asc") == 1
).select(
    col("tanggal"),
    col("ticker").alias("top_loser_ticker"),
    spark_round(col("return_pct"), 4).alias("top_loser_return_pct"),
    spark_round(col("harga"), 2).alias("top_loser_harga")
)

gold_top_mover = top_gainer.join(
    top_loser,
    on="tanggal",
    how="outer"
).orderBy("tanggal")

print("Gold Table 2 - Top Mover Per Hari:")
gold_top_mover.show(truncate=False)

gold_top_mover.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("lakehouse_data/gold/top_mover_harian")

print("Saved: lakehouse_data/gold/top_mover_harian")

# =============================================
# GOLD TABLE 3: MOMENTUM + ANOMALI SAHAM
# =============================================
print("\n--- Building Gold Table 3: Momentum & Anomaly Detection ---")

window_ticker = Window.partitionBy("ticker").orderBy("timestamp")

gold_momentum = silver_api.withColumn(
    "prev_harga",
    lag("harga").over(window_ticker)
).withColumn(
    "delta_harga",
    spark_round(col("harga") - col("prev_harga"), 2)
).withColumn(
    "momentum",
    when(col("delta_harga") > 0, "NAIK")
    .when(col("delta_harga") < 0, "TURUN")
    .otherwise("STABIL")
).withColumn(
    "volume_rank",
    rank().over(
        Window.partitionBy("ticker").orderBy(desc("volume"))
    )
)

gold_momentum_final = gold_momentum.select(
    "ticker",
    "timestamp",
    "harga",
    "prev_harga",
    "delta_harga",
    "momentum",
    "volume",
    "volume_rank",
    "z_score",
    "is_outlier"
)

print("Gold Table 3 - Momentum & Anomaly:")
gold_momentum_final.show(truncate=False)

gold_momentum_final.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("lakehouse_data/gold/momentum_anomali")

print("Saved: lakehouse_data/gold/momentum_anomali")

# =============================================
# GOLD TABLE 4: SENTIMENT vs MARKET MOVEMENT
# =============================================
print("\n--- Building Gold Table 4: Sentiment vs Market Movement ---")

api_join = silver_api.select(
    "ticker",
    "harga",
    "return_pct",
    "timestamp",
    "jam"
)

rss_join = silver_rss.select(
    "judul",
    "sumber",
    "sentimen",
    "waktu_terbit",
    "jam"
)

gold_sentiment_market = api_join.join(
    rss_join,
    on="jam",
    how="inner"
)

gold_sentiment_market = gold_sentiment_market.withColumn(
    "market_match",
    when(
        (col("sentimen") == "positif") &
        (col("return_pct") > 0),
        "SEJALAN_POSITIF"
    ).when(
        (col("sentimen") == "negatif") &
        (col("return_pct") < 0),
        "SEJALAN_NEGATIF"
    ).otherwise("TIDAK_SEJALAN")
)

gold_sentiment_market_final = gold_sentiment_market.select(
    "ticker",
    "harga",
    "return_pct",
    "judul",
    "sumber",
    "sentimen",
    "market_match",
    "timestamp",
    "waktu_terbit",
    "jam"
)

print("Gold Table 4 - Sentiment vs Market:")
gold_sentiment_market_final.show(truncate=False)

gold_sentiment_market_final.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("lakehouse_data/gold/sentiment_market")

print("Saved: lakehouse_data/gold/sentiment_market")

# =============================================
# RINGKASAN GOLD LAYER
# =============================================
print("\n" + "=" * 60)
print("GOLD LAYER SUMMARY")
print("=" * 60)

g1 = spark.read.format("delta").load(
    "lakehouse_data/gold/summary_per_ticker"
)

g2 = spark.read.format("delta").load(
    "lakehouse_data/gold/top_mover_harian"
)

g3 = spark.read.format("delta").load(
    "lakehouse_data/gold/momentum_anomali"
)

g4 = spark.read.format("delta").load(
    "lakehouse_data/gold/sentiment_market"
)

print(f"gold/summary_per_ticker : {g1.count()} rows, {len(g1.columns)} columns")
print(f"gold/top_mover_harian   : {g2.count()} rows, {len(g2.columns)} columns")
print(f"gold/momentum_anomali   : {g3.count()} rows, {len(g3.columns)} columns")
print(f"gold/sentiment_market   : {g4.count()} rows, {len(g4.columns)} columns")

# =============================================
# EXPORT GOLD KE JSON UNTUK FLASK DASHBOARD
# =============================================
print("\nExporting Gold tables to JSON for Flask...")

os.makedirs("data", exist_ok=True)

# Gold Table 1
gold_return_data = g1.toPandas().to_dict(orient="records")

with open(
    "data/gold_return.json",
    "w",
    encoding="utf-8"
) as f:
    json.dump(
        gold_return_data,
        f,
        ensure_ascii=False,
        indent=2,
        default=str
    )

print("Exported: data/gold_return.json")

# Gold Table 2
gold_volatilitas_data = g2.toPandas().to_dict(orient="records")

with open(
    "data/gold_volatilitas.json",
    "w",
    encoding="utf-8"
) as f:
    json.dump(
        gold_volatilitas_data,
        f,
        ensure_ascii=False,
        indent=2,
        default=str
    )

print("Exported: data/gold_volatilitas.json")

# Gold Table 3
gold_momentum_data = g3.toPandas().to_dict(orient="records")

with open(
    "data/gold_momentum.json",
    "w",
    encoding="utf-8"
) as f:
    json.dump(
        gold_momentum_data,
        f,
        ensure_ascii=False,
        indent=2,
        default=str
    )

print("Exported: data/gold_momentum.json")

# Gold Table 4
gold_sentiment_data = g4.toPandas().to_dict(orient="records")

with open(
    "data/gold_sentiment_market.json",
    "w",
    encoding="utf-8"
) as f:
    json.dump(
        gold_sentiment_data,
        f,
        ensure_ascii=False,
        indent=2,
        default=str
    )

print("Exported: data/gold_sentiment_market.json")

print("\nGold layer complete. Sekarang jalankan Flask: python app.py")

# spark.stop() selalu paling bawah!
spark.stop()