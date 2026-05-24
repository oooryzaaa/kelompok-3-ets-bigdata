import os
import shutil
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['hadoop.home.dir'] = "C:\\hadoop"

spark = SparkSession.builder.appName("Bronze-SahamMeter").getOrCreate()

print("🚀 Memulai proses ingestion ke lapisan Bronze (Format PARQUET)...")

save_path_api = "lakehouse_data/bronze/saham_api"
save_path_rss = "lakehouse_data/bronze/saham_rss"

# ==========================================
# 2. INGEST DATA API SAHAM
# ==========================================
try:
    api_df = spark.read.option("multiLine", True).json("./data_dummy/live_api.json")
    bronze_api_df = api_df.withColumn("_ingested_at", current_timestamp()) \
                          .withColumn("_source", lit("api_yfinance"))
    
    os.makedirs(save_path_api, exist_ok=True)
    bronze_api_df.toPandas().to_parquet(f"{save_path_api}/data.parquet", index=False, coerce_timestamps="ms", use_deprecated_int96_timestamps=False, allow_truncated_timestamps=True)
    print("✅ Data API Saham berhasil diubah ke format Parquet (Bronze Layer)!")
except Exception as e:
    print(f"❌ Gagal memproses data API Saham: {e}")

# ==========================================
# 3. INGEST DATA RSS BERITA
# ==========================================
try:
    rss_df = spark.read.option("multiLine", True).json("./data_dummy/live_rss.json")
    bronze_rss_df = rss_df.withColumn("_ingested_at", current_timestamp()) \
                          .withColumn("_source", lit("rss_berita"))
    
    os.makedirs(save_path_rss, exist_ok=True)
    bronze_rss_df.toPandas().to_parquet(f"{save_path_rss}/data.parquet", index=False, coerce_timestamps="ms", use_deprecated_int96_timestamps=False, allow_truncated_timestamps=True)
    print("✅ Data RSS Berita berhasil diubah ke format Parquet (Bronze Layer)!")
except Exception as e:
    print(f"❌ Gagal memproses data RSS Berita: {e}")

print("🎉 Proses Bronze selesai! Kasih tahu tim kalau mereka bisa lanjut nge-run 02_silver.py")