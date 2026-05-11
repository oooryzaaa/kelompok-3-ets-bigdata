import os
import json
import time
import traceback
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, TimestampType
)
import logging
import warnings

# Suppress semua warning
warnings.filterwarnings("ignore")

# Suppress log Spark yang verbose
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)

# ── Konfigurasi HDFS ──────────────────────────────────────────────────────────
HDFS_HOST      = "100.74.49.87"
HDFS_PORT      = 8020
HDFS_BASE      = f"hdfs://{HDFS_HOST}:{HDFS_PORT}"
HDFS_API_DIR   = f"{HDFS_BASE}/data/saham/api/"
HDFS_RSS_DIR   = f"{HDFS_BASE}/data/saham/rss/"
HDFS_HASIL_DIR = f"{HDFS_BASE}/data/saham/hasil/"

# ── Output lokal ──────────────────────────────────────────────────────────────
DASHBOARD_DIR  = Path(__file__).resolve().parent.parent / "dashboard" / "data"
DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_JSON    = DASHBOARD_DIR / "spark_results.json"

# ── Perusahaan target untuk Analisis 3 ────────────────────────────────────────
TARGET_COMPANIES = {
    "BCA"    : ["BCA", "Bank Central Asia"],
    "BRI"    : ["BRI", "Bank Rakyat Indonesia"],
    "Telkom" : ["Telkom", "TLKM"],
    "Astra"  : ["Astra", "ASII"],
    "Mandiri": ["Mandiri", "Bank Mandiri"],
}

schema_saham = StructType([
    StructField("ticker",       StringType(),  True),
    StructField("harga",        DoubleType(),  True),
    StructField("open",         DoubleType(),  True),
    StructField("high",         DoubleType(),  True),
    StructField("low",          DoubleType(),  True),
    StructField("volume",       LongType(),    True),
    StructField("change_pct",   DoubleType(),  True),
    StructField("is_simulated", StringType(),  True),
    StructField("timestamp",    StringType(),  True),
])

schema_berita = StructType([
    StructField("id",           StringType(), True),
    StructField("judul",        StringType(), True),
    StructField("ringkasan",    StringType(), True),
    StructField("sentimen",     StringType(), True),
    StructField("sumber",       StringType(), True),
    StructField("timestamp",    StringType(), True),
    StructField("url",          StringType(), True),
    StructField("waktu_terbit", StringType(), True),
])

def hdfs_path_exists(spark, hdfs_path: str) -> bool:
    try:
        jvm  = spark._jvm
        conf = spark._jsc.hadoopConfiguration()
        fs   = jvm.org.apache.hadoop.fs.FileSystem.get(
                   jvm.java.net.URI(hdfs_path), conf)
        return fs.exists(jvm.org.apache.hadoop.fs.Path(hdfs_path))
    except Exception as e:
        print(f"⚠️  HDFS exists check gagal: {e}")
        return False

def hdfs_list_files(spark, hdfs_path: str) -> list[str]:
    try:
        jvm    = spark._jvm
        conf   = spark._jsc.hadoopConfiguration()
        fs     = jvm.org.apache.hadoop.fs.FileSystem.get(
                     jvm.java.net.URI(hdfs_path), conf)
        status = fs.listStatus(jvm.org.apache.hadoop.fs.Path(hdfs_path))
        paths  = [s.getPath().toString() for s in status]
        return paths
    except Exception as e:
        print(f"⚠️  HDFS list gagal untuk {hdfs_path}: {e}")
        return []

def buat_dummy_saham(spark):
    return spark.createDataFrame([], schema=schema_saham)

def buat_dummy_berita(spark):
    return spark.createDataFrame([], schema=schema_berita)

def simpan_ke_hdfs(df, hdfs_path: str, label: str):
    try:
        (
            df.coalesce(1)            
            .write
            .mode("overwrite")
            .option("encoding", "UTF-8")
            .json(hdfs_path)
        )
        print(f"   ✅ [{label}] tersimpan ke {hdfs_path}")
        return True
    except Exception as e:
        print(f"   ❌ [{label}] gagal simpan ke HDFS: {e}")
        return False

def run_analysis(spark):
    print(f"[{datetime.now()}] Memulai analisis spark otomatis...")
    
    # ── Baca Data Saham ──
    df_saham = None
    SAHAM_DARI_HDFS = False
    try:
        if not hdfs_path_exists(spark, HDFS_API_DIR):
            raise FileNotFoundError(f"Path tidak ditemukan: {HDFS_API_DIR}")
        
        file_list  = hdfs_list_files(spark, HDFS_API_DIR)
        json_files = [f for f in file_list if f.endswith(".json")]
        
        if not json_files:
            raise ValueError("Tidak ada file .json di direktori HDFS API")
            
        df_saham = (
            spark.read
            .option("multiLine", "true")
            .option("mode", "PERMISSIVE")
            .schema(schema_saham)
            .json(HDFS_API_DIR + "*.json")
        )
        
        if df_saham.count() > 0:
            SAHAM_DARI_HDFS = True
        else:
            df_saham = buat_dummy_saham(spark)
    except Exception as e:
        print(f"   ❌ Gagal baca saham HDFS: {e}")
        df_saham = buat_dummy_saham(spark)
        
    df_saham = df_saham.dropna(subset=["ticker", "harga", "open"])
    
    # Update live API file untuk Dashboard
    try:
        with open(DASHBOARD_DIR / "live_api.json", "w", encoding="utf-8") as f:
            json.dump(df_saham.toPandas().to_dict(orient="records"), f, ensure_ascii=False, indent=2, default=str)
    except Exception as e:
        print(f"   ⚠️ Gagal update live_api.json: {e}")

    # ── Analisis 1: Return ──
    df_agg = df_saham.groupBy("ticker").agg(
        F.first("open",  ignorenulls=True).alias("harga_awal"),
        F.last("harga",  ignorenulls=True).alias("harga_terkini"),
        F.max("high").alias("harga_tertinggi"),
        F.min("low").alias("harga_terendah"),
        F.sum("volume").alias("total_volume"),
        F.last("timestamp", ignorenulls=True).alias("timestamp_terakhir"),
    )
    
    df_return = df_agg.withColumn(
        "return_pct",
        F.round(
            (F.col("harga_terkini") - F.col("harga_awal")) / F.col("harga_awal") * 100,
            4
        )
    ).withColumn(
        "status",
        F.when(F.col("return_pct") > 0, "NAIK")
         .when(F.col("return_pct") < 0, "TURUN")
         .otherwise("FLAT")
    ).orderBy(F.col("return_pct").desc())
    
    result_return = df_return.toPandas().to_dict(orient="records")

    # ── Analisis 2: Volatilitas ──
    df_volatilitas = df_saham.groupBy("ticker").agg(
        F.round(F.stddev("harga"), 4).alias("stddev_harga"),
        F.round(F.avg("harga"), 4).alias("rata_rata_harga"),
        F.max("high").alias("high"),
        F.min("low").alias("low"),
        F.count("*").alias("jumlah_snapshot"),
        F.last("timestamp", ignorenulls=True).alias("timestamp_terakhir"),
    ).withColumn(
        "range_intraday",
        F.round(F.col("high") - F.col("low"), 2)
    ).withColumn(
        "cv_pct",
        F.round(
            F.when(F.col("rata_rata_harga") > 0,
                   F.col("stddev_harga") / F.col("rata_rata_harga") * 100)
             .otherwise(0.0),
            4
        )
    ).withColumn(
        "level_volatilitas",
        F.when(F.col("cv_pct") > 2.0, "TINGGI")
         .when(F.col("cv_pct") > 0.5, "SEDANG")
         .otherwise("RENDAH")
    ).orderBy(F.col("cv_pct").desc())
    
    result_volatilitas = df_volatilitas.toPandas().fillna(0).to_dict(orient="records")

    # ── Baca Data Berita ──
    df_berita = None
    BERITA_DARI_HDFS = False
    try:
        if not hdfs_path_exists(spark, HDFS_RSS_DIR):
            raise FileNotFoundError(f"Path tidak ditemukan: {HDFS_RSS_DIR}")
            
        file_list  = hdfs_list_files(spark, HDFS_RSS_DIR)
        json_files = [f for f in file_list if f.endswith(".json")]
        
        if not json_files:
            raise ValueError("Tidak ada file .json di direktori HDFS RSS")
            
        df_berita = (
            spark.read
            .option("multiLine", "true")
            .option("mode", "PERMISSIVE")
            .schema(schema_berita)
            .json(HDFS_RSS_DIR + "*.json")
        )
        if df_berita.count() > 0:
            BERITA_DARI_HDFS = True
        else:
            df_berita = buat_dummy_berita(spark)
    except Exception as e:
        print(f"   ❌ Gagal baca berita HDFS: {e}")
        df_berita = buat_dummy_berita(spark)
        
    df_berita = df_berita.dropna(subset=["judul"])

    # Update live RSS file untuk Dashboard
    try:
        with open(DASHBOARD_DIR / "live_rss.json", "w", encoding="utf-8") as f:
            json.dump(df_berita.toPandas().to_dict(orient="records"), f, ensure_ascii=False, indent=2, default=str)
    except Exception as e:
        print(f"   ⚠️ Gagal update live_rss.json: {e}")

    # ── Analisis 3: Frekuensi Berita ──
    df_tagged = df_berita
    for company, keywords in TARGET_COMPANIES.items():
        pattern  = "|".join(keywords)
        col_name = f"mention_{company.lower()}"
        df_tagged = df_tagged.withColumn(
            col_name,
            F.when(
                F.regexp_extract(F.upper(F.col("judul")), pattern.upper(), 0) != "",
                1
            ).otherwise(0)
        )
        
    mention_counts = {}
    for company in TARGET_COMPANIES:
        col_name = f"mention_{company.lower()}"
        try:
            count = df_tagged.agg(F.sum(col_name).cast("long")).collect()[0][0] or 0
        except:
            count = 0
        mention_counts[company] = int(count)
        
    rows_frekuensi = [
        (company, int(count), list(TARGET_COMPANIES[company]))
        for company, count in sorted(mention_counts.items(), key=lambda x: -x[1])
    ]
    
    df_frekuensi = spark.createDataFrame(
        [(r[0], r[1]) for r in rows_frekuensi],
        schema=StructType([
            StructField("perusahaan",     StringType(), True),
            StructField("jumlah_sebutan", LongType(),   True),
        ])
    )
    
    result_frekuensi = [
        {"perusahaan": r[0], "jumlah_sebutan": r[1], "keywords": r[2]}
        for r in rows_frekuensi
    ]

    # ── Simpan Hasil ──
    output_payload = {
        "metadata": {
            "project"       : "SahamMeter",
            "kelompok"      : 3,
            "generated_at"  : datetime.now().isoformat(),
            "spark_version" : spark.version,
            "saham_dari_hdfs" : SAHAM_DARI_HDFS,
            "berita_dari_hdfs": BERITA_DARI_HDFS,
            "hdfs_base"     : HDFS_BASE,
        },
        "analisis_1_return": result_return,
        "analisis_2_volatilitas": result_volatilitas,
        "analisis_3_frekuensi_berita": result_frekuensi,
    }
    
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output_payload, f, ensure_ascii=False, indent=2, default=str)
        
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    simpan_ke_hdfs(df_return,      f"{HDFS_HASIL_DIR}return_{timestamp_str}",      "Return Saham")
    simpan_ke_hdfs(df_volatilitas, f"{HDFS_HASIL_DIR}volatilitas_{timestamp_str}", "Volatilitas")
    simpan_ke_hdfs(df_frekuensi,   f"{HDFS_HASIL_DIR}frekuensi_{timestamp_str}",   "Frekuensi Berita")
    
    print(f"[{datetime.now()}] Selesai analisis & simpan.")

def main():
    spark = (
        SparkSession.builder
        .appName("SahamMeter-ETS-AutoAnalysis")
        .config("spark.hadoop.dfs.datanode.hostname",                "100.74.49.87")
        .config("spark.hadoop.dfs.client.use.datanode.hostname",     "true")
        .config("spark.hadoop.dfs.datanode.use.datanode.hostname",   "true")
        .config("spark.hadoop.fs.hdfs.impl",
                "org.apache.hadoop.hdfs.DistributedFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.hdfs.impl",
                "org.apache.hadoop.fs.Hdfs")
        .config("spark.hadoop.dfs.client.socket-timeout",            "10000")
        .config("spark.hadoop.ipc.client.connect.timeout",           "10000")
        .config("spark.network.timeout",                             "120s")
        .config("spark.sql.legacy.timeParserPolicy",                 "LEGACY")
        .config("spark.sql.jsonGenerator.ignoreNullFields",          "false")
        .config("spark.hadoop.fs.hdfs.impl.disable.cache",           "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"✅ Auto Analysis SparkSession aktif : {spark.version}")
    
    while True:
        try:
            run_analysis(spark)
        except Exception as e:
            print(f"❌ Error saat analisis: {e}")
            traceback.print_exc()
            
        time.sleep(5)

if __name__ == "__main__":
    main()
