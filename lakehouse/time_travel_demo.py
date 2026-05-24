import os
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['hadoop.home.dir'] = "C:\\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, avg
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

builder = SparkSession.builder.appName("TimeTravel-SahamMeter") \
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

print("=" * 60)
print("TIME TRAVEL DEMO - DELTA LAKE (SahamMeter)")
print("=" * 60)

dt = DeltaTable.forPath(spark, SILVER_PATH)

print("\nHistory before any changes:")
dt.history().select("version", "timestamp", "operation").show(truncate=False)

# Snapshot versi 0
v0 = spark.read.format("delta").option("versionAsOf", 0).load(SILVER_PATH)
count_v0 = v0.count()
avg_harga_v0 = v0.select(avg("harga")).collect()[0][0]
print(f"\nVersion 0 - Initial Data")
print(f"Total rows  : {count_v0}")
print(f"Avg harga   : {round(avg_harga_v0, 2)}")

# Operasi 1 - UPDATE
print("\n" + "-" * 60)
print("Operation 1: UPDATE - flag outliers as REVIEWED")

dt.update(
    condition=col("is_outlier") == True,
    set={"z_score": lit(-99.0)}
)

v1 = spark.read.format("delta").option("versionAsOf", 1).load(SILVER_PATH)
print(f"Version 1 - Post UPDATE")
print(f"Total rows  : {v1.count()}")

# Operasi 2 - DELETE
print("\n" + "-" * 60)
print("Operation 2: DELETE - remove rows with return_pct NULL")

dt2 = DeltaTable.forPath(spark, SILVER_PATH)
dt2.delete(condition=col("ticker") == "GOTO")

v2 = spark.read.format("delta").option("versionAsOf", 2).load(SILVER_PATH)
count_v2 = v2.count()
print(f"Version 2 - Post DELETE")
print(f"Total rows  : {count_v2}")
print(f"Rows deleted: {count_v0 - count_v2}")

# History lengkap
print("\n" + "=" * 60)
print("Full table history:")
DeltaTable.forPath(spark, SILVER_PATH) \
    .history() \
    .select("version", "timestamp", "operation") \
    .show(truncate=False)

# Perbandingan lintas versi
print("=" * 60)
print("Cross-version comparison:")
print("=" * 60)

for ver in [0, 1, 2]:
    df_ver = spark.read.format("delta").option("versionAsOf", ver).load(SILVER_PATH)
    n = df_ver.count()
    avg_r = df_ver.select(avg("return_pct")).collect()[0][0]
    label = {0: "Initial", 1: "Post-UPDATE", 2: "Post-DELETE"}[ver]
    print(f"\nVersion {ver} - {label}")
    print(f"  Rows         : {n}")
    print(f"  Avg return % : {round(avg_r, 4) if avg_r else 'N/A'}")

# Rollback ke versi 0
print("\n" + "=" * 60)
print("Rollback to Version 0 (original data)")

v0_restore = spark.read.format("delta").option("versionAsOf", 0).load(SILVER_PATH)
v0_restore.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(SILVER_PATH)

final_count = spark.read.format("delta").load(SILVER_PATH).count()
print(f"Rollback complete. Current rows: {final_count} (matches version 0: {count_v0})")

print("\nTime Travel demo complete.")
spark.stop()
