import json
import os
import threading
import time
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient

# ============================================================
# KONFIGURASI
# ============================================================

KAFKA_BROKER   = "100.74.49.87:9092"
TOPIC_API      = "saham-api"
TOPIC_RSS      = "saham-rss"
INTERVAL       = 300

HDFS_URL       = "http://100.74.49.87:9870"
HDFS_USER      = "root"
HDFS_PATH_API  = "/data/saham/api"
HDFS_PATH_RSS  = "/data/saham/rss"

LOCAL_API_JSON = "dashboard/data/live_api.json"
LOCAL_RSS_JSON = "dashboard/data/live_rss.json"

os.makedirs("dashboard/data", exist_ok=True)

hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)

# ============================================================
# BUFFER
# ============================================================

buffer_api = []
buffer_rss = []
lock_api   = threading.Lock()
lock_rss   = threading.Lock()

# ============================================================
# SIMPAN KE HDFS
# ============================================================

def simpan_ke_hdfs(data_snapshot, hdfs_path, label):
    if not data_snapshot:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {label}: buffer kosong, skip.")
        return

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    nama_file = f"{timestamp}.json"
    path_hdfs = f"{hdfs_path}/{nama_file}"
    konten    = json.dumps(data_snapshot, ensure_ascii=False, indent=2)

    try:
        with hdfs_client.write(path_hdfs, encoding="utf-8", overwrite=True) as f:
            f.write(konten)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {label}: {len(data_snapshot)} record -> HDFS {path_hdfs}")
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Gagal simpan HDFS {label}: {e}")

# ============================================================
# UPDATE FILE LOKAL UNTUK DASHBOARD
# ============================================================

def update_lokal(data_snapshot, path_json):
    if not data_snapshot:
        return
    with open(path_json, "w") as f:
        json.dump(data_snapshot[-50:], f, ensure_ascii=False, indent=2)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Lokal diupdate: {path_json}")

# ============================================================
# CONSUMER THREAD
# ============================================================

def consume_topic(topic, buffer, lock):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER,
        group_id=f"consumer-{topic}",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000,
    )

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Consumer {topic} siap.")

    while True:
        try:
            for message in consumer:
                with lock:
                    buffer.append(message.value)
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Error {topic}: {e}")
        time.sleep(1)

# ============================================================
# MAIN
# ============================================================

print("Consumer to HDFS siap. Mulai membaca dari Kafka...")

thread_api = threading.Thread(target=consume_topic, args=(TOPIC_API, buffer_api, lock_api), daemon=True)
thread_rss = threading.Thread(target=consume_topic, args=(TOPIC_RSS, buffer_rss, lock_rss), daemon=True)

thread_api.start()
thread_rss.start()

while True:
    time.sleep(INTERVAL)

    with lock_api:
        snapshot_api = buffer_api.copy()
        buffer_api.clear()

    with lock_rss:
        snapshot_rss = buffer_rss.copy()
        buffer_rss.clear()

    simpan_ke_hdfs(snapshot_api, HDFS_PATH_API, "api")
    simpan_ke_hdfs(snapshot_rss, HDFS_PATH_RSS, "rss")

    update_lokal(snapshot_api, LOCAL_API_JSON)
    update_lokal(snapshot_rss, LOCAL_RSS_JSON)
