import feedparser
import json
import time
import hashlib
import os
from datetime import datetime
from kafka import KafkaProducer

# ============================================================
# KONFIGURASI
# ============================================================

KAFKA_BROKER = "100.74.49.87:9092"
KAFKA_TOPIC  = "saham-rss"
INTERVAL     = 300  # polling setiap 5 menit

RSS_SOURCES = [
    "https://www.cnnindonesia.com/ekonomi/rss",
    "https://rss.kompas.com/feed/kompas.com/money",
	    "https://rss.tempo.co/nasional",

]

SENT_IDS_FILE = "kafka/sent_ids.txt"  # menyimpan ID artikel yang sudah dikirim

# ============================================================
# INISIALISASI
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    enable_idempotence=True,
    acks="all",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)


def load_sent_ids():
    if not os.path.exists(SENT_IDS_FILE):
        return set()
    with open(SENT_IDS_FILE, "r") as f:
        return set(line.strip() for line in f if line.strip())


def save_sent_id(article_id):
    with open(SENT_IDS_FILE, "a") as f:
        f.write(article_id + "\n")


def make_id(url):
    return hashlib.md5(url.encode()).hexdigest()[:8]


# ============================================================
# DETEKSI SENTIMEN SEDERHANA
# ============================================================

KATA_POSITIF = ["naik", "bullish", "untung", "rekor", "tumbuh", "profit", "meningkat", "optimis"]
KATA_NEGATIF = ["turun", "bearish", "rugi", "anjlok", "merosot", "koreksi", "jatuh", "pesimis"]

def deteksi_sentimen(judul):
    judul_lower = judul.lower()
    for kata in KATA_POSITIF:
        if kata in judul_lower:
            return "positif"
    for kata in KATA_NEGATIF:
        if kata in judul_lower:
            return "negatif"
    return "netral"


# ============================================================
# FETCH DAN KIRIM RSS
# ============================================================

def fetch_dan_kirim(sent_ids):
    total_terkirim = 0

    for url in RSS_SOURCES:
        feed = feedparser.parse(url)

        for entry in feed.entries:
            article_url = getattr(entry, "link", "")
            article_id  = make_id(article_url)

            if article_id in sent_ids:
                continue

            judul    = getattr(entry, "title", "")
            ringkasan = getattr(entry, "summary", "")
            waktu    = getattr(entry, "published", datetime.now().isoformat())
            sumber   = feed.feed.get("title", url)

            data = {
                "id"        : article_id,
                "judul"     : judul,
                "url"       : article_url,
                "ringkasan" : ringkasan[:300],
                "sumber"    : sumber,
                "sentimen"  : deteksi_sentimen(judul),
                "waktu_terbit": waktu,
                "timestamp" : datetime.now().isoformat(),
            }

            producer.send(KAFKA_TOPIC, key=article_id, value=data)
            sent_ids.add(article_id)
            save_sent_id(article_id)
            total_terkirim += 1

            print(f"[{datetime.now().strftime('%H:%M:%S')}] Terkirim: {judul[:60]}")

    producer.flush()
    return total_terkirim


# ============================================================
# MAIN LOOP
# ============================================================

print("Producer RSS siap. Mulai polling...")

while True:
    sent_ids = load_sent_ids()
    jumlah   = fetch_dan_kirim(sent_ids)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {jumlah} artikel baru dikirim. Tunggu {INTERVAL // 60} menit...")
    time.sleep(INTERVAL)
