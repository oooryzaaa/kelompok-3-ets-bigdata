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
INTERVAL     = 60  # polling setiap 1 menit (60 detik)
MAKS_PER_SOURCE = 10  # maksimal artikel per sumber RSS

# Sesuai dengan Topik 4: SahamMeter (utama: bisnis.com, backup: cnn/kompas/tempo)
RSS_SOURCES = [
    "https://www.cnbcindonesia.com/market/rss",       
    "https://www.antaranews.com/rss/ekonomi-bisnis.xml",
    "https://www.cnnindonesia.com/ekonomi/rss",        
    "https://rss.tempo.co/bisnis",                     
]

# Path disesuaikan agar bisa jalan dari root folder
SENT_IDS_FILE = "kafka/sent_ids.txt"

# ============================================================
# INISIALISASI
# ============================================================

# Error handling jika Kafka belum nyala
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        enable_idempotence=True,
        acks="all",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    print(f"✅ Berhasil terhubung ke Kafka: {KAFKA_BROKER}")
except Exception as e:
    print(f"❌ Gagal terhubung ke Kafka. Pastikan Kafka sudah menyala!")
    print(f"Error: {e}")
    exit(1)

def load_sent_ids():
    # Buat foldernya jika belum ada
    os.makedirs(os.path.dirname(SENT_IDS_FILE), exist_ok=True)
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

KATA_POSITIF = ["naik", "bullish", "untung", "rekor", "tumbuh", "profit", "meningkat", "optimis", "lonjakan", "cuan", "hijau"]
KATA_NEGATIF = ["turun", "bearish", "rugi", "anjlok", "merosot", "koreksi", "jatuh", "pesimis", "jeblok", "merah", "ambruk"]

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
        try:
            feed = feedparser.parse(url)
            terkirim_source = 0

            # Cek jika feed gagal dimuat
            if hasattr(feed, 'status') and feed.status not in [200, 301, 302]:
                print(f"⚠️ Peringatan: Gagal mengambil RSS dari {url} (Status: {feed.status})")
                continue

            for entry in feed.entries:
                if terkirim_source >= MAKS_PER_SOURCE:
                    break

                article_url = getattr(entry, "link", "")
                article_id  = make_id(article_url)

                if article_id in sent_ids:
                    continue

                judul     = getattr(entry, "title", "Tanpa Judul")
                ringkasan = getattr(entry, "summary", "")
                waktu     = getattr(entry, "published", datetime.now().isoformat())
                
                # Ambil nama sumber berita (jika tidak ada fallback ke URL aslinya)
                sumber    = getattr(feed.feed, "title", url.split("/")[2]) 

                data = {
                    "id"          : article_id,
                    "judul"       : judul,
                    "url"         : article_url,
                    "ringkasan"   : ringkasan[:300], # Potong max 300 karakter
                    "sumber"      : sumber,
                    "sentimen"    : deteksi_sentimen(judul),
                    "waktu_terbit": waktu,
                    "timestamp"   : datetime.now().isoformat(),
                }

                # Kirim ke Kafka
                producer.send(KAFKA_TOPIC, key=article_id, value=data)
                
                # Simpan ke cache lokal agar tidak dikirim ganda di menit berikutnya
                sent_ids.add(article_id)
                save_sent_id(article_id)
                
                total_terkirim += 1
                terkirim_source += 1

                print(f"[{datetime.now().strftime('%H:%M:%S')}]  Terkirim: {judul[:60]}...")

        except Exception as e:
            print(f" Error saat mengambil RSS dari {url}: {e}")

    producer.flush() # Pastikan semua data benar-benar terkirim sebelum lanjut
    return total_terkirim

# ============================================================
# MAIN LOOP
# ============================================================

print("\n Producer RSS siap. Memulai penarikan data...")

while True:
    sent_ids = load_sent_ids()
    jumlah   = fetch_dan_kirim(sent_ids)
    
    if jumlah > 0:
        print(f" [{datetime.now().strftime('%H:%M:%S')}] {jumlah} artikel baru berhasil dikirim ke Kafka.")
    else:
        print(f" [{datetime.now().strftime('%H:%M:%S')}] Tidak ada berita baru. Standby...")
        
    print(f" Menunggu {INTERVAL} detik untuk penarikan berikutnya...\n")
    time.sleep(INTERVAL)