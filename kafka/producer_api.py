# ============================================
# SAHAM METER - Producer API
# Nadia: fetch harga saham via yfinance
# ============================================

import yfinance as yf
import json
import time
from datetime import datetime
from kafka import KafkaProducer

# ============================================
# 1. KONFIGURASI
# ============================================
KAFKA_BROKER = "100.74.49.87:9092"  # nanti ganti dengan IP Oryza
KAFKA_TOPIC = "saham-api"
SAHAM_LIST = ["BBCA.JK", "BBRI.JK", "TLKM.JK", "ASII.JK", "BMRI.JK"]
INTERVAL = 300  # 5 menit = 300 detik

# ============================================
# 2. INISIALISASI PRODUCER
# ============================================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    enable_idempotence=True,
    acks="all",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

print("✅ Producer siap!")

# ============================================
# 3. CEK JAM BURSA
# ============================================
def jam_bursa():
    now = datetime.now()
    # Senin-Jumat, jam 09.00-15.30 WIB
    if now.weekday() >= 5:  # Sabtu/Minggu
        return False
    if now.hour < 9 or now.hour > 15:
        return False
    if now.hour == 15 and now.minute > 30:
        return False
    return True

# ============================================
# 4. FETCH HARGA SAHAM
# ============================================
def fetch_harga(ticker):
    try:
        saham = yf.Ticker(ticker)
        info = saham.fast_info
        
        data = {
            "ticker": ticker.replace(".JK", ""),  # BBCA.JK -> BBCA
            "harga": info.last_price,
            "volume": info.three_month_average_volume,
            "timestamp": datetime.now().isoformat()
        }
        return data
    except Exception as e:
        print(f"❌ Error fetch {ticker}: {e}")
        return None

# ============================================
# 5. SIMULATOR (untuk di luar jam bursa)
# ============================================
harga_terakhir = {
    "BBCA": 9500, "BBRI": 4800, 
    "TLKM": 3900, "ASII": 5200, "BMRI": 6100
}

import random
def simulate_harga(ticker):
    kode = ticker.replace(".JK", "")
    harga_lama = harga_terakhir[kode]
    # Naik/turun random ±1%
    perubahan = random.uniform(-0.01, 0.01)
    harga_baru = round(harga_lama * (1 + perubahan), 2)
    harga_terakhir[kode] = harga_baru
    
    return {
        "ticker": kode,
        "harga": harga_baru,
        "volume": random.randint(100000, 5000000),
        "timestamp": datetime.now().isoformat(),
        "is_simulated": True  # tandai bahwa ini data simulasi
    }

# ============================================
# 6. MAIN LOOP
# ============================================
print("🚀 Producer mulai berjalan...")

while True:
    for ticker in SAHAM_LIST:
        if jam_bursa():
            data = fetch_harga(ticker)
            print(f"📈 LIVE: {data}")
        else:
            data = simulate_harga(ticker)
            print(f"🤖 SIMULASI: {data}")
        
        if data:
            producer.send(
                KAFKA_TOPIC,
                key=data["ticker"],
                value=data
            )
    
    producer.flush()
    print(f"✅ {datetime.now().strftime('%H:%M:%S')} - Semua saham terkirim ke Kafka")
    print(f"⏳ Tunggu {INTERVAL//60} menit...")
    time.sleep(INTERVAL)