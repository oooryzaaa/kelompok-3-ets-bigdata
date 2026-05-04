# ============================================
# SAHAM METER - Producer API (Upgrade: 10 Saham)
# ============================================

import yfinance as yf
import json
import time
import random
import logging
import warnings
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import os
os.environ["YFINANCE_NO_EXCEPTIONS"] = "1"

# Suppress yfinance internal warnings
warnings.filterwarnings("ignore")
logging.getLogger("yfinance").setLevel(logging.CRITICAL)
logging.getLogger("kafka").setLevel(logging.WARNING)

# ============================================
# 1. KONFIGURASI
# ============================================
KAFKA_BROKER = "100.74.49.87:9092"
KAFKA_TOPIC  = "saham-api"
INTERVAL     = 60

SAHAM_LIST = [
    "BBCA.JK", "BBRI.JK", "TLKM.JK", "ASII.JK", "BMRI.JK",
    "UNVR.JK", "GOTO.JK", "ICBP.JK", "INDF.JK", "PGAS.JK",
]

# ============================================
# 2. LOGGING
# ============================================
os.makedirs("kafka/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("kafka/logs/producer_api.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ============================================
# 3. INISIALISASI PRODUCER
# ============================================
def buat_producer():
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                enable_idempotence=True,
                acks="all",
                retries=5,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8")
            )
            log.info("✅ Kafka Producer berhasil terhubung!")
            return p
        except Exception as e:
            log.warning(f"⚠️ Gagal konek ke Kafka: {e}. Coba lagi 10 detik...")
            time.sleep(10)

producer = buat_producer()

# ============================================
# 4. CEK JAM BURSA IDX
# ============================================
def jam_bursa():
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    if now.hour < 9 or now.hour > 15:
        return False
    if now.hour == 15 and now.minute > 30:
        return False
    return True

# ============================================
# 5. FETCH HARGA REAL (jam bursa)
# ============================================
def fetch_harga(ticker):
    try:
        saham = yf.Ticker(ticker)
        hist  = saham.history(period="1d", interval="1m")

        if hist.empty:
            log.warning(f"⚠️ {ticker}: data kosong, pakai simulator")
            return None

        last_price = round(float(hist["Close"].iloc[-1]), 2)
        open_price = round(float(hist["Open"].iloc[0]),   2)
        high_price = round(float(hist["High"].max()),     2)
        low_price  = round(float(hist["Low"].min()),      2)
        volume     = int(hist["Volume"].iloc[-1])

        change_pct = None
        if open_price and open_price != 0:
            change_pct = round((last_price - open_price) / open_price * 100, 4)

        return {
            "ticker":       ticker.replace(".JK", ""),
            "harga":        last_price,
            "open":         open_price,
            "high":         high_price,
            "low":          low_price,
            "change_pct":   change_pct,
            "volume":       volume,
            "timestamp":    datetime.now().isoformat(),
            "is_simulated": False
        }
    except Exception as e:
        log.error(f"❌ Error fetch {ticker}: {e}")
        return None

# ============================================
# 6. SIMULATOR (di luar jam bursa)
# ============================================
harga_basis = {
    "BBCA": 9500,  "BBRI": 4800,  "TLKM": 3900,
    "ASII": 5200,  "BMRI": 6100,  "UNVR": 2100,
    "GOTO": 88,    "ICBP": 11500, "INDF": 7200,
    "PGAS": 1450,
}
harga_sim = dict(harga_basis)

def simulate_harga(ticker):
    kode = ticker.replace(".JK", "")
    base = harga_sim.get(kode, 1000)

    perubahan  = random.uniform(-0.015, 0.015)
    harga_baru = round(base * (1 + perubahan), 2)
    harga_sim[kode] = harga_baru

    open_p     = round(base * random.uniform(0.99, 1.01), 2)
    high_p     = round(max(open_p, harga_baru) * random.uniform(1.0, 1.008), 2)
    low_p      = round(min(open_p, harga_baru) * random.uniform(0.992, 1.0),  2)
    change_pct = round((harga_baru - open_p) / open_p * 100, 4)

    return {
        "ticker":       kode,
        "harga":        harga_baru,
        "open":         open_p,
        "high":         high_p,
        "low":          low_p,
        "change_pct":   change_pct,
        "volume":       random.randint(500_000, 10_000_000),
        "timestamp":    datetime.now().isoformat(),
        "is_simulated": True
    }

# ============================================
# 7. KIRIM KE KAFKA
# ============================================
def kirim_ke_kafka(data):
    try:
        future = producer.send(
            KAFKA_TOPIC,
            key=data["ticker"],
            value=data
        )
        future.get(timeout=10)
    except KafkaError as e:
        log.error(f"❌ Gagal kirim {data['ticker']} ke Kafka: {e}")

# ============================================
# 8. MAIN LOOP
# ============================================
log.info("🚀 Producer API mulai berjalan... (10 saham)")

while True:
    mode = "LIVE" if jam_bursa() else "SIMULASI"
    log.info(f"--- Polling [{mode}] {datetime.now().strftime('%H:%M:%S')} ---")

    for ticker in SAHAM_LIST:
        # Langsung simulator kalau di luar jam bursa
        if jam_bursa():
            data = fetch_harga(ticker)
            if data is None:
                data = simulate_harga(ticker)
        else:
            data = simulate_harga(ticker)

        if data:
            kirim_ke_kafka(data)
            ikon = "📈" if not data.get("is_simulated") else "🤖"
            log.info(f"{ikon} {data['ticker']:5s} | Rp {data['harga']:>10,.2f} | "
                     f"change={data.get('change_pct',0):+.2f}% | "
                     f"vol={data.get('volume',0):,}")

    producer.flush()
    log.info(f"✅ Semua {len(SAHAM_LIST)} saham terkirim ke Kafka")
    log.info(f"⏳ Tunggu {INTERVAL // 60} menit...\n")
    time.sleep(INTERVAL)