# SahamMeter 📈
Sistem monitoring saham IDX real-time menggunakan Big Data Pipeline.

## Anggota Kelompok
| Bagian | Nama | Kontribusi |
|--------|------|------------|
| A - Infra/Docker | Oryza | Setup Hadoop, Kafka, HDFS, README |
| B - Producer API | Nadia | producer_api.py, yfinance, simulator |
| C - Producer RSS + Consumer | Jose | producer_rss.py, consumer_to_hdfs.py |
| D - Spark Analysis | Binar | spark/analysis.ipynb, 3 analisis |
| E - Dashboard | Gilang | dashboard/app.py, index.html |

## Arsitektur Sistem
yfinance API → producer_api.py ─┐
├─→ Kafka → consumer_to_hdfs.py → HDFS → Spark → Dashboard Flask
RSS Feed    → producer_rss.py ─┘

## Cara Menjalankan

### Prasyarat
- Docker & Docker Compose terinstall
- Python 3.8+
- pip install kafka-python yfinance feedparser pyspark flask

### 1. Setup Infrastruktur (jalankan sekali)
```bash
./setup.sh
```
Atau manual:
```bash
docker compose -f docker-compose-hadoop.yml up -d
sleep 30
docker exec namenode hdfs dfs -mkdir -p /data/saham/api
docker exec namenode hdfs dfs -mkdir -p /data/saham/rss
docker exec namenode hdfs dfs -mkdir -p /data/saham/hasil
docker compose -f docker-compose-kafka.yml up -d
sleep 20
docker exec kafka-broker kafka-topics.sh --create --topic saham-api --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec kafka-broker kafka-topics.sh --create --topic saham-rss --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 2. Jalankan Consumer (background)
```bash
python kafka/consumer_to_hdfs.py &
```

### 3. Jalankan Producers
```bash
python kafka/producer_api.py &
python kafka/producer_rss.py &
```

### 4. Verifikasi Data Masuk ke HDFS
```bash
# Tunggu 2-5 menit, lalu:
docker exec namenode hdfs dfs -ls /data/saham/api/
docker exec namenode hdfs dfs -ls /data/saham/rss/
```

### 5. Jalankan Spark Analysis
```bash
jupyter notebook spark/analysis.ipynb
# Jalankan semua cell
```

### 6. Jalankan Dashboard
```bash
python dashboard/app.py
# Buka http://localhost:5000
```

## Screenshot
<!-- Isi setelah demo berjalan -->
- [ ] HDFS Web UI (localhost:9870)
- [ ] Kafka consumer output
- [ ] Dashboard berjalan

## Tantangan & Refleksi
- **Tantangan**: 
- **Solusi**: 

