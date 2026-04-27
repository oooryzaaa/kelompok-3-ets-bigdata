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
[  Topic Kafka  ]
  - saham-api  → data harga saham (BBCA, BBRI, TLKM, ASII, BMRI)
  - saham-rss  → artikel berita pasar modal

[ yfinance API ]                               [ RSS Feed Berita ]
       │                                                │
       ▼                                                ▼
┌──────────────┐                               ┌────────────────┐
│ producer_api │                               │  producer_rss  │
└──────┬───────┘                               └────────┬───────┘
       │                                                │
       ▼                                                ▼
╔═══════════════════════════════════════════════════════════════╗
║                         APACHE KAFKA                          ║
║      (Topic: saham-api)               (Topic: saham-rss)      ║
╚═══════════════════════════════╤═══════════════════════════════╝
                                │
                                ▼
                        ┌───────────────┐
                        │   consumer_   │
                        │    to_hdfs    │
                        └───────┬───────┘
                                │
                                ▼
╔═══════════════════════════════════════════════════════════════╗
║                          HADOOP HDFS                          ║
║      /data/saham/api/                 /data/saham/rss/        ║
╚═══════════════════════════════╤═══════════════════════════════╝
                                │
                                ▼
                        ┌───────────────┐
                        │ Apache Spark  │
                        │ (analysis.py) │
                        └───────┬───────┘
                                │
                                ▼
                        ┌───────────────┐
                        │   Dashboard   │
                        │    (Flask)    │
                        └───────────────┘

##  Struktur Folder
saham-meter/
├── docker-compose-hadoop.yml
├── docker-compose-kafka.yml
├── hadoop.env
├── setup.sh
├── kafka/
│   ├── producer_api.py
│   ├── producer_rss.py
│   └── consumer_to_hdfs.py
├── spark/
│   └── analysis.ipynb
├── dashboard/
│   ├── app.py
│   ├── data/
│   │   ├── live_api.json
│   │   ├── live_rss.json
│   │   └── spark_results.json
│   └── templates/
│       └── index.html
└── README.md

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
# Jalankan Hadoop
docker compose -f docker-compose-hadoop.yml up -d
sleep 30

# Lalu buat direktori HDFS
docker exec namenode hdfs dfs -mkdir -p /data/saham/api
docker exec namenode hdfs dfs -mkdir -p /data/saham/rss
docker exec namenode hdfs dfs -mkdir -p /data/saham/hasil

# Jalankan Kafka
docker compose -f docker-compose-kafka.yml up -d
sleep 20

# Pengecekan docker 
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 

# Topic kafka
docker exec kafka-broker kafka-topics.sh --create --topic saham-api --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec kafka-broker kafka-topics.sh --create --topic saham-rss --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Verifikasi topics
docker exec kafka-broker kafka-topics --list --bootstrap-server localhost:9092
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
![alt text](image/namenode.png.png)
![alt text](<image/Direktori HDFS.png>)

- [ ] Kafka consumer output
![alt text](<image/docker image.png>)
![alt text](<image/kafka topics.png>)

- [ ] Dashboard berjalan

## Tantangan & Refleksi
- **Tantangan**: 
- **Solusi**: 

## Urutan Menjalankan Saat Demo
1. Buka Docker Desktop
2. Start Hadoop → Start Kafka → Verifikasi topics
3. Jalankan consumer_to_hdfs.py (background)
4. Jalankan producer_api.py + producer_rss.py
5. Tunggu ~5 menit → cek data masuk HDFS
6. Jalankan Spark analysis.ipynb
7. Jalankan dashboard app.py
8. Buka localhost:5000 → demo!
