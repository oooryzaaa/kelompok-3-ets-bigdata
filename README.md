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
![alt text](image/namenode.png)

- [ ] Kafka consumer output
![alt text](<image/docker image.png>)
![alt text](<image/kafka topics.png>)
![alt text](image/bigdata1.png)

- [ ] Dashboard berjalan
![alt text](image/dashboard.png)
![alt text](image/RSS_Berita.png)
![alt text](<image/Grafik Saham.png>)
![alt text](<image/Analisis Spark.png>)

## Urutan Menjalankan Saat Demo
1. Buka Docker Desktop
2. Start Hadoop → Start Kafka → Verifikasi topics
3. Jalankan consumer_to_hdfs.py (background)
4. Jalankan producer_api.py + producer_rss.py
5. Tunggu ~5 menit → cek data masuk HDFS
6. Jalankan Spark analysis.ipynb
7. Jalankan dashboard app.py
8. Buka localhost:5000 → demo!


## Bagian A: Infrastruktur Terdistribusi (Hadoop & Kafka)
Penanggung Jawab: Oryza Qiara Ramadhani

Bagian ini memuat konfigurasi fondasi Big Data untuk proyek SahamMeter. Seluruh environment dikontainerisasi menggunakan Docker untuk memastikan konsistensi antarpengembang dan mensimulasikan lingkungan cloud terdistribusi. Infrastruktur dibagi menjadi dua file Compose yang terpisah (hadoop dan kafka) untuk menerapkan prinsip High Availability dan Decoupled Architecture.

### Arsitektur Container
Infrastruktur ini menjalankan total 6 container yang terhubung dalam satu jaringan internal Docker:
- Storage Layer (Hadoop Cluster):
- namenode (HDFS Master) - Web UI: Port 9870
- datanode (HDFS Worker)
- resourcemanager (YARN Master)
- nodemanager (YARN Worker)
- Ingestion Layer (Kafka Cluster):
- zookeeper (Cluster Manager)
- kafka-broker (Message Broker) - Port 9092

Catatan: Seluruh data di-hosting terpusat dan dapat diakses oleh anggota tim secara remote melalui IP Tailscale 100.74.49.87.

### Struktur Data & Antrean
Sistem ini telah dikonfigurasi dengan jalur data (Single Source of Truth) sebagai berikut:

Kafka Topics:
- saham-api (Untuk data harga saham real-time Yahoo Finance)
- saham-rss (Untuk data teks berita dari portal RSS)

HDFS Directories:
- /data/saham/api/ (Penyimpanan mentah data API)
- /data/saham/rss/ (Penyimpanan mentah data RSS)
- /data/saham/hasil/ (Penyimpanan hasil analitik Apache Spark)

### Cara Menjalankan Infrastruktur
Pastikan Docker Desktop sudah berjalan di sistem (Windows/Mac/Linux). Untuk kemudahan operasional, kami telah menyediakan script otomatisasi menggunakan PowerShell.

Cara Otomatis (Sangat Direkomendasikan):
Buka terminal dan jalankan script berikut untuk mematikan container lama, menyalakan ulang secara sinkron, dan mengecek kesiapan Kafka:

` .\restart.ps1 `

Cara Manual:
Jika ingin menyalakan cluster secara terpisah, gunakan perintah berikut:

```
# Menyalakan Hadoop Cluster
docker compose -f docker-compose-hadoop.yml up -d

# Menyalakan Kafka Cluster
docker compose -f docker-compose-kafka.yml up -d
```

### Cara Verifikasi Sistem
Setelah infrastruktur menyala, lakukan pengecekan berikut untuk memastikan sistem berjalan normal sebelum menjalankan Producer dan Consumer:

1. Mengecek Status Kafka Topic

```
docker exec -it kafka-broker kafka-topics --list --bootstrap-server localhost:9092
```
(Ekspektasi output: Muncul daftar topic saham-api dan saham-rss)

2. Mengecek Status HDFS (Storage)
Buka browser dan akses HDFS Web UI melalui:
` http://100.74.49.87:9870 `
Arahkan ke menu Utilities > Browse the file system dan pastikan direktori /data/saham/ sudah terbentuk dengan baik.

## B - Producer API (Nadia)

### Deskripsi
Producer yang mengambil harga saham real-time dari yfinance dan mengirimkannya ke Kafka topic `saham-api` setiap 5 menit. Dilengkapi simulator otomatis untuk di luar jam bursa (09.00–15.30 WIB).

### File yang Dibuat
- `kafka/producer_api.py`

### Prasyarat
1. Tailscale sudah terinstall dan terhubung ke jaringan kelompok
2. Kafka sudah berjalan di laptop Oryza (Bagian A)
3. Topic `saham-api` sudah dibuat

### Instalasi
pip install kafka-python yfinance

### Konfigurasi
Buka `kafka/producer_api.py`, sesuaikan IP laptop Oryza:
KAFKA_BROKER = "100.74.49.87:9092"

### Cara Menjalankan
python kafka/producer_api.py

### Output yang Diharapkan
✅ Producer siap!

🚀 Producer mulai berjalan...

📈 LIVE: {'ticker': 'BBCA', 'harga': 6025.0, 'volume': 205154435, 'timestamp': '2026-04-27T14:02:27'}

📈 LIVE: {'ticker': 'BBRI', 'harga': 3090.0, 'volume': 254001573, 'timestamp': '2026-04-27T14:02:27'}

📈 LIVE: {'ticker': 'TLKM', 'harga': 2820.0, 'volume': 135196724, 'timestamp': '2026-04-27T14:02:28'}

📈 LIVE: {'ticker': 'ASII', 'harga': 6200.0, 'volume': 48836235,  'timestamp': '2026-04-27T14:02:28'}

📈 LIVE: {'ticker': 'BMRI', 'harga': 4410.0, 'volume': 185660877, 'timestamp': '2026-04-27T14:02:28'}

✅ 14:02:28 - Semua saham terkirim ke Kafka

⏳ Tunggu 5 menit...

### Verifikasi Data Masuk ke Kafka
Jalankan di laptop Oryza:
docker exec -it kafka-broker kafka-console-consumer --topic saham-api --from-beginning --bootstrap-server localhost:9092

### Catatan
- Jam bursa aktif: Senin–Jumat 09.00–15.30 WIB → data LIVE dari yfinance
- Di luar jam bursa → simulator otomatis aktif (harga naik/turun ±1%)
- Field is_simulated: true menandakan data hasil simulator
- Jalankan setelah Bagian A (infrastruktur) sudah aktif


# Producer RSS & Consumer to HDFS (Putri Joselina Silitonga)

Dokumentasi teknis untuk dua komponen pipeline SahamMeter: **Producer RSS** yang mengambil dan mempublikasikan berita pasar modal ke Kafka, serta **Consumer to HDFS** yang mengonsumsi pesan dari Kafka dan menyimpannya ke Hadoop HDFS.

---

## Daftar Isi

- [Producer RSS](#producer-rss)
- [Consumer to HDFS](#consumer-to-hdfs)
- [Bukti Sistem Berjalan](#bukti-sistem-berjalan)

---

## Producer RSS

### Deskripsi

Producer RSS bertugas melakukan polling berita keuangan dan pasar modal dari sumber RSS Indonesia secara periodik. Setiap artikel yang belum pernah dikirim akan dideteksi sentimennya secara otomatis, lalu dipublikasikan ke Kafka topic `saham-rss`. Sistem dirancang idempoten — artikel yang sudah terkirim tidak akan pernah dikirim ulang meskipun masih muncul di feed, bahkan setelah producer di-restart.

---

### Lokasi File

| File | Keterangan |
|------|------------|
| `kafka/producer_rss.py` | Script utama producer RSS |
| `kafka/sent_ids.txt` | Penyimpanan persisten ID artikel yang sudah dikirim |

---

### Dependensi

```bash
pip install kafka-python feedparser
```

---

### Konfigurasi

| Variabel | Nilai Default | Keterangan |
|----------|---------------|------------|
| `KAFKA_BROKER` | `100.74.49.87:9092` | IP broker Kafka via Tailscale |
| `KAFKA_TOPIC` | `saham-rss` | Nama topic Kafka tujuan |
| `INTERVAL` | `300` | Jeda antar polling dalam detik |
| `SENT_IDS_FILE` | `kafka/sent_ids.txt` | File penyimpanan ID artikel |

---

### Sumber RSS

| Media | URL Feed |
|-------|----------|
| CNN Indonesia (Ekonomi) | `https://www.cnnindonesia.com/ekonomi/rss` |
| Kompas Money | `https://rss.kompas.com/feed/kompas.com/money` |
| Tempo Nasional | `https://rss.tempo.co/nasional` |

---

### Cara Menjalankan

Pastikan Kafka broker aktif dan topic `saham-rss` sudah dibuat sebelum menjalankan script ini.

```bash
python kafka/producer_rss.py
```

---

### Alur Kerja

1. Memuat daftar ID artikel yang sudah dikirim dari `sent_ids.txt`
2. Melakukan fetch setiap URL feed menggunakan `feedparser`
3. Memeriksa setiap artikel — jika ID sudah ada di daftar, artikel dilewati
4. Menganalisis sentimen artikel berdasarkan kata kunci di judul
5. Mengemas data dalam format JSON dan mengirim ke Kafka
6. Menyimpan ID artikel baru ke `sent_ids.txt`
7. Menunggu 5 menit, lalu mengulang dari langkah 1

---

### Mekanisme Deduplikasi

Setiap artikel diidentifikasi menggunakan **MD5 hash 8 karakter dari URL-nya**. Hash disimpan secara persisten di `kafka/sent_ids.txt`. Artikel yang hash-nya sudah tercatat akan dilewati tanpa dikirim ulang.

**Contoh isi `kafka/sent_ids.txt`:**

```
a3f1e2c4
b9d2a7f1
c8e3d501
d4f7b293
```

---

### Deteksi Sentimen Otomatis

Sentimen dideteksi berdasarkan kata kunci yang ditemukan dalam judul artikel.

| Label | Kata Kunci Pemicu |
|-------|-------------------|
| `positif` | naik, bullish, untung, rekor, tumbuh, profit, meningkat, optimis |
| `negatif` | turun, bearish, rugi, anjlok, merosot, koreksi, jatuh, pesimis |
| `netral` | tidak ditemukan kata kunci dari kedua kategori di atas |

---

### Struktur Data yang Dikirim ke Kafka

```json
{
  "id": "a3f1e2c4",
  "judul": "BNI Catat Kinerja Solid Kuartal I 2026",
  "url": "https://www.cnnindonesia.com/ekonomi/...",
  "ringkasan": "PT Bank Negara Indonesia mencatat pertumbuhan laba bersih...",
  "sumber": "CNN Indonesia",
  "sentimen": "positif",
  "waktu_terbit": "Thu, 30 Apr 2026 01:30:00 +0700",
  "timestamp": "2026-04-30T01:44:10"
}
```

> Field `ringkasan` dibatasi maksimal **300 karakter** dari konten artikel.

---

### Contoh Output Terminal

```
Producer RSS siap. Mulai polling...
[01:44:10] Terkirim: BNI Catat Kinerja Solid Kuartal I 2026
[01:44:10] Terkirim: Prabowo Groundbreaking 13 Proyek Hilirisasi Senilai Rp 116 T
[01:44:10] Terkirim: Bus Jemaah Haji Indonesia di Madinah Mengalami Kecelakaan
[01:44:10] Terkirim: Fakta-fakta Terkini Kecelakaan Kereta di Bekasi Timur
[01:44:10] 150 artikel baru dikirim. Tunggu 5 menit...
[01:49:11] 0 artikel baru dikirim. Tunggu 5 menit...
```

> Output `0 artikel baru dikirim` pada siklus kedua membuktikan deduplikasi berjalan dengan benar.

---

### Verifikasi Data di Kafka

```bash
docker exec -it kafka-broker kafka-console-consumer.sh \
  --topic saham-rss \
  --from-beginning \
  --bootstrap-server localhost:9092
```

---

---

## Consumer to HDFS

### Deskripsi

Consumer membaca pesan secara paralel dari dua Kafka topic (`saham-api` dan `saham-rss`) menggunakan multi-threading, lalu menyimpan data ke Hadoop HDFS dalam format JSON bertimestamp setiap 5 menit. Data terbaru juga disalin ke file lokal yang digunakan langsung oleh dashboard Flask sebagai cache.

Arsitektur menggunakan **buffer berbasis thread** — dua thread berjalan paralel mengisi buffer masing-masing, sementara main thread menguras buffer secara terjadwal dan mem-flush hasilnya ke HDFS.

---

### Lokasi File

| File | Keterangan |
|------|------------|
| `kafka/consumer_to_hdfs.py` | Script utama consumer |
| `dashboard/data/live_api.json` | Cache lokal 50 data harga saham terbaru |
| `dashboard/data/live_rss.json` | Cache lokal 50 berita terbaru |

---

### Dependensi

```bash
pip install kafka-python hdfs
```

---

### Konfigurasi

| Variabel | Nilai Default | Keterangan |
|----------|---------------|------------|
| `KAFKA_BROKER` | `100.74.49.87:9092` | IP broker Kafka via Tailscale |
| `TOPIC_API` | `saham-api` | Topic Kafka data harga saham |
| `TOPIC_RSS` | `saham-rss` | Topic Kafka data berita |
| `HDFS_URL` | `http://100.74.49.87:9870` | URL NameNode HDFS |
| `HDFS_USER` | `root` | User autentikasi HDFS |
| `HDFS_PATH_API` | `/data/saham/api` | Path HDFS untuk data harga |
| `HDFS_PATH_RSS` | `/data/saham/rss` | Path HDFS untuk data berita |
| `INTERVAL` | `300` | Interval flush ke HDFS dalam detik |

---

### Cara Menjalankan

> **Penting:** Consumer harus dijalankan **sebelum** producers aktif agar tidak ada pesan yang terlewat.

```bash
python kafka/consumer_to_hdfs.py &
```

---

### Arsitektur Internal

| Komponen | Tugas |
|----------|-------|
| Thread 1 | Consume topic `saham-api` secara terus-menerus, isi `buffer_api` |
| Thread 2 | Consume topic `saham-rss` secara terus-menerus, isi `buffer_rss` |
| Main Thread | Setiap 5 menit: ambil snapshot buffer → tulis ke HDFS → update file lokal |

Setiap buffer dilindungi oleh `threading.Lock()` untuk mencegah race condition saat thread consumer dan main thread mengakses buffer secara bersamaan.

---

### Format Penyimpanan di HDFS

File disimpan dengan nama bertimestamp sehingga setiap siklus menghasilkan file baru tanpa menimpa data sebelumnya.

| Path HDFS | Contoh Nama File |
|-----------|------------------|
| `/data/saham/api/` | `2026-04-30_01-21-57.json` |
| `/data/saham/api/` | `2026-04-30_01-27-05.json` |
| `/data/saham/rss/` | `2026-04-30_01-22-04.json` |
| `/data/saham/rss/` | `2026-04-30_01-34-43.json` |

Setiap file berisi array JSON dari semua pesan yang masuk selama interval 5 menit terakhir.

---

### Output Lokal untuk Dashboard

| File | Isi | Digunakan Oleh |
|------|-----|----------------|
| `dashboard/data/live_api.json` | 50 data harga saham terbaru | `dashboard/app.py` |
| `dashboard/data/live_rss.json` | 50 berita terbaru | `dashboard/app.py` |

File ini memungkinkan Flask membaca data terbaru tanpa perlu query langsung ke HDFS setiap request.

---

### Contoh Output Terminal

```
Consumer to HDFS siap. Mulai membaca dari Kafka...
[01:21:55] Consumer saham-api siap.
[01:21:55] Consumer saham-rss siap.
[01:26:57] api: 10 record -> HDFS /data/saham/api/2026-04-30_01-21-57.json
[01:26:57] rss: 150 record -> HDFS /data/saham/rss/2026-04-30_01-22-04.json
[01:26:57] Lokal diupdate: dashboard/data/live_api.json
[01:26:57] Lokal diupdate: dashboard/data/live_rss.json
[01:31:57] api: 10 record -> HDFS /data/saham/api/2026-04-30_01-27-05.json
[01:31:57] rss: 0 record, skip.
```

---

### Verifikasi Data di HDFS

```bash
# Cek daftar file yang tersimpan
docker exec namenode hdfs dfs -ls /data/saham/api/
docker exec namenode hdfs dfs -ls /data/saham/rss/

# Baca isi file terbaru
docker exec namenode hdfs dfs -cat /data/saham/api/2026-04-30_01-48-37.json | head -30
```

---

### Catatan Teknis

- Consumer menggunakan `auto_offset_reset="earliest"` — jika di-restart, semua pesan lama dibaca ulang dari awal. Berguna untuk recovery, namun dapat menghasilkan duplikasi record di HDFS.
- `consumer_timeout_ms=1000` memastikan consumer tidak blocking saat tidak ada pesan baru masuk.
- Jika koneksi HDFS gagal pada satu siklus, error dicatat ke stdout tetapi consumer tetap berjalan dan mencoba kembali pada siklus berikutnya.
- Direktori `dashboard/data/` dibuat otomatis oleh script jika belum ada.

---

---

## Bukti Sistem Berjalan

### Producer RSS — Artikel Terkirim ke Kafka

![Producer RSS berhasil mengirim 150 artikel ke Kafka topic saham-rss](image/bigdata1.png)

> Screenshot menunjukkan producer berhasil mengirim 150 artikel pada polling pertama pukul 01:44:10. Polling berikutnya pukul 01:49:11 menghasilkan 0 artikel baru, membuktikan mekanisme deduplikasi berjalan sempurna.

---

### Producer API — Data Saham Terkirim ke Kafka

![Producer API mengirim 10 saham dalam mode simulasi](image/bigdata2.png)

> Screenshot menunjukkan 10 saham blue-chip IDX berhasil dikirim ke Kafka topic `saham-api` dalam mode SIMULASI. Data mencakup harga, persentase perubahan, dan volume transaksi untuk setiap emiten.

---

### Consumer — Data Berhasil Masuk ke HDFS

![Verifikasi file JSON di HDFS dan live_api.json lokal berhasil dibuat](image/bigdata4.png)

## Bagian D: Analisis Saham dengan Apache Spark 

### Deskripsi
Bagian ini bertanggung jawab untuk memproses dan menganalisis data mentah (*raw data*) yang telah disimpan di HDFS menggunakan Apache Spark. Proses analitik dilakukan secara *batch processing* melalui *Jupyter Notebook* untuk menghasilkan wawasan (*insights*) terkait pergerakan harga saham dan sentimen berita pasar modal. Hasil analisis kemudian diekspor menjadi file JSON agar dapat divisualisasikan oleh Dashboard.

### Lokasi File
| File | Keterangan |
|------|------------|
| `spark/analysis.ipynb` | Notebook utama yang berisi logika transformasi dan analisis data |
| `dashboard/data/spark_results.json` | File output hasil analitik yang akan dibaca oleh frontend |

### Dependensi
```bash
pip install pyspark pandas findspark
jupyter notebook spark/analysis.ipynb
./spark/run_auto_analysis.sh
```

### 3 Metrik Analisis Utama
Apache Spark membaca rentetan data JSON dari direktori /data/saham/api/ dan /data/saham/rss/ di HDFS untuk melakukan tiga perhitungan utama secara simultan:

#### Analisis 1: Pergerakan & Return Saham

- Tujuan: Mengetahui emiten mana yang memberikan persentase perubahan harga tertinggi (Cuan) selama sesi berjalan.

- Metode: Spark mengagregasi data berdasarkan ticker, membandingkan harga pembukaan awal (harga_awal) dengan harga penutupan terakhir (harga_terkini). Hasil return_pct kemudian dikategorikan menjadi status NAIK, TURUN, atau FLAT.

#### Analisis 2: Tingkat Volatilitas & Risiko Harga

- Tujuan: Mengukur seberapa liar pergerakan harga saham untuk menilai profil risiko setiap emiten.

- Metode: Menggunakan perhitungan Standar Deviasi (stddev_harga) terhadap seluruh titik harga yang masuk. Angka tersebut lalu dibagi dengan rata-rata harga untuk mendapatkan Coefficient of Variation (cv_pct). Saham akan dilabeli volatilitas TINGGI (> 2.0%), SEDANG (> 0.5%), atau RENDAH.

#### Analisis 3: Frekuensi Penyebutan Berita (Media Exposure)

- Tujuan: Mengetahui emiten blue-chip mana yang sedang ramai diperbincangkan oleh media nasional.

- Metode: Spark memindai kolom judul dari seluruh arsip berita RSS menggunakan Regex. Sistem menghitung jumlah sebutan (mention counts) untuk 5 perusahaan target: BCA, BRI, Telkom, Astra, dan Mandiri berdasarkan keywords yang sudah ditentukan.

### Cara Menjalankan
Pastikan infrastruktur Hadoop (Namenode & Datanode) sudah berjalan, serta Producer dan Consumer telah aktif menyuntikkan data ke HDFS.

- Buka terminal (Bash/WSL) dan arahkan ke direktori root proyek saham-meter/.
- Jalankan skrip analisis otomatis (pastikan environment Python sudah aktif):

``` Bash
./spark/run_auto_analysis.sh
``` 
Skrip akan berjalan tanpa henti dan mencetak log Selesai analisis & simpan setiap 5 detik. Output yang Diharapkan

```` Bash
File dashboard/data/spark_results.json akan terus diperbarui secara real-time.
````

Spark juga akan mem-backup hasil analisis (return, volatilitas, frekuensi) ke HDFS di dalam direktori /data/saham/hasil/ yang diberi label timestamp unik.

## Bagian E: Dashboard

Bagian ini adalah ujung tombak interaksi pengguna (End-User Interface). Berupa aplikasi web interaktif yang bertugas memvisualisasikan seluruh aliran data Big Data—mulai dari harga saham live, berita real-time, hingga hasil komputasi analitik Apache Spark. Aplikasi ini dibangun menggunakan antarmuka modern yang secara otomatis memperbarui data (auto-refresh) setiap 5 detik tanpa perlu memuat ulang (reload) halaman.

### Lokasi File

| File | Keterangan |
|-----------|------------------|
| `dashboard/app.py` | `Backend menggunakan Flask bertugas mengekspos endpoint API dari pembacaan file JSON lokal.` |
| `dashboard/templates/index.html` | `Frontend utama yang memuat UI/UX, logika JavaScript, dan rendering grafik.` |
| `dashboard/data/*.json` | `Direktori cache data hasil sinkronisasi dari HDFS yang menjadi sumber bacaan Flask.` |

### Dependensi
``` Bash
pip install flask
```

### Fitur Utama Dashboard

1. Ticker Tape & Market Strip: Pita berjalan (scrolling text) di bagian atas yang menampilkan ringkasan harga saham live dan Top Mover (saham dengan pergerakan paling ekstrem).

2. Dynamic UI & Dark Mode: Antarmuka responsif yang dapat diakses melalui laptop maupun smartphone, dilengkapi tombol switch Dark/Light Mode yang tersimpan di Local Storage.

3. Live Market & History Charts: Tabel harga saham yang terhubung langsung dengan Kafka/HDFS, serta grafik garis (line chart) untuk melacak rekam jejak harga (historical prices) menggunakan Chart.js.

4. News Radar: Menampilkan agregasi 10 berita pasar modal terbaru secara real-time, lengkap dengan badge warna untuk hasil deteksi sentimen (Positif/Negatif/Netral).

5. Spark Insights: Merender tabel hasil analitik (Return & Volatilitas) serta grafik batang (bar chart) Frekuensi Sebutan Berita hasil dari batch processing Apache Spark.

### Cara Menjalankan
Pastikan komponen Consumer dan Spark sudah berjalan dan berhasil menciptakan file JSON di dalam folder dashboard/data/ sebelum menyalakan dashboard ini.

- Buka terminal baru dan jalankan script Flask:
``` bash 
python dashboard/app.py
```

- Buka browser (Chrome/Edge/Safari/Firefox) dan akses alamat:
``` bash
http://localhost:5000 atau http://127.0.0.1:5000
``` 
### Alur Kerja (Bagaimana Ini Bekerja?)

Aplikasi Flask tidak melakukan komputasi berat. Ia membaca file live_api.json, live_rss.json, dan spark_results.json dengan pengamanan try-except. JavaScript di sisi frontend akan melakukan fetching ke /api/data setiap 5.000 milidetik, mendistribusikan data tersebut ke tabel yang sesuai, dan me-redraw canvas Chart.js secara dinamis.