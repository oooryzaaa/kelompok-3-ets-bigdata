# Bronze Layer: Data Ingestion ( Nadia Kirana Afifah Prahandita ) 


Dokumentasi teknis untuk komponen **Bronze Layer** pada pipeline *SahamMeter*. Komponen ini bertanggung jawab melakukan ekstraksi data mentah (*raw*) dari sumber eksternal dan melakukan persistensi data dalam format yang efisien untuk analisis lanjut.

## Daftar Isi
* [Deskripsi](#deskripsi)
* [Alur Kerja](#alur-kerja)
* [Implementasi Teknis](#implementasi-teknis)
* [Cara Menjalankan & Validasi](#cara-menjalankan--validasi)
* [Bukti Sistem Berjalan](#bukti-sistem-berjalan)

## Deskripsi
Bronze Layer bertugas melakukan *ingestion* data dari API Saham dan RSS Feed Berita secara periodik. Data yang diambil langsung dikonversi ke dalam format **Apache Parquet**. Pendekatan ini memastikan data tersimpan dalam bentuk *schema-enforced* dan terkompresi dengan baik sebelum masuk ke tahap transformasi di *Silver Layer*.

## Alur Kerja
1. Melakukan *request* data dari API dan *fetch* URL RSS feed.
2. Memproses data mentah menggunakan Apache Spark.
3. Melakukan konversi format dari JSON menjadi **Apache Parquet**.
4. Menyimpan *output* ke folder `lakehouse_data/`.
5. Memberikan log status keberhasilan proses di terminal.

## Implementasi Teknis
* **Teknologi:** Python, Apache Spark.
* **Format Penyimpanan:** Apache Parquet.
* **Keunggulan:** Mendukung *columnar storage* yang mempercepat *query* dan kompresi tinggi untuk penyimpanan data dalam jumlah besar.

## Cara Menjalankan & Validasi
Untuk menjalankan proses ingestion dari sumber data ke sistem:
```
# Menjalankan script Bronze Layer
python lakehouse/01_bronze.py

```
**Penjelasan Command:**

`python lakehouse/01_bronze.py`: Menjalankan worker Spark yang bertugas melakukan extract dari API, merapikan skema data, dan melakukan write ke format `.parquet` di dalam direktori `lakehouse_data/`.
Cara Validasi Hasil:
Setelah menjalankan perintah, pastikan file `.parquet` sudah terbentuk di dalam folder `lakehouse_data/`.

## Bukti Sistem Berjalan
Output eksekusi pada terminal menunjukkan proses berjalan sukses:

<img width="1045" height="298" alt="image" src="https://github.com/user-attachments/assets/11dd41fc-c24d-437d-adde-b0c60f5d8fee" />


# 2. Silver Layer & Time Travel 

## Tugas yang Dikerjakan

- `02_silver.py` — Cleaning data dari Bronze ke Silver (Delta Lake)
- `time_travel_demo.py` — Demonstrasi Time Travel Delta Lake

---

## Arsitektur Silver Layer

```
Bronze (Parquet)
    ├── lakehouse_data/bronze/saham_api   ← data mentah API
    └── lakehouse_data/bronze/saham_rss   ← data mentah RSS
                        ↓
                  02_silver.py
                        ↓
Silver (Delta Lake)
    ├── lakehouse_data/silver/saham_api   ← data bersih + Z-Score
    └── lakehouse_data/silver/saham_rss   ← data bersih + cast timestamp
```

---

## Transformasi Cleaning — 02_silver.py

### API Data

| No | Transformasi | Sebelum | Sesudah | Alasan |
|----|-------------|---------|---------|--------|
| T1 | Hapus duplikat (`ticker` + `timestamp`) | 50 baris | 50 baris | Record ganda dari producer akan merusak kalkulasi return dan volume |
| T2 | Hapus baris NULL (`ticker`, `harga`) | 50 baris | 50 baris | Harga NULL tidak bisa dipakai untuk menghitung return_pct maupun analisis volatilitas |
| T3 | Filter nilai tidak logis (`harga <= 0`, `high < low`) | 50 baris | 50 baris | Harga negatif atau high < low adalah data korup dari sumber API |
| T4 | Cast timestamp + feature engineering | 50 baris | 50 baris | Timestamp harus bertipe TimestampType agar Window Function bisa dipakai di Gold layer |

### Kolom Baru di Silver (tidak ada di Bronze)

| Kolom | Keterangan |
|-------|-----------|
| `return_pct` | Persentase return: `(harga - open) / open * 100` |
| `price_range` | Selisih high - low sebagai indikator volatilitas harian |
| `jam` | Jam dari timestamp, untuk analisis temporal di Gold |
| `hari_minggu` | Hari dalam seminggu (1=Minggu, 7=Sabtu) |
| `mean_harga` | Rata-rata harga per ticker (window function) |
| `std_harga` | Standar deviasi harga per ticker |
| `z_score` | Z-Score harga relatif terhadap rata-rata ticker |
| `is_outlier` | Flag `true` jika Z-Score > 2 (harga anomali) |
| `_cleaned_at` | Timestamp saat proses cleaning dijalankan |

Bronze memiliki 11 kolom. Setelah cleaning, Silver memiliki 20 kolom.

> Outlier tidak dihapus di Silver. Data tetap dipertahankan agar Gold layer bisa menganalisis kejadian anomali harga secara terpisah.

### RSS Data

| No | Transformasi | Sebelum | Sesudah | Alasan |
|----|-------------|---------|---------|--------|
| T1 | Hapus duplikat (`id`) | 50 baris | 50 baris | Artikel yang sama kadang dipublikasikan ulang dengan ID identik |
| T2 | Filter NULL + cast `waktu_terbit` | 50 baris | 50 baris | Artikel tanpa judul tidak bisa dipakai untuk analisis korelasi berita-harga di Gold |

---

## Mengapa Delta Lake Lebih Baik dari Parquet/JSON Biasa?

| Fitur | JSON/Parquet (ETS lama) | Delta Lake (Tugas ini) |
|-------|------------------------|----------------------|
| ACID Transaction | Tidak ada | Ada |
| Schema enforcement | Tidak ada | Ada |
| History perubahan | Tidak ada | Ada (Time Travel) |
| Rollback data | Tidak bisa | Bisa ke versi manapun |
| Update / Delete | Tidak bisa | Bisa |
| Audit trail | Tidak ada | Ada (operation log) |

---

## Time Travel  — time_travel_demo.py

### Alur Operasi

```
Version 0 (WRITE)   → Data Silver awal hasil cleaning, 50 baris
        ↓
Version 1 (UPDATE)  → Z-score outlier diubah menjadi -99.0
        ↓
Version 2 (DELETE)  → Semua baris ticker GOTO dihapus (5 baris)
        ↓
Rollback            → Data dikembalikan ke Version 0 (50 baris)
```

### Hasil Perbandingan Lintas Versi

| Versi | Operasi | Jumlah Baris | Avg Return % |
|-------|---------|-------------|-------------|
| 0 | Initial (post-cleaning) | 50 | 0.0636 |
| 1 | Post-UPDATE | 50 | 0.0636 |
| 2 | Post-DELETE (GOTO dihapus) | 45 | 0.0179 |
| Rollback | Kembali ke Version 0 | 50 | 0.0636 |

Menghapus 5 baris ticker GOTO mengubah rata-rata return dari 0.0636 menjadi 0.0179. Ini membuktikan bahwa Time Travel tidak hanya menyimpan data, tetapi juga memungkinkan audit dampak perubahan data terhadap analisis — sesuatu yang tidak mungkin dilakukan dengan JSON atau Parquet biasa.

---

## Cara Menjalankan

```bash
# Aktifkan virtual environment
source venv/bin/activate

# Jalankan Silver layer
python lakehouse/02_silver.py

# Jalankan Time Travel demo
python lakehouse/time_travel_demo.py

# Cek perbandingan Bronze vs Silver dan semua versi
python lakehouse/cek_data.py
```

---

## Screenshot Output

### 02_silver.py
![silver 1](../image/silvers1.jpeg)
![silver 2](../image/silvers2.jpeg)

### time_travel_demo.py
![timetravel 1](../image/timetravel.jpeg)
![timetravel 2](../image/timetravel2.jpeg)

## Hasil
![result](../image/result.jpeg)
![result2](../image/result2.jpeg)

# 3. Gold Layer & Flask Dashboard Update (Oryza Qiara Ramadhani)

## Tugas yang Dikerjakan

- `03_gold.py` — Membangun 2 tabel Gold dari Silver layer (Delta Lake)
- `app.py` — Update Flask Dashboard agar membaca dari tabel Delta 

---

## Arsitektur Gold Layer

```
Silver (Delta Lake)
    ├── lakehouse_data/silver/saham_api   ← data bersih + Z-Score
    └── lakehouse_data/silver/saham_rss   ← data bersih + cast timestamp
                        ↓
                  03_gold.py
                        ↓
Gold (Delta Lake)
    ├── lakehouse_data/gold/summary_per_ticker   ← ringkasan per ticker
    └── lakehouse_data/gold/top_mover_harian     ← top gainer & loser per hari
                        ↓
              Export ke JSON
                        ↓
    ├── data/gold_return.json       ← dibaca Flask
    └── data/gold_volatilitas.json  ← dibaca Flask
```

---

## Tabel Gold yang Dihasilkan — 03_gold.py

### Gold Table 1: Summary Per Ticker

Mereproduksi analisis ETS lama: ringkasan performa setiap emiten.

| Kolom | Keterangan |
|-------|-----------|
| `ticker` | Kode emiten saham |
| `avg_harga` | Rata-rata harga penutupan |
| `min_harga` | Harga terendah |
| `max_harga` | Harga tertinggi |
| `total_volume` | Total volume transaksi |
| `avg_volume` | Rata-rata volume per sesi |
| `avg_return_pct` | Rata-rata persentase return |
| `avg_price_range` | Rata-rata selisih high-low (indikator volatilitas) |
| `jumlah_data_points` | Jumlah record yang dianalisis |
| `jumlah_outlier` | Jumlah data harga anomali (Z-Score > 2) |

### Gold Table 2: Top Mover Per Hari

Mereproduksi analisis ETS lama: emiten dengan pergerakan paling ekstrem tiap hari.

| Kolom | Keterangan |
|-------|-----------|
| `tanggal` | Tanggal sesi perdagangan |
| `top_gainer_ticker` | Emiten dengan return tertinggi |
| `top_gainer_return_pct` | Persentase return tertinggi |
| `top_gainer_harga` | Harga penutupan top gainer |
| `top_loser_ticker` | Emiten dengan return terendah |
| `top_loser_return_pct` | Persentase return terendah |
| `top_loser_harga` | Harga penutupan top loser |

### Hasil Output

```
gold/summary_per_ticker  : 10 rows, 13 columns
gold/top_mover_harian    :  1 rows,  7 columns
```

---

## Update Flask Dashboard — app.py

### Perbandingan: Sebelum vs Sesudah

| Aspek | Sebelum (ETS) | Sesudah (Lakehouse) |
|-------|--------------|---------------------|
| Sumber data Gold | File JSON dari HDFS | Tabel Delta Lake |
| Cara update data | Spark analysis.ipynb manual | Jalankan 03_gold.py |
| Format perantara | JSON dari HDFS | JSON di-export dari Delta |
| Route `/api/gold/return` | Baca `gold_return.json` dari HDFS | Baca `gold_return.json` dari Delta export |
| Route `/api/gold/volatilitas` | Baca `gold_volatilitas.json` dari HDFS | Baca `gold_volatilitas.json` dari Delta export |

> Flask tidak perlu diubah strukturnya — cukup pastikan `03_gold.py` dijalankan lebih dulu agar JSON yang dibaca Flask sudah berasal dari Delta, bukan dari HDFS lagi.

### Alur Kerja Baru

```
python lakehouse/03_gold.py   ← generate Delta + export JSON
python app.py                 ← Flask baca JSON hasil Delta
```

---

## Cara Menjalankan

```bash
# Aktifkan virtual environment
source venv/bin/activate

# Jalankan Gold layer + export JSON
python lakehouse/03_gold.py

# Jalankan Flask Dashboard
python app.py
# Buka http://localhost:5000
```

---

## Screenshot Output

### 03_gold.py
![alt text](<../image/gold table.png>)
![alt text](<../image/gold table2.png>)

### Flask Dashboard (data dari Delta)
![alt text](<../image/dashboard delta.png>)

# 4. Advanced Gold Analytics & Cross-Source Intelligence (Gemilang Lingua)

Dokumentasi teknis untuk pengembangan **Gold Layer lanjutan** pada pipeline *SahamMeter*. Fokus utama pada tahap ini adalah membangun analisis tingkat lanjut menggunakan fitur advanced PySpark seperti **Window Function**, **lag()**, dan **cross-source join** antara data saham dan berita.

## Tugas yang Dikerjakan

- Enhancement `03_gold.py`
- Penambahan **Gold Table 3** berbasis Window Function & trend analysis
- Penambahan **Gold Table 4** berbasis join antara data saham dan RSS berita
- Export hasil analisis ke JSON untuk Flask Dashboard

---

# Arsitektur Advanced Gold Layer

```text
Silver (Delta Lake)
    ├── lakehouse_data/silver/saham_api
    └── lakehouse_data/silver/saham_rss
                        ↓
                Advanced Gold Processing
                        ↓
Gold (Enhanced Analytics)
    ├── lakehouse_data/gold/momentum_anomali
    └── lakehouse_data/gold/sentiment_market
                        ↓
                Export JSON Flask
                        ↓
    ├── data/gold_momentum.json
    └── data/gold_sentiment_market.json
```

---

# Gold Table 3 — Momentum & Anomaly Detection

## Deskripsi

Gold Table 3 digunakan untuk menganalisis:

- Momentum perubahan harga saham
- Tren return antar waktu
- Perubahan harga sebelumnya vs sekarang
- Deteksi kondisi market ekstrem menggunakan `lag()`

Tabel ini memanfaatkan **Window Function** untuk melakukan analisis historis antar baris data dalam ticker yang sama.

---

## Teknologi PySpark yang Digunakan

| Fitur | Fungsi |
|-------|--------|
| `Window.partitionBy()` | Mengelompokkan analisis per ticker |
| `orderBy(timestamp)` | Mengurutkan data berdasarkan waktu |
| `lag()` | Mengambil harga sebelumnya |
| `round()` | Membulatkan hasil analisis |
| Delta Lake | Penyimpanan tabel Gold |

---

## Alur Analisis

```text
Harga Sekarang
        ↓
Bandingkan dengan harga sebelumnya (lag)
        ↓
Hitung perubahan momentum
        ↓
Deteksi anomali return
        ↓
Simpan ke Gold Layer
```

---

## Kolom yang Dihasilkan

| Kolom | Keterangan |
|-------|-----------|
| `ticker` | Kode saham |
| `timestamp` | Waktu transaksi |
| `harga` | Harga saat ini |
| `prev_harga` | Harga sebelumnya menggunakan `lag()` |
| `momentum_harga` | Selisih harga sekarang vs sebelumnya |
| `return_pct` | Persentase return |
| `z_score` | Nilai anomali statistik |
| `is_outlier` | Flag data anomali |
| `rank_momentum` | Ranking momentum terbesar |

---

## Insight yang Didapat

- Mengetahui saham dengan lonjakan harga tercepat
- Mendeteksi pola perubahan market
- Menemukan anomali pergerakan harga
- Menjadi dasar analisis volatilitas real-time

---

# Gold Table 4 — Sentiment vs Market Movement

## Deskripsi

Gold Table 4 menggabungkan data:

- Harga saham dari Silver API
- Sentimen berita dari Silver RSS

Tujuan utamanya adalah membangun **cross-source intelligence** untuk melihat hubungan antara sentimen berita dan pergerakan market saham.

---

## Konsep Join

```text
Silver API (harga saham)
            JOIN
Silver RSS (berita finansial)
            ↓
Analisis sentimen vs market movement
```

Join dilakukan menggunakan atribut waktu (`jam`) agar berita yang muncul pada jam tertentu dapat dibandingkan dengan kondisi market pada waktu yang sama.

---

## Kolom yang Dihasilkan

| Kolom | Keterangan |
|-------|-----------|
| `ticker` | Emiten saham |
| `harga` | Harga saham |
| `return_pct` | Persentase return |
| `sentimen` | Sentimen berita |
| `judul` | Judul berita |
| `sumber` | Sumber berita |
| `jam` | Jam publikasi/transaksi |
| `market_match` | Kesesuaian sentimen dengan kondisi market |

---

## Interpretasi Market Match

| Kondisi | Hasil |
|---------|-------|
| Sentimen positif + return naik | MATCH |
| Sentimen negatif + return turun | MATCH |
| Sentimen positif + return turun | NOT MATCH |
| Sentimen negatif + return naik | NOT MATCH |

---

## Keunggulan Cross-Source Join

| Analisis Tradisional | Analisis Lakehouse |
|---------------------|-------------------|
| Data harga terpisah | Harga + berita terintegrasi |
| Tidak ada korelasi berita | Ada sentiment intelligence |
| Analisis statis | Insight real-time |
| Satu sumber data | Multi-source analytics |

---

# Export JSON untuk Flask Dashboard

Setelah Gold Table selesai dibuat, data diexport menjadi JSON agar dapat dibaca Flask Dashboard.

## File JSON yang Dihasilkan

| File | Fungsi |
|------|--------|
| `gold_momentum.json` | Data momentum & anomaly |
| `gold_sentiment_market.json` | Data sentimen vs market |

---

# Cara Menjalankan

```bash
# Aktifkan virtual environment
source venv/bin/activate

# Jalankan Gold analytics lanjutan
python lakehouse/03_gold.py

# Jalankan Flask Dashboard
python app.py
```

---

# Hasil Output

```text
gold/momentum_anomali      : data momentum saham berhasil dibuat
gold/sentiment_market      : data cross-source berhasil dibuat
```

---

# Kesimpulan

Pengembangan Gold Layer lanjutan berhasil menambahkan kemampuan:

- Advanced analytics menggunakan Window Function
- Analisis historis menggunakan `lag()`
- Deteksi anomali market
- Integrasi multi-source data
- Insight hubungan sentimen berita dengan market saham

Pendekatan ini membuat arsitektur *SahamMeter* berkembang dari sekadar data pipeline menjadi platform analitik berbasis *Lakehouse Intelligence*.