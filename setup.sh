#!/bin/bash
# setup.sh — jalankan sekali untuk setup seluruh infrastruktur SahamMeter

echo "=============================="
echo "  SahamMeter - Setup Infra    "
echo "=============================="

# 1. Buat folder yang diperlukan
echo "[1/6] Membuat struktur folder..."
mkdir -p kafka spark dashboard/data dashboard/templates

# 2. Jalankan Hadoop
echo "[2/6] Menjalankan Hadoop..."
docker compose -f docker-compose-hadoop.yml up -d

echo "      Menunggu Namenode siap (30 detik)..."
sleep 30

# 3. Verifikasi Hadoop
echo "[3/6] Verifikasi Hadoop..."
docker exec namenode hdfs dfsadmin -report | head -5

# 4. Buat direktori HDFS
echo "[4/6] Membuat direktori HDFS..."
docker exec namenode hdfs dfs -mkdir -p /data/saham/api
docker exec namenode hdfs dfs -mkdir -p /data/saham/rss
docker exec namenode hdfs dfs -mkdir -p /data/saham/hasil
docker exec namenode hdfs dfs -chmod -R 777 /data/saham
docker exec namenode hdfs dfs -ls /data/saham/

# 5. Jalankan Kafka
echo "[5/6] Menjalankan Kafka..."
docker compose -f docker-compose-kafka.yml up -d

echo "      Menunggu Kafka siap (20 detik)..."
sleep 20

# 6. Buat Kafka Topics
echo "[6/6] Membuat Kafka topics..."
docker exec kafka-broker kafka-topics.sh \
  --create --topic saham-api \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

docker exec kafka-broker kafka-topics.sh \
  --create --topic saham-rss \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

# Verifikasi topics
echo ""
echo "Topics yang terbuat:"
docker exec kafka-broker kafka-topics.sh \
  --list --bootstrap-server localhost:9092

echo ""
echo "=============================="
echo "  Setup selesai!              "
echo "  HDFS Web UI : http://localhost:9870"
echo "  Kafka broker: localhost:9092"
echo "=============================="