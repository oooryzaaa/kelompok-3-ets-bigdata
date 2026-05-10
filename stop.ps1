# stop.sh
docker compose -f docker-compose-kafka.yml down
docker compose  -f docker-compose-hadoop.yml down

# Jalankan
# .\stop.ps1