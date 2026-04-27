# restart.sh
docker compose -f docker-compose-hadoop.yml up -d
Start-Sleep -Seconds 30
docker compose -f docker-compose-kafka.yml up -d
Start-Sleep -Seconds 20
docker exec kafka-broker kafka-topics --list --bootstrap-server localhost:9092

# jalankan 
./restart.sh