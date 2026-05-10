#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

source spark/spark-env/bin/activate

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  spark/analysis_stream.py
