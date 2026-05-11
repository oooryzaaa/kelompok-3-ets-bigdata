#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

source spark/spark-env/bin/activate

python spark/auto_analysis.py
