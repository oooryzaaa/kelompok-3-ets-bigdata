from pathlib import Path
import json

from flask import Flask, jsonify, render_template

app = Flask(__name__)

# ============================================================
# PENGATURAN DIREKTORI DATA (REVISI MULTI-FOLDER)
# ============================================================
BASE_DIR = Path(__file__).resolve().parent  # Mengarah ke folder ./dashboard/
ROOT_DIR = BASE_DIR.parent                  # Mengarah ke folder utama / root

LEGACY_DATA_DIR = BASE_DIR / "data"         # ./dashboard/data/
GOLD_DATA_DIR = ROOT_DIR / "data"           # ./data/ (di halaman depan)

# File Lama bawaan ETS
SPARK_RESULTS_FILE      = LEGACY_DATA_DIR / "spark_results.json"
LIVE_API_FILE           = LEGACY_DATA_DIR / "live_api.json"
LIVE_RSS_FILE           = LEGACY_DATA_DIR / "live_rss.json"

# File Baru dari Delta Lake
GOLD_RETURN_FILE        = GOLD_DATA_DIR / "gold_return.json"
GOLD_VOLATILITAS_FILE   = GOLD_DATA_DIR / "gold_volatilitas.json"
GOLD_MOMENTUM_FILE      = GOLD_DATA_DIR / "gold_momentum.json"
GOLD_SENTIMENT_FILE     = GOLD_DATA_DIR / "gold_sentiment_market.json"


EMPTY_SPARK_RESULTS = {
    "metadata": {},
    "analisis_1_return": [],
    "analisis_2_volatilitas": [],
    "analisis_3_frekuensi_berita": [],
}

def read_json_file(path, default_value):
    """Read a JSON file safely and convert NaN/Infinity to null."""
    try:
        if not path.exists() or path.stat().st_size == 0:
            return default_value
            
        with path.open("r", encoding="utf-8") as file:
            content = file.read().strip()
            if not content:
                return default_value
            
            # FITUR BARU: Penawar racun NaN dari Spark
            # Mengubah NaN, Infinity, dan -Infinity menjadi None (null di web)
            def handle_nan(c):
                return None
                
            # Cek apakah formatnya JSON biasa (diawali [ atau { )
            if content.startswith('[') or content.startswith('{'):
                return json.loads(content, parse_constant=handle_nan)
            else:
                # Jika tidak, parse sebagai JSON Lines (format bawaan PySpark)
                return [json.loads(line, parse_constant=handle_nan) for line in content.split('\n') if line.strip()]
                
    except Exception as e:
        print(f"⚠️ Error saat membaca {path.name}: {e}")
        return default_value

def read_spark_results():
    data = read_json_file(SPARK_RESULTS_FILE, EMPTY_SPARK_RESULTS.copy())
    if not isinstance(data, dict):
        return EMPTY_SPARK_RESULTS.copy()
    return {
        "metadata":                    data.get("metadata") if isinstance(data.get("metadata"), dict) else {},
        "analisis_1_return":           data.get("analisis_1_return") if isinstance(data.get("analisis_1_return"), list) else [],
        "analisis_2_volatilitas":      data.get("analisis_2_volatilitas") if isinstance(data.get("analisis_2_volatilitas"), list) else [],
        "analisis_3_frekuensi_berita": data.get("analisis_3_frekuensi_berita") if isinstance(data.get("analisis_3_frekuensi_berita"), list) else [],
    }

def read_live_api():
    data = read_json_file(LIVE_API_FILE, [])
    return data if isinstance(data, list) else []

def read_live_rss():
    data = read_json_file(LIVE_RSS_FILE, [])
    return data if isinstance(data, list) else []

def read_gold_return():
    data = read_json_file(GOLD_RETURN_FILE, [])
    return data if isinstance(data, list) else []

def read_gold_volatilitas():
    data = read_json_file(GOLD_VOLATILITAS_FILE, [])
    return data if isinstance(data, list) else []

def read_gold_momentum():
    data = read_json_file(GOLD_MOMENTUM_FILE, [])
    return data if isinstance(data, list) else []

def read_gold_sentiment():
    data = read_json_file(GOLD_SENTIMENT_FILE, [])
    return data if isinstance(data, list) else []

@app.route("/")
def index():
    # Cukup render HTML saja, data akan ditarik otomatis oleh JavaScript Fetch di frontend
    return render_template("index.html")

@app.route("/api/data")
def api_data():
    return jsonify({
        "spark":            read_spark_results(),
        "live_api":         read_live_api(),
        "live_rss":         read_live_rss(),
        "gold_return":      read_gold_return(),
        "gold_volatilitas": read_gold_volatilitas(),
        "gold_momentum":    read_gold_momentum(),
        "gold_sentiment":   read_gold_sentiment(),
    })

@app.route("/api/spark")
def api_spark():
    return jsonify(read_spark_results())

# ============================================================
# ENDPOINT BARU — Gold Delta Lake
# ============================================================

@app.route("/api/gold/return")
def api_gold_return():
    return jsonify(read_gold_return())

@app.route("/api/gold/volatilitas")
def api_gold_volatilitas():
    return jsonify(read_gold_volatilitas())

@app.route("/api/gold/momentum")
def api_gold_momentum():
    return jsonify(read_gold_momentum())

@app.route("/api/gold/sentiment")
def api_gold_sentiment():
    return jsonify(read_gold_sentiment())

@app.route("/api/gold")
def api_gold():
    return jsonify({
        "return":      read_gold_return(),
        "volatilitas": read_gold_volatilitas(),
        "momentum":    read_gold_momentum(),
        "sentiment":   read_gold_sentiment(),
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)