from pathlib import Path
import json

from flask import Flask, jsonify, render_template


app = Flask(__name__)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
SPARK_RESULTS_FILE = DATA_DIR / "spark_results.json"
LIVE_API_FILE = DATA_DIR / "live_api.json"
LIVE_RSS_FILE = DATA_DIR / "live_rss.json"


EMPTY_SPARK_RESULTS = {
    "metadata": {},
    "analisis_1_return": [],
    "analisis_2_volatilitas": [],
    "analisis_3_frekuensi_berita": [],
}


def read_json_file(path, default_value):
    """Read a JSON file safely and return a default value when unavailable."""
    try:
        if not path.exists() or path.stat().st_size == 0:
            return default_value

        with path.open("r", encoding="utf-8") as file:
            return json.load(file)
    except (json.JSONDecodeError, OSError):
        return default_value


def read_spark_results():
    data = read_json_file(SPARK_RESULTS_FILE, EMPTY_SPARK_RESULTS.copy())

    if not isinstance(data, dict):
        return EMPTY_SPARK_RESULTS.copy()

    return {
        "metadata": data.get("metadata") if isinstance(data.get("metadata"), dict) else {},
        "analisis_1_return": data.get("analisis_1_return") if isinstance(data.get("analisis_1_return"), list) else [],
        "analisis_2_volatilitas": data.get("analisis_2_volatilitas") if isinstance(data.get("analisis_2_volatilitas"), list) else [],
        "analisis_3_frekuensi_berita": data.get("analisis_3_frekuensi_berita") if isinstance(data.get("analisis_3_frekuensi_berita"), list) else [],
    }


def read_live_api():
    data = read_json_file(LIVE_API_FILE, [])
    return data if isinstance(data, list) else []


def read_live_rss():
    data = read_json_file(LIVE_RSS_FILE, [])
    return data if isinstance(data, list) else []


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/data")
def api_data():
    return jsonify(
        {
            "spark": read_spark_results(),
            "live_api": read_live_api(),
            "live_rss": read_live_rss(),
        }
    )


@app.route("/api/spark")
def api_spark():
    return jsonify(read_spark_results())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
