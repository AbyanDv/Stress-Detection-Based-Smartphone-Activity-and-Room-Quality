from flask import Flask, request, jsonify
from datetime import datetime
import csv
import os
import fuzzy_logic
from package_map import PACKAGE_NAME_MAP, APP_CATEGORY_MAP

app = Flask(__name__)

def format_hms(seconds):
    """Konversi detik ke format H jam M menit S detik"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours} jam {minutes} menit {secs} detik"

# =======================
# FILE PENYIMPANAN
# =======================
CSV_USAGE = "usage_data.csv"
CSV_SENSOR = "sensor_data.csv"

# Buat file CSV jika belum ada
if not os.path.exists(CSV_USAGE):
    with open(CSV_USAGE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp", "app_name", "package", "category",
            "usage_time", "total_screen_time", "fuzzy_level"
        ])

if not os.path.exists(CSV_SENSOR):
    with open(CSV_SENSOR, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "temperature", "humidity", "air_quality"])

# =======================
# ENDPOINT: ANDROID
# =======================
@app.route('/receive_usage', methods=['POST'])
def receive_usage():
    data = request.get_json(force=True, silent=False)
    if not data:
        return jsonify({"status": "error", "message": "no json received"}), 400

    total_sec_all = data.get("total_screen_time_s", 0)
    usage_list = data.get("usage_data", [])

    now = datetime.now().isoformat()
    formatted_total = format_hms(total_sec_all)
    total_hours = total_sec_all / 3600
    level, message = fuzzy_logic.fuzzy_level(total_hours)


    print(f"\n[{now}] === Android Usage Data ===")
    print(f"Total Screen Time: {formatted_total} → Level: {level}")
    print(f"Jumlah Aplikasi: {len(usage_list)}")

    with open(CSV_USAGE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for item in usage_list:
            pkg = item.get("package")
            sec = item.get("foreground_time_s", 0)
            app_name = PACKAGE_NAME_MAP.get(pkg, pkg)
            formatted = format_hms(sec)
            print(f" - {app_name} ({pkg}) [{category}] → {formatted}")
            category = APP_CATEGORY_MAP.get(pkg, "Lainnya")
            writer.writerow([now, app_name, pkg, category, formatted, formatted_total, level])


    return jsonify({
    "status": "ok",
    "source": "android",
    "message": message,
    "total_apps": len(usage_list),
    "total_usage": formatted_total,
    "usage_level": level
    }), 200


# =======================
# ENDPOINT: IoT (ESP32)
# =======================
@app.route('/receive_sensor', methods=['POST'])
def receive_sensor():
    data = request.get_json(force=True, silent=False)
    if not data:
        return jsonify({"status": "error", "message": "no json received"}), 400

    suhu = data.get("temperature")
    kelembapan = data.get("humidity")
    kualitas_udara = data.get("air_quality")

    now = datetime.now().isoformat()
    print(f"\n[{now}] === IoT Sensor Data ===")
    print(f"Suhu: {suhu} °C | Kelembapan: {kelembapan} % | Kualitas Udara: {kualitas_udara} %")

    with open(CSV_SENSOR, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([now, suhu, kelembapan, kualitas_udara])

    return jsonify({
        "status": "ok",
        "source": "iot",
        "message": "Sensor data saved",
        "data": {
            "temperature": suhu,
            "humidity": kelembapan,
            "air_quality": kualitas_udara
        }
    }), 200

# =======================
# JALANKAN SERVER
# =======================
if __name__ == "__main__":
    print("Server Flask aktif di http://0.0.0.0:5000 ...")
    app.run(host="0.0.0.0", port=5000, debug=True)
