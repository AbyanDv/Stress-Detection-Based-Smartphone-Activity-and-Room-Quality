from flask import Flask, request, jsonify
from datetime import datetime

app = Flask(__name__)

# Mapping package ke nama aplikasi
PACKAGE_NAME_MAP = {
    "com.ss.android.ugc.trill": "TikTok",
    "com.twitter.android": "Twitter",
    "com.whatsapp": "WhatsApp",
    "com.instagram.android": "Instagram",
    "com.miui.home": "System launcher",
    "com.android.chrome": "Chrome",
    "com.google.android.apps.docs.editors.sheets": "Google Sheets",
    "com.google.android.apps.tachyon": "Google Meet",
    "com.miui.securitycenter": "Security",
    "com.google.android.youtube": "YouTube",
    "com.android.vending": "Google Play Store",
    "com.miui.gallery": "Gallery",
    "com.facebook.katana": "Facebook",
    "org.telegram.messenger": "Telegram",
    "com.google.android.packageinstaller": "Package Installer",
    "com.google.android.apps.wellbeing": "Digital Wellbeing",
    "com.google.android.permissioncontroller": "Permission Controller",
    "com.miui.cleaner": "Cleaner",
    "com.xiaomi.account": "Xiaomi Account",
    "com.mi.android.globalFileexplorer": "File Manager",
    "com.android.systemui": "System UI",
    "com.miui.aod": "Always-on display"
}


def fuzzy_level(total_hours):
    """Menentukan level screen time berdasarkan total jam"""
    if total_hours <= 2:
        return "Low"
    elif total_hours <= 5:
        return "Moderate"
    else:
        return "High"


def format_hms(seconds):
    """Konversi detik ke format H jam M menit S detik"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours} jam {minutes} menit {secs} detik"


@app.route('/receive_usage', methods=['POST'])
def receive_usage():
    data = request.get_json(force=True, silent=False)
    if not data:
        return jsonify({"status": "error", "message": "no json received"}), 400

    total_sec_all = data.get("total_screen_time_s", 0)
    usage_list = data.get("usage_data", [])

    now = datetime.now().isoformat()
    print(f"\n[{now}] Received Total Screen Time: {total_sec_all} seconds")
    print(f"Received payload ({len(usage_list)} apps):")

    # Format tiap aplikasi dengan waktu H M S
    for item in usage_list:
        pkg = item.get("package")
        sec = item.get("foreground_time_s", 0)
        app_name = PACKAGE_NAME_MAP.get(pkg, pkg)
        formatted = format_hms(sec)
        print(f" - {app_name} ({pkg}) -> {formatted}")

    # Total waktu + level fuzzy
    formatted_total = format_hms(total_sec_all)
    total_hours = total_sec_all / 3600
    level = fuzzy_level(total_hours)

    print(f"\nTotal penggunaan: {formatted_total} → Level: {level}")

    return jsonify({
        "status": "ok",
        "total_apps": len(usage_list),
        "total_usage": formatted_total,
        "usage_level": level
    }), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
