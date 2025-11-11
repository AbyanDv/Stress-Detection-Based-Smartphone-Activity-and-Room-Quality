package com.example.screentimemonitoring

import android.app.usage.UsageStats
import android.app.usage.UsageStatsManager
import android.content.Context
import android.content.Intent
import android.os.Bundle
import android.provider.Settings
import android.widget.Button
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import androidx.work.* // <-- WorkManager diperlukan di sini
import org.json.JSONArray
import org.json.JSONObject
import java.util.concurrent.TimeUnit // Diperlukan untuk interval WorkManager

class MainActivity : AppCompatActivity() {

    private lateinit var tvResult: TextView
    private lateinit var btnRequestPermission: Button
    private lateinit var btnGetUsage: Button

    // VARIABEL HANDLER/RUNNABLE DIHAPUS

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        tvResult = findViewById(R.id.tvResult)
        btnRequestPermission = findViewById(R.id.btnRequestPermission)
        btnGetUsage = findViewById(R.id.btnUsage)

        btnRequestPermission.setOnClickListener {
            requestUsageAccess()
        }

        btnGetUsage.setOnClickListener {
            if (hasUsageAccess()) {
                // 1. Tampilkan data sekali (untuk user melihat hasil)
                showUsageStats()

                // 2. Jadwalkan monitoring berkala WorkManager
                schedulePeriodicMonitoring()
                tvResult.append("\n\n✅ Monitoring Berkala (15 Menit) WorkManager Dijadwalkan!")
            } else {
                tvResult.text = "Akses belum diizinkan! Klik tombol atas dulu."
            }
        }

        // Cek dan jadwalkan WorkManager saat aplikasi dimulai (opsional, tapi disarankan)
        if (hasUsageAccess()) {
            schedulePeriodicMonitoring()
        }
    }

    private fun hasUsageAccess(): Boolean {
        // ... (Fungsi ini tetap sama)
        val appOps = getSystemService(Context.APP_OPS_SERVICE) as android.app.AppOpsManager
        val mode = appOps.checkOpNoThrow(
            android.app.AppOpsManager.OPSTR_GET_USAGE_STATS,
            android.os.Process.myUid(),
            packageName
        )
        return mode == android.app.AppOpsManager.MODE_ALLOWED
    }

    private fun requestUsageAccess() {
        startActivity(Intent(Settings.ACTION_USAGE_ACCESS_SETTINGS))

        val batteryIntent = Intent(Settings.ACTION_IGNORE_BATTERY_OPTIMIZATION_SETTINGS).apply {
            data = android.net.Uri.parse("package:$packageName")
        }
        startActivity(batteryIntent)
    }

    private fun showPopupMessage(message: String) {
        runOnUiThread {
            val builder = androidx.appcompat.app.AlertDialog.Builder(this)
            builder.setTitle("Peringatan Penggunaan Layar")
            builder.setMessage(message)
            builder.setPositiveButton("OK", null)
            val dialog = builder.create()
            dialog.show()
        }
    }
    private fun showUsageStats() {
        // --- 1. PENGAMBILAN DATA ---
        val usageStatsManager = getSystemService(Context.USAGE_STATS_SERVICE) as UsageStatsManager
        val endTime = System.currentTimeMillis()
        val startTime = endTime - 1000 * 60 * 60 * 24 // 24 jam terakhir

        val usageStatsList: List<UsageStats> = usageStatsManager.queryUsageStats(
            UsageStatsManager.INTERVAL_BEST,
            startTime,
            endTime
        )

        if (usageStatsList.isNullOrEmpty()) {
            tvResult.text = "Data kosong! Pastikan izin akses penggunaan sudah diaktifkan."
            return
        }

        // --- 2. PEMROSESAN DATA (Membuat Map) ---
        val usageMap = mutableMapOf<String, Long>()
        var totalScreenTimeSeconds: Long = 0
        for (usage in usageStatsList) {
            val totalSec = usage.totalTimeInForeground / 1000
            if (totalSec > 0) {
                usageMap[usage.packageName] = (usageMap[usage.packageName] ?: 0) + totalSec
                totalScreenTimeSeconds += totalSec
            }
        }

        // --- 3. MEMBUAT JSON ARRAY UNTUK DIKIRIM ---
        val jsonArray = JSONArray()
        val pm = packageManager
        val top10 = usageMap.entries.sortedByDescending { it.value }.take(10)

        for ((pkg, totalSec) in top10) {
            val appName = try {
                pm.getApplicationLabel(pm.getApplicationInfo(pkg, 0)).toString()
            } catch (e: Exception) {
                pkg
            }

            // Tambahkan ke JSON Array
            val jsonObj = JSONObject()
            jsonObj.put("package", pkg)
            jsonObj.put("app_name", appName)
            jsonObj.put("foreground_time_s", totalSec.toInt())
            jsonArray.put(jsonObj)

            // Tambahkan ke StringBuilder untuk tampilan UI (Lanjutan dari logika lama Anda)
            val jam = totalSec / 3600
            val menit = (totalSec % 3600) / 60
            val detik = totalSec % 60
            // ... (lanjutkan membuat stringbuilder untuk UI jika diperlukan)
        }

        // --- 4. KIRIM DATA INSTAN (PANGGILAN YANG HILANG) ---
        sendDataInstantly(jsonArray, totalScreenTimeSeconds) // ✅ Panggilan di sini!

        // --- 5. TAMPILKAN HASIL DI UI (Logika UI lama Anda) ---
        val sb = StringBuilder()

        // Saya asumsikan Anda ingin melihat hasil di UI:
        sb.append("=== Top 10 Penggunaan 24 Jam Terakhir ===\n\n")
        for ((pkg, totalSec) in top10) {
            val appName = try {
                pm.getApplicationLabel(pm.getApplicationInfo(pkg, 0)).toString()
            } catch (e: Exception) {
                pkg
            }
            val jam = totalSec / 3600
            val menit = (totalSec % 3600) / 60
            val detik = totalSec % 60
            sb.append("$appName - ${jam}j ${menit}m ${detik}d\n")
        }

        tvResult.text = sb.toString()
    }
    private fun sendDataInstantly(jsonArray: JSONArray, totalScreenTimeSeconds: Long) {
        Thread {
            try {
                // 1. BUAT OBJEK JSON UTAMA
                val finalPayload = JSONObject()
                finalPayload.put("total_screen_time_s", totalScreenTimeSeconds)
                finalPayload.put("usage_data", jsonArray)

                val client = OkHttpClient()
                val mediaType = "application/json; charset=utf-8".toMediaType()
                val body = finalPayload.toString().toRequestBody(mediaType)

                val request = Request.Builder()
                    .url("http://192.168.1.101:5000/receive_usage")
                    .post(body)
                    .build()

                val response = client.newCall(request).execute()
                val respBody = response.body?.string()

                runOnUiThread {
                    tvResult.append("\n\n(INSTANT) Server Response: ${response.code}\n$respBody")

                    try {
                        val jsonResp = JSONObject(respBody ?: "")
                        val message = jsonResp.optString("message", "")
                        if (message.isNotEmpty()) {
                            showPopupMessage(message) // ✅ tampilkan popup setelah kirim data
                        }
                    } catch (e: Exception) {
                        e.printStackTrace()
                        tvResult.append("\n\n(Gagal memproses respons server): ${e.message}")
                    }
                }
            } catch (e: Exception) {
                e.printStackTrace()
                runOnUiThread {
                    tvResult.append("\n\n(INSTANT) Gagal kirim data: ${e.message}")
                }
            }
        }.start()
    }

    private fun schedulePeriodicMonitoring() {
        val workManager = WorkManager.getInstance(applicationContext)
        val tag = "UsageMonitorTag"

        val constraints = Constraints.Builder()
            .setRequiredNetworkType(NetworkType.CONNECTED)
            .build()

        // GANTI KE PERIODIC WORK REQUEST
        val periodicWorkRequest = PeriodicWorkRequestBuilder<UsageDataWorker>(
            15, TimeUnit.MINUTES, // Interval 15 menit
            5, TimeUnit.MINUTES // Fleksibel di 5 menit terakhir
        )
            .setConstraints(constraints)
            .addTag(tag)
            .build()

        // Antrekan pekerjaan WorkManager
        workManager.enqueueUniquePeriodicWork(
            tag,
            ExistingPeriodicWorkPolicy.REPLACE, // Ganti pekerjaan lama jika ada
            periodicWorkRequest
        )

        // Log di UI untuk konfirmasi
        tvResult.append("\n\n✅ MONITORING BERKALA (15 Menit) DIJADWALKAN ULANG.")
    }
}