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
// Hapus import OkHttp yang tidak perlu di sini
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
    }

    // Fungsi ini kini hanya untuk menampilkan data di UI, bukan untuk tugas berkala.
    private fun showUsageStats() {
        // Isi fungsi ini sama seperti sebelumnya, tetapi HAPUS panggilan sendDataToServer(..)

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

        // Gabungkan duplicate package (Logika tetap sama)
        val usageMap = mutableMapOf<String, Long>()
        var totalScreenTimeSeconds: Long = 0
        for (usage in usageStatsList) {
            val totalSec = usage.totalTimeInForeground / 1000
            if (totalSec > 0) {
                usageMap[usage.packageName] = (usageMap[usage.packageName] ?: 0) + totalSec
                totalScreenTimeSeconds += totalSec
            }
        }

        // Siapkan StringBuilder (Logika tetap sama)
        val sb = StringBuilder()
        sb.append("=== Top 10 Penggunaan 24 Jam Terakhir ===\n\n")

        val pm = packageManager
        val top10 = usageMap.entries.sortedByDescending { it.value }.take(10)

        for ((pkg, totalSec) in top10) {
            val appName = try {
                pm.getApplicationLabel(pm.getApplicationInfo(pkg, 0)).toString()
            } catch (e: Exception) {
                pkg
            }

            val jam = totalSec / 3600
            val menit = (totalSec % 3600) / 60
            val detik = totalSec % 60

            sb.append("$appName ($pkg) → ${jam}h ${menit}m ${detik}s\n\n")
        }

        tvResult.text = sb.toString()
        // HAPUS: sendDataToServer(jsonArray, totalScreenTimeSeconds)
    }


    // FUNGSI sendDataToServer() DIHAPUS DARI SINI

    // FUNGSI BARU: Menjadwalkan WorkManager
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