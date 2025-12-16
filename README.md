# 🧠 Stress Detection Based on Smartphone Activity and Room Quality

Proyek ini bertujuan untuk **mendeteksi tingkat stres pengguna** berdasarkan **aktivitas smartphone** dan **kualitas lingkungan ruangan** menggunakan pendekatan **Fuzzy Logic**.  
Sistem ini menggabungkan data dari **smartphone** (melalui Digital Wellbeing dan sensor internal) serta **IoT device** (sensor suhu, kelembapan, dan kualitas udara) untuk menghasilkan analisis kondisi mental pengguna secara otomatis dan berkelanjutan.

---

## 📱 Sistem Utama

### 1. Pengumpulan Data
- **Smartphone Data:**
  - Durasi penggunaan aplikasi (terutama kategori sosial, hiburan, dan produktivitas)
  - Frekuensi buka-tutup aplikasi
  - Aktivitas layar aktif/inaktif
- **IoT Sensor Data:**
  - Suhu (°C)
  - Kelembapan (%)
  - Kualitas udara (CO₂, VOC, atau PM2.5)

Data dikirim secara **otomatis dan berkala** ke server Python melalui API, **meskipun aplikasi tidak sedang dibuka**, agar sistem dapat memantau kondisi pengguna secara real-time.

---

### 2. Penyimpanan Dataset
Semua data aktivitas dikombinasikan dan disimpan dalam **dataset `.csv`** agar mudah digunakan untuk:
- Analisis statistik
- Pelatihan model fuzzy
- Monitoring perubahan stres pengguna dari waktu ke waktu

---

### 3. Pemrosesan Data (Server Python)
Server menggunakan **Flask** untuk menerima data dari smartphone dan perangkat IoT.  
Kemudian data diolah menggunakan **Fuzzy Logic System** dengan parameter utama seperti:
- Intensitas penggunaan smartphone
- Kondisi lingkungan (panas, lembap, atau pengap)
- Pola aktivitas harian

Output sistem berupa **tingkat stres (rendah, sedang, tinggi)**.

---

### 4. Hasil Deteksi
Hasil akhir ditampilkan pada perangkat pengguna melalui **pop-up notifikasi**, menyerupai notifikasi sistem (mirip implementasi SFML GUI).  
Contoh:
> ⚠️ Anda tampak stres. Disarankan untuk beristirahat sejenak dan kurangi penggunaan smartphone.

---

## ⚙️ Arsitektur Sistem
Smartphone ─┬──> Flask Server (Python)
│ │
│ ├──> Fuzzy Processing
│ │
IoT Sensors ─┘ └──> Dataset (.csv) + Pop-up Result

---

## 🧩 Teknologi yang Digunakan
- **Python (Flask, Numpy, Pandas, Scikit-Fuzzy)**  
- **Android (Digital Wellbeing / Usage Stats API)**  
- **IoT (ESP32 / DHT11 / MQ135 atau sensor lingkungan lainnya)**  
- **CSV Dataset Logging**  
- **Fuzzy Logic Inference System**  

---

## 🧠 Metode Fuzzy Logic
Sistem fuzzy digunakan untuk mengubah input numerik menjadi kategori linguistik seperti:
- *Durasi penggunaan tinggi*
- *Suhu ruangan panas*
- *Kelembapan rendah*

Dengan rule base seperti:
IF usage IS high AND temperature IS hot THEN stress IS high
IF usage IS moderate AND air_quality IS good THEN stress IS low

---

## 📊 Output
- Dataset otomatis disimpan ke file `dataset.csv`
- Hasil inferensi fuzzy ditampilkan di log dan pop-up device
- Dapat diperluas untuk visualisasi dashboard atau pelatihan model AI di masa depan

---

## 🚀 Tujuan Akhir
Membangun sistem **deteksi stres cerdas** berbasis **aktivitas digital dan lingkungan**, yang:
- Berjalan otomatis di background
- Menggabungkan sumber data lintas perangkat
- Memberikan umpan balik langsung kepada pengguna

---

## 👩‍💻 Pengembang
**Nama:** Zefa, Abyan, Nabil, Raihan  
**Program Studi:** Teknik Komputer, Universitas Jenderal Soedirman  
**Tahun:** 2025  

---

## 📁 Struktur Proyek (Contoh)

System Monitoring/
│
├── App/                         # Project Android
│   ├── app/src/main/java/...
│   └── build.gradle.kts
│
├── Server/                      # Server Python
│   ├── server.py
│   ├── usage_data.csv
│   └── usage_log.txt
│
└── README.md                    # Dokumentasi proyek

---

## 🧩 Rencana Pengembangan
- [ ] Menambah model Machine Learning sebagai pembanding
- [ ] Menampilkan hasil pada dashboard web
- [ ] Sinkronisasi data dengan cloud storage
- [ ] Pengujian lapangan untuk validasi data real

---

## 📜 Lisensi
Proyek ini dikembangkan untuk tujuan akademik dan penelitian.

