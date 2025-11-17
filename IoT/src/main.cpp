#include <WiFi.h>
#include <HTTPClient.h>
#include <DHT.h>
#include <Wire.h>
#include <Adafruit_SSD1306.h>
#include <Adafruit_GFX.h>

// Konfigurasi WiFi (simulasi Wokwi)
const char* ssid = "Wokwi-GUEST";
const char* password = "";

// Konfigurasi Server Flask (gunakan HTTP untuk Wokwi; ganti ke HTTPS jika ngrok)
const char* serverUrl = "http://192.168.1.100:5000/receive_sensor";  // Ganti dengan URL ngrok jika perlu (e.g., https://abc123.ngrok.io/receive_sensor)

// Konfigurasi Sensor
#define DHTPIN 15
#define DHTTYPE DHT22
#define MQ2_PIN 34
DHT dht(DHTPIN, DHTTYPE);

// Konfigurasi OLED SSD1306 (untuk display)
#define SCREEN_WIDTH 128
#define SCREEN_HEIGHT 64
#define OLED_RESET -1
Adafruit_SSD1306 display(SCREEN_WIDTH, SCREEN_HEIGHT, &Wire, OLED_RESET);

void setup() {
  Serial.begin(115200);
  
  // Inisialisasi DHT
  dht.begin();
  
  // Inisialisasi OLED
  if (!display.begin(SSD1306_SWITCHCAPVCC, 0x3C)) {
    Serial.println(F("SSD1306 allocation failed"));
    for (;;);
  }
  display.clearDisplay();
  display.setTextSize(1);
  display.setTextColor(WHITE);
  display.setCursor(0, 0);
  display.println("IoT Sensor Display");
  display.display();
  
  // Koneksi WiFi
  WiFi.begin(ssid, password);
  Serial.print("Connecting to WiFi");
  while (WiFi.status() != WL_CONNECTED) {
    delay(1000);
    Serial.print(".");
  }
  Serial.println("\nConnected to WiFi!");
  display.println("WiFi Connected");
  display.display();
}

void loop() {
  // Baca sensor
  float suhu = dht.readTemperature();
  float kelembapan = dht.readHumidity();
  int mq2_value = analogRead(MQ2_PIN);
  float kualitas_udara = map(mq2_value, 0, 4095, 0, 100);  // 0–100%
  
  // Cek jika DHT gagal
  if (isnan(suhu) || isnan(kelembapan)) {
    Serial.println("Failed to read from DHT sensor!");
    display.clearDisplay();
    display.setCursor(0, 0);
    display.println("Sensor Error!");
    display.display();
    delay(5000);
    return;
  }
  
  // Tampilkan di Serial
  Serial.println("=== Data Sensor ===");
  Serial.printf("Suhu: %.2f °C | Kelembapan: %.2f %% | Kualitas Udara: %.2f %%\n",
                suhu, kelembapan, kualitas_udara);
  
  // Tampilkan di OLED
  display.clearDisplay();
  display.setCursor(0, 0);
  display.printf("Temp: %.1f C\n", suhu);
  display.printf("Hum: %.1f %%\n", kelembapan);
  display.printf("Air Qual: %.1f %%\n", kualitas_udara);
  display.printf("Sending...");
  display.display();
  
  // Kirim data ke server Flask
  if (WiFi.status() == WL_CONNECTED) {
    WiFiClient client;  // Gunakan WiFiClient untuk HTTP (bukan WiFiClientSecure)
    HTTPClient http;
    http.begin(client, serverUrl);
    http.addHeader("Content-Type", "application/json");
    
    String payload = "{\"temperature\":" + String(suhu, 2) +
                     ",\"humidity\":" + String(kelembapan, 2) +
                     ",\"air_quality\":" + String(kualitas_udara, 2) + "}";
    
    int httpResponseCode = http.POST(payload);
    Serial.print("Response Code: ");
    Serial.println(httpResponseCode);
    
    if (httpResponseCode > 0) {
      String response = http.getString();
      Serial.println("Server Response: " + response);
      display.printf("Sent OK");
    } else {
      Serial.println("Failed to send data.");
      display.printf("Send Failed");
    }
    http.end();
  } else {
    Serial.println("WiFi disconnected!");
    display.printf("WiFi Error");
  }
  
  display.display();
  delay(10000);  // Kirim setiap 10 detik
}
