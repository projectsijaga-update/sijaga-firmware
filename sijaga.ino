/********************************************************************************
 * DETEKTOR JATUH CERDAS - VERSI EFISIENSI DAYA (REVISI)
 *
 * MODIFIKASI: Diintegrasikan dengan Firebase Realtime Database
 * PUSTAKA: Menggunakan Firebase_ESP_Client.h
 * PERBAIKAN v3: Menggunakan setJSON (bukan setString) & memperbaiki logika error
 *
 * FITUR UTAMA:
 * - Model ML dari Edge Impulse.
 * - State machine untuk validasi jatuh.
 * - ML Gate: Model ML hanya berjalan jika ada anomali G-force.
 * - Jaringan on-Demand: WiFi/Firebase only active when sending data.
 * - Logika Koneksi Non-Blocking: Deteksi jatuh tetap berjalan
 * meskipun WiFi/Firebase gagal terhubung.
 * - Sinkronisasi Waktu : Diperlukan oleh Firebase_ESP_Client.h
 * - Logika GPS 'null': Mengirim 'null' jika lokasi tidak valid.
 * - Heartbeat 30 Menit: Mengirim status aman setiap 30 menit.
 ********************************************************************************/

/* -------------------- 1. INCLUDES & LIBRARY (MODIFIKASI) -------------------- */
#include <SiJaga_inferencing.h>
#include <Adafruit_MPU6050.h>
#include <Adafruit_Sensor.h>
#include <Wire.h>
#include <TinyGPS++.h>
#include <WiFi.h>
#include <Firebase_ESP_Client.h> // <-- Pustaka yang diminta
#include <time.h>                   // <-- Diperlukan untuk NTP

/* -------------------- 2. KONFIGURASI & PARAMETER (MODIFIKASI) -------------------- */
// --- Konfigurasi WiFi ---
const char* ssid = "nubia Neo 5G";         // <-- GANTI DENGAN NAMA WIFI ANDA
const char* password = "12345678"; // <-- GANTI DENGAN PASSWORD WIFI ANDA

// --- (BARU) Konfigurasi Firebase Realtime Database ---
// URL Database Anda
#define FIREBASE_HOST "https://sijaga-95af3-default-rtdb.asia-southeast1.firebasedatabase.app/"
// Database Secret Anda (Metode Legacy)
#define FIREBASE_AUTH "xPHN5wbacgI46ui5kWxaHL7EHCvo6xbb2XIuHIgH"
// Path utama di RTDB tempat data akan ditulis
const char* firebase_path = "lansia/status";

// --- (BARU) Konfigurasi NTP (Waktu) ---
const char* ntp_server1 = "pool.ntp.org";
const char* ntp_server2 = "time.nist.gov";
const long gmt_offset_sec = 0; // Sesuaikan jika perlu (misal: WIB = 7 * 3600)
const int daylight_offset_sec = 0;
const int NTP_SYNC_TIMEOUT_MS = 10000; // Timeout 10 detik untuk sinkronisasi waktu

// --- (Point 3) Offset Kalibrasi (Sengaja 0.0 sesuai data latih) ---
const float AX_OFFSET_MS2 = 0.0;
const float AY_OFFSET_MS2 = 0.0;
const float AZ_OFFSET_MS2 = 0.0;
const float GX_OFFSET_RADS = 0.0;
const float GY_OFFSET_RADS = 0.0;
const float GZ_OFFSET_RADS = 0.0;

// --- Definisi Pin ---
const int RESET_BUTTON_PIN = 4;
const int BUZZER_PIN = 5;

// --- Parameter State Machine ---
const float CONFIDENCE_THRESHOLD_MEDIUM = 0.95;
const float CONFIDENCE_THRESHOLD_HIGH   = 0.98;
const int   INACTIVITY_DURATION_MS      = 5000;
const int   PRE_ALARM_DURATION_MS       = 15000;
const float MOTION_THRESHOLD            = 1.5;
const float ORIENTATION_THRESHOLD       = 4.0;

// --- (BARU - Point 1) Parameter Efisiensi ---
const long HEARTBEAT_INTERVAL_MS = 1800000; // 30 menit (30 * 60 * 1000)

// --- (BARU - Point 6) Parameter ML Gate ---
const float ML_GATE_FREEFALL_G = 0.5; // Threshold G rendah (mendekati 0)
const float ML_GATE_IMPACT_G   = 3.0; // Threshold G tinggi (benturan)

/* -------------------- 3. VARIABEL GLOBAL & OBJEK (MODIFIKASI) -------------------- */
Adafruit_MPU6050 mpu;
TinyGPSPlus gps;
HardwareSerial gpsSerial(2);

// --- (BARU) Objek Firebase ---
FirebaseData fbdo;
FirebaseAuth auth;
FirebaseConfig config;
FirebaseJson json; // (PERBAIKAN) Buat objek JSON global agar tidak dibuat ulang terus-menerus

enum StatusPerangkat { NORMAL, PRA_ALARM, SEDANG_MEMERIKSA, JATUH_TERKONFIRMASI };
StatusPerangkat status_perangkat = NORMAL;

unsigned long timer_pemeriksaan = 0;
int windows_to_skip = 3;
float svm_buffer[200];
int svm_buffer_index = 0;
float buffer[EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE] = {0};

// --- (MODIFIKASI) Variabel Global Jaringan Non-Blocking ---
// Ganti nama state agar lebih jelas
enum NetworkState { NET_OFF, NET_WANTED, NET_CONNECTING_WIFI, NET_SYNCING_NTP, NET_CONFIGURING_FIREBASE, NET_READY_TO_SEND };
NetworkState network_state = NET_OFF;
unsigned long last_heartbeat_time = 0;
unsigned long last_alarm_send_time = 0; // Untuk spam alarm
String pending_message = "";
String pending_message_path = ""; 
bool disconnect_after_send = true; 
  
/* -------------------- 4. FUNGSI SETUP (MODIFIKASI) -------------------- */
void setup() {
  Serial.begin(115200);
  // (Point 4) GPS Serial diaktifkan di awal dan biarkan berjalan
  gpsSerial.begin(9600, SERIAL_8N1, 16, 17);
  while (!Serial);
  Serial.println("Inisialisasi Detektor Jatuh Cerdas (Versi Efisiensi - FIREBASE CLIENT)...");

  pinMode(RESET_BUTTON_PIN, INPUT_PULLUP);
  pinMode(BUZZER_PIN, OUTPUT);

  if (!mpu.begin()) {
    Serial.println("Gagal menemukan sensor MPU6050!"); while (1);
  }
  Serial.println("MPU6050 Ditemukan.");
  
  mpu.setAccelerometerRange(MPU6050_RANGE_16_G);
  mpu.setGyroRange(MPU6050_RANGE_2000_DEG);
  mpu.setFilterBandwidth(MPU6050_BAND_21_HZ);

  // --- (BARU) Konfigurasi Awal Firebase Client (Non-Blocking) ---
  Serial.println("Mengkonfigurasi Firebase...");
  config.host = FIREBASE_HOST;
  
  // (PERBAIKAN 1) Menggunakan 'signer.tokens.legacy_token' untuk Database Secret
  config.signer.tokens.legacy_token = FIREBASE_AUTH;
  
  // Email & UID dummy diperlukan untuk auth secret
  auth.user.email = "esp32@sijaga.com";
  auth.user.password = "password";
  
  // (PERBAIKAN 2) Menggunakan nama variabel timeout yang baru
  config.timeout.rtdbKeepAlive = 20000; // 20 detik keep-alive


  Serial.println("Setup selesai. Mematikan WiFi.");
  WiFi.mode(WIFI_OFF); // Pastikan WiFi mati saat startup
  network_state = NET_OFF;

  // (Point 1) Kirim status aman pertama kali saat startup
  last_heartbeat_time = millis();
  kirim_status_aman("Aman - Startup"); 
  
  if (EI_CLASSIFIER_RAW_SAMPLES_PER_FRAME != 6) {
    ei_printf("ERR: Jumlah sumbu sensor tidak cocok!\n");
  }
}

/* -------------------- 5. FUNGSI LOOP UTAMA (MODIFIKASI) -------------------- */
void loop() {
  // (Point 4) GPS selalu membaca data di background
  while (gpsSerial.available() > 0) {
    gps.encode(gpsSerial.read());
  }
  
  // (Point 1 & 5) Mesin status untuk koneksi jaringan non-blocking
  handle_network_connection();

  // (Point 1) Cek timer untuk heartbeat 30 menit
  handle_heartbeat();

  // (Point 6) Fungsi baru untuk sensor dan ML Gate
  handle_sensor_and_ml();
}

/* -------------------- 6. LOGIKA SENSOR & ML GATE (TIDAK BERUBAH) -------------------- */
void handle_sensor_and_ml() {
  // 1. Isi buffer sensor
  for (size_t ix = 0; ix < EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE; ix += EI_CLASSIFIER_RAW_SAMPLES_PER_FRAME) {
    sensors_event_t a, g, temp;
    mpu.getEvent(&a, &g, &temp);
    
    float ax_calibrated = a.acceleration.x - AX_OFFSET_MS2;
    float ay_calibrated = a.acceleration.y - AY_OFFSET_MS2;
    float az_calibrated = a.acceleration.z - AZ_OFFSET_MS2;
    float gx_calibrated_rads = g.gyro.x - GX_OFFSET_RADS;
    float gy_calibrated_rads = g.gyro.y - GY_OFFSET_RADS;
    float gz_calibrated_rads = g.gyro.z - GZ_OFFSET_RADS;

    buffer[ix + 0] = ax_calibrated / SENSORS_GRAVITY_STANDARD;
    buffer[ix + 1] = ay_calibrated / SENSORS_GRAVITY_STANDARD;
    buffer[ix + 2] = az_calibrated / SENSORS_GRAVITY_STANDARD;
    buffer[ix + 3] = gx_calibrated_rads * 180 / M_PI;
    buffer[ix + 4] = gy_calibrated_rads * 180 / M_PI;
    buffer[ix + 5] = gz_calibrated_rads * 180 / M_PI;
    
    delay(5);
  }

  // 2. Logika Pemanasan
  if (windows_to_skip > 0) {
    windows_to_skip--;
    Serial.print("Periode pemanasan... Sisa: "); Serial.println(windows_to_skip);
    return;
  }

  // 3. (BARU - Point 6) ML Gate
  bool triggerML = false;
  for (size_t ix = 0; ix < EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE; ix += EI_CLASSIFIER_RAW_SAMPLES_PER_FRAME) {
      float ax_g = buffer[ix + 0]; // Data sudah dalam G
      float ay_g = buffer[ix + 1];
      float az_g = buffer[ix + 2];
      float mag = sqrt(ax_g * ax_g + ay_g * ay_g + az_g * az_g);
      
      if (mag < ML_GATE_FREEFALL_G || mag > ML_GATE_IMPACT_G) {
          triggerML = true;
          Serial.println(">>> ML GATE: Anomali G-force terdeteksi! Menjalankan ML...");
          break; 
      }
  }

  // Jika tidak ada anomali DAN status NORMAL, lewati ML
  if (!triggerML && status_perangkat == NORMAL) {
      // Serial.println("ML Gate: Aman, skip ML."); // (Aktifkan untuk debug)
      return; 
  }

  // 4. Jalankan Classifier
  signal_t signal;
  numpy::signal_from_buffer(buffer, EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE, &signal);
  
  ei_impulse_result_t result = {0};
  run_classifier(&signal, &result, false);
  
  // 5. Panggil state machine utama
  handle_state_machine(result);
}

/* -------------------- 7. (MODIFIKASI v3) LOGIKA KONEKSI JARINGAN -------------------- */

// (MODIFIKASI) Fungsi untuk meminta pengiriman data
void request_network_send(String path, String payload, bool disconnect) {
  if (network_state == NET_OFF) {
    pending_message = payload;
    pending_message_path = path; 
    disconnect_after_send = disconnect;
    network_state = NET_WANTED; 
    Serial.println("NET: Permintaan kirim diterima. Status -> NET_WANTED");
  } else {
    // Jika jaringan sibuk (misal: sedang kirim heartbeat, lalu jatuh)
    // Prioritaskan pesan baru jika itu JATUH
    if (status_perangkat == JATUH_TERKONFIRMASI) {
      Serial.println("NET: Jaringan sibuk, TAPI INI JATUH. Menimpa pesan...");
      pending_message = payload;
      pending_message_path = path;
      disconnect_after_send = false;
    } else {
      Serial.println("NET: Gagal kirim, jaringan sedang sibuk.");
    }
  }
}

// (PERBAIKAN v3) Fungsi untuk mematikan jaringan secara paksa
void disconnect_network() {
    Serial.println("NET: Mematikan jaringan secara manual...");
    WiFi.mode(WIFI_OFF);
    network_state = NET_OFF;
    // (PERBAIKAN) JANGAN hapus 'pending_message' di sini.
    // Biarkan si pemanggil yang memutuskan.
}

// (MODIFIKASI) Mesin status koneksi (Non-Blocking)
void handle_network_connection() {
  switch (network_state) {
    case NET_OFF:
      // Tidak melakukan apa-apa, hemat daya
      break;
    
    case NET_WANTED:
      // Ada permintaan kirim, nyalakan WiFi
      Serial.print("NET: Menyalakan WiFi... Menghubungkan ke: ");
      Serial.println(ssid);
      WiFi.mode(WIFI_STA);
      WiFi.begin(ssid, password);
      network_state = NET_CONNECTING_WIFI;
      break;
    
    case NET_CONNECTING_WIFI:
      // Menunggu WiFi terhubung
      if (WiFi.status() == WL_CONNECTED) {
        Serial.println("\nNET: WiFi terhubung! IP: " + WiFi.localIP().toString());
        network_state = NET_SYNCING_NTP; // (BARU) Lanjut ke sinkronisasi Waktu
      } else {
        Serial.print("."); // Tampilan 'loading'
        delay(100); // Jangan spam loop
      }
      break;

    case NET_SYNCING_NTP:
      // (PERBAIKAN 3) Menambahkan kurung kurawal {} untuk memperbaiki error 'jump to case label'
      { 
        // (BARU) State untuk sinkronisasi waktu (NTP), semi-blocking
        Serial.print("NET: WiFi terhubung. Sinkronisasi Waktu NTP...");
        configTime(gmt_offset_sec, daylight_offset_sec, ntp_server1, ntp_server2);
        
        unsigned long startAttemptTime = millis();
        while (time(nullptr) < 1672531200) { // Cek apakah waktu sudah valid (lewat 1 Jan 2023)
          if (millis() - startAttemptTime > NTP_SYNC_TIMEOUT_MS) {
              Serial.println("\nNET: Gagal sinkronisasi NTP (timeout). Mematikan WiFi.");
              disconnect_network(); // Gagal, matikan saja
              // Keluar dari state, tapi JANGAN 'return' agar loop() utama tetap jalan
              break; 
          }
          Serial.print(".");
          delay(200);
        }
        
        // Cek lagi apakah kita berhasil atau timeout
        if (time(nullptr) >= 1672531200) {
            Serial.println("\nNET: Waktu NTP didapat.");
            network_state = NET_CONFIGURING_FIREBASE; // Lanjut ke auth Firebase
        }
      }
      break;

    case NET_CONFIGURING_FIREBASE:
      // (MODIFIKASI) Menggantikan state 'NET_CONNECTING_MQTT'
      Serial.print("NET: Mengkonfigurasi Firebase Auth...");
      
      // Panggil Firebase.begin() SEKARANG setelah WiFi dan NTP siap
      Firebase.begin(&config, &auth);
      Firebase.reconnectWiFi(true); // Memberitahu lib bahwa WiFi sudah siap
      
      // Cek apakah auth berhasil (biasanya instan dengan secret)
      if (Firebase.ready()) {
          Serial.println(" Sukses!");
          network_state = NET_READY_TO_SEND;
      } else {
          Serial.print(" Gagal! Error: ");
          Serial.println(fbdo.errorReason());
          Serial.println("NET: Gagal init Firebase, mematikan WiFi.");
          disconnect_network(); // Gagal, matikan saja
      }
      break;
    
    case NET_READY_TO_SEND:
      // (PERBAIKAN v3) Logika pengiriman dan error handling
      { // Tambahkan scope untuk variabel
        
        // Cek dulu apakah koneksi masih ada
        if (!Firebase.ready()) {
             Serial.println("NET: Koneksi Firebase/WiFi terputus, re-init...");
             // Jika putus, harus mengulang dari sinkronisasi NTP
             network_state = NET_SYNCING_NTP;
             break; // Keluar dari case ini
        }

        // Sudah terhubung, kirim pesan yang tertunda
        if (pending_message != "") {
          Serial.print("NET: Mengirim pesan Firebase ke path: ");
          Serial.println(pending_message_path);
          Serial.print("NET: Payload: ");
          Serial.println(pending_message);

          // (PERBAIKAN v3) Muat string JSON kita ke objek json global
          json.setJsonData(pending_message); 

          // (PERBAIKAN v3) Gunakan Firebase.RTDB.setJSON()
          if (Firebase.RTDB.setJSON(&fbdo, pending_message_path, &json)) {
            Serial.println("NET: Firebase.RTDB.setJSON() BERHASIL");
            pending_message = ""; // Kosongkan buffer
            pending_message_path = "";
          } else {
            Serial.print("NET: Firebase.RTDB.setJSON() GAGAL: ");
            Serial.println(fbdo.errorReason());
            
            if (disconnect_after_send) { // Ini adalah heartbeat / status aman
               Serial.println("NET: Gagal kirim (tidak kritis), mematikan WiFi.");
               disconnect_network(); // Matikan saja, hemat daya
               pending_message = ""; // <-- HAPUS pesan (tidak perlu dikirim ulang)
               pending_message_path = "";
            } else { // Ini adalah mode JATUH
               Serial.println("NET: Gagal kirim JATUH, akan dicoba lagi...");
               // JANGAN hapus pesan, biarkan state machine mencoba lagi
            }
            break; // <-- Keluar dari case SETELAH gagal kirim
          }
        }

        // Cek jika sudah tidak ada pesan (pending_message == "") DAN harus disconnect
        if (pending_message == "" && disconnect_after_send) {
          Serial.println("NET: Pesan terkirim, mematikan WiFi...");
          disconnect_network();
        } else if (pending_message == "" && !disconnect_after_send) {
          // Mode JATUH, pesan berhasil terkirim, tapi jangan disconnect
        }
      } // Akhir dari scope
      break;
  }
}

// (TIDAK BERUBAH) Fungsi untuk cek heartbeat 30 menit
void handle_heartbeat() {
  if (status_perangkat == NORMAL && network_state == NET_OFF) {
    if (millis() - last_heartbeat_time > HEARTBEAT_INTERVAL_MS) {
      Serial.println("HEARTBEAT: 30 menit tercapai. Mengirim status aman...");
      kirim_status_aman("Aman - Heartbeat");
      last_heartbeat_time = millis(); // Reset timer
    }
  }
}


/* -------------------- 8. FUNGSI PENGIRIMAN DATA (MODIFIKASI) -------------------- */

// (MODIFIKASI v2) Helper untuk membuat JSON payload
String build_json_payload(String status) {
  char json_buffer[256];
  unsigned long now = millis();
  
  if (gps.location.isValid()) {
    Serial.print("Lokasi Terdeteksi: ");
    Serial.print(gps.location.lat(), 6); Serial.print(", "); Serial.println(gps.location.lng(), 6);
    
    // (PERBAIKAN 4 - v2) Mengganti status.c.str() menjadi status.c_str()
    snprintf(json_buffer, sizeof(json_buffer), 
      "{\"status\": \"%s\", \"latitude\": %.6f, \"longitude\": %.6f, \"timestamp\": \"%lu\"}", 
      status.c_str(), gps.location.lat(), gps.location.lng(), now);
  } else {
    Serial.println("Lokasi GPS belum ditemukan (invalid). Mengirim 'null'...");
     // (PERBAIKAN 5 - v2) Mengganti status.c.str() menjadi status.c_str()
     snprintf(json_buffer, sizeof(json_buffer), 
      "{\"status\": \"%s\", \"latitude\": null, \"longitude\": null, \"timestamp\": \"%lu\"}", 
      status.c_str(), now);
  }
  return String(json_buffer);
}

// (MODIFIKASI) Fungsi untuk memicu pengiriman status aman
void kirim_status_aman(String reason) {
  String payload = build_json_payload(reason);
  // Kirim ke path Firebase utama
  request_network_send(firebase_path, payload, true); // true = disconnect setelah kirim
}

// (MODIFIKASI) Fungsi untuk memicu pengiriman status JATUH
void kirim_status_jatuh() {
  Serial.println("\n--- MENGIRIM PERINGATAN JATUH VIA FIREBASE ---");
  tone(BUZZER_PIN, 1500);
  
  String payload = build_json_payload("JATUH");
  // Kirim ke path Firebase utama
  request_network_send(firebase_path, payload, false); // false = JANGAN disconnect
}


/* -------------------- 9. FUNGSI STATE MACHINE & BANTUAN (MODIFIKASI) -------------------- */
// Sebagian besar tidak berubah, hanya panggilan fungsinya

void handle_state_machine(ei_impulse_result_t result) {
  float confidence_jatuh = result.classification[0].value;
  
  switch (status_perangkat) {
    case NORMAL:
      Serial.print("Status: NORMAL | Conf. Jatuh: "); Serial.println(confidence_jatuh, 2);
      if (confidence_jatuh > CONFIDENCE_THRESHOLD_HIGH) {
        status_perangkat = PRA_ALARM;
        timer_pemeriksaan = millis();
        Serial.println(">>> JATUH KRITIS TERDETEKSI! Memulai Pra-Alarm 15 detik...");
        tone(BUZZER_PIN, 1000, 200);
      } 
      else if (confidence_jatuh > CONFIDENCE_THRESHOLD_MEDIUM) {
        status_perangkat = SEDANG_MEMERIKSA;
        timer_pemeriksaan = millis();
        svm_buffer_index = 0;
        Serial.println(">>> Potensi jatuh terdeteksi! Memulai pemeriksaan 5 detik...");
        kirim_status_aman("Aman - Pengecekan");
      }
      break;

    case PRA_ALARM:
      Serial.print("Status: PRA_ALARM | Waktu tersisa: "); Serial.print((PRE_ALARM_DURATION_MS - (millis() - timer_pemeriksaan)) / 1000); Serial.println("s");
      tone(BUZZER_PIN, 1200, 100);
      if (digitalRead(RESET_BUTTON_PIN) == LOW) {
        status_perangkat = NORMAL;
        Serial.println("--- Pra-Alarm Dibatalkan oleh pengguna ---");
        noTone(BUZZER_PIN);
        kirim_status_aman("Aman - Reset");
      }
      else if (millis() - timer_pemeriksaan > PRE_ALARM_DURATION_MS) {
        status_perangkat = JATUH_TERKONFIRMASI;
        noTone(BUZZER_PIN);
        kirim_status_jatuh(); // Panggil fungsi baru
        last_alarm_send_time = millis(); // Set timer spam
      }
      break;

    case SEDANG_MEMERIKSA:
      sensors_event_t a_now, g_now, temp_now; 
      mpu.getEvent(&a_now, &g_now, &temp_now);
      collect_svm_data(a_now.acceleration.x, a_now.acceleration.y, a_now.acceleration.z);
      Serial.print("Status: SEDANG_MEMERIKSA | Timer: "); Serial.print((millis() - timer_pemeriksaan) / 1000); Serial.println("s");
      
      if (millis() - timer_pemeriksaan > INACTIVITY_DURATION_MS) {
        float motion_level = calculate_motion_level();
        Serial.print("    -> Level gerakan: "); Serial.println(motion_level);
        if (motion_level < MOTION_THRESHOLD) {
          if (check_final_orientation()) { 
            status_perangkat = JATUH_TERKONFIRMASI;
            kirim_status_jatuh(); // Panggil fungsi baru
            last_alarm_send_time = millis(); // Set timer spam
          } else {
            status_perangkat = NORMAL;
            Serial.println("--- Alarm Dibatalkan (orientasi normal) ---");
          }
        } else {
          status_perangkat = NORMAL;
          Serial.println("--- Alarm Dibatalkan (terdeteksi gerakan) ---");
        }
      }
      break;

    case JATUH_TERKONFIRMASI:
      Serial.println("!!! ALARM: JATUH TERKONFIRMASI. Tekan tombol untuk reset. !!!");
      tone(BUZZER_PIN, 1500);
      
      // (MODIFIKASI) Tetap kirim peringatan jatuh setiap 5 detik
      if (millis() - last_alarm_send_time > 5000) {
        Serial.println("NET: Mengirim ulang peringatan JATUH...");
        kirim_status_jatuh(); // Kirim ulang
        last_alarm_send_time = millis(); // Reset timer spam
      }

      if (digitalRead(RESET_BUTTON_PIN) == LOW) {
        status_perangkat = NORMAL;
        Serial.println("--- Sistem di-reset ke NORMAL oleh pengguna ---");
        noTone(BUZZER_PIN);
        
        // (MODIFIKASI) Logika Reset Jaringan
        if(network_state != NET_OFF) {
            // Jika masih terhubung (mode JATUH), kirim pesan reset dulu baru disconnect
            String payload = build_json_payload("Aman - Reset");
            Serial.println("NET: Mengirim status 'Aman - Reset' sebelum disconnect...");
            
            // (PERBAIKAN v3) Gunakan setJSON juga di sini
            json.setJsonData(payload);
            if (Firebase.RTDB.setJSON(&fbdo, firebase_path, &json)) {
                Serial.println("NET: Pesan 'Reset' terkirim.");
            } else {
                Serial.println("NET: Gagal kirim 'Reset'.");
            }
            delay(100); // Beri waktu kirim
            disconnect_network();
        } else {
            // Jika jaringan sudah mati, kirim pesan reset (dia akan nyala lalu mati lagi)
            kirim_status_aman("Aman - Reset");
        }
      }
      delay(200); // delay asli dari kode Anda
      break;
  }
}

// --- Fungsi Bantuan (Tidak Berubah) ---
bool check_final_orientation() {
  sensors_event_t a, g, temp;
  mpu.getEvent(&a, &g, &temp);
  float ax_cal = a.acceleration.x - AX_OFFSET_MS2;
  float ay_cal = a.acceleration.y - AY_OFFSET_MS2;
  float az_cal = a.acceleration.z - AZ_OFFSET_MS2;
  const float GRAVITY_HIGH_THRESHOLD = 7.5;
  const float GRAVITY_LOW_THRESHOLD  = 4.0;
  bool isVerticalAxisLow = abs(az_cal) < GRAVITY_LOW_THRESHOLD;
  bool isHorizontalAxisHigh = abs(ax_cal) > GRAVITY_HIGH_THRESHOLD || abs(ay_cal) > GRAVITY_HIGH_THRESHOLD;
  if (isVerticalAxisLow && isHorizontalAxisHigh) {
    Serial.println("    -> Cek Orientasi: GAGAL (Posisi tergeletak terdeteksi)");
    return true;
  }
  Serial.println("    -> Cek Orientasi: AMAN (Posisi masih tegak/normal)");
  return false;
}

void collect_svm_data(float ax, float ay, float az) {
  if (svm_buffer_index < 200) {
    svm_buffer[svm_buffer_index] = sqrt(ax * ax + ay * ay + az * az);
    svm_buffer_index++;
  } else {
    for(int i = 0; i < 199; i++) svm_buffer[i] = svm_buffer[i+1];
    svm_buffer[199] = sqrt(ax * ax + ay * ay + az * az);
  }
}

float calculate_motion_level() {
  int current_samples = (svm_buffer_index < 200) ? svm_buffer_index : 200;
  if (current_samples == 0) return 99.0;
  float sum = 0.0, mean = 0.0, std_dev_sum = 0.0;
  for (int i = 0; i < current_samples; i++) sum += svm_buffer[i];
  mean = sum / current_samples;
  for (int i = 0; i < current_samples; i++) std_dev_sum += pow(svm_buffer[i] - mean, 2);
  return sqrt(std_dev_sum / current_samples);
}
