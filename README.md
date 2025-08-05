# Final-Project-Log-Analyzer
# AI Destekli Log Anomali Tespit Sistemi

Bu proje, dağıtık sistemlerden gelen log verilerini gerçek zamanlı olarak analiz eden ve **anomalileri yapay zekâ desteğiyle tespit eden bir mikroservis tabanlı sistemdir**.

---

## Proje Özeti

Log verileri sistemlerin iç işleyişini anlamak ve hataları erken tespit edebilmek için kritik öneme sahiptir. Bu projede amaç, büyük miktarda log verisi arasından **normal dışı davranışları otomatik olarak tespit etmek** ve kullanıcıya açıklayıcı öneriler sunmaktır.

---

## Sistem Mimarisi

- **Kafka** → Log akışlarını yöneten mesajlaşma sistemi  
- **Zookeeper** → Kafka'yı yöneten koordinatör  
- **Producer** → Farklı sistem loglarını Kafka'ya gönderen modül  
- **Backend (FastAPI)** → AI modeli ile anomalileri analiz eder ve veritabanına kaydeder  
- **PostgreSQL** → Anomali verilerini tutar  
- **Frontend (React)** → Kullanıcıya görsel arayüz sunar (anomalileri, skorları, açıklamaları görüntüler)  

Tüm bileşenler `Docker` ile izole çalışır ve `docker-compose` ile kolayca yönetilir.

---

## Yapay Zekâ Desteği

Model: `Isolation Forest` (Unsupervised Learning)

Eğitim için kullanılan öznitelikler:
- EventTemplate uzunluğu  
- Saat bilgisi (timestamp'tan çıkarılır)  
- Log seviyesi (`INFO`, `WARN`, `ERROR`)  
- TF-IDF (EventTemplate içindeki kelimelere göre)  

Model `train_advanced_model.py` ile eğitilir ve `backend/app/advanced_model.pkl` olarak kaydedilir.

---

## Kurulum ve Başlatma

# 1. Modeli eğit
python backend/app/train_advanced_model.py

# 2. Servisleri başlat
docker-compose up --build
Uygulama çalıştığında:

API: http://localhost:8000/anomalies/{log_type}

Arayüz: http://localhost:3001

🖼Arayüz Özellikleri
Log tipi bazında filtreleme

Anomali listesi ve açıklamaları

Yapay zekâ skorları ve öneriler

Tablo + grafik gösterimleri (Chart.js, Tailwind, DaisyUI)

Örnek API Çıktısı
json
Copy
Edit
{
  "log_type": "zookeeper-traces",
  "trace_id": "zookeeper_1234",
  "ts": "17:44:32,982",
  "event": "E13",
  "score": -0.1976,
  "rec": "⚠ Moderate anomaly – review system logs",
  "reason": "Consider increasing timeout.",
  "type": "Timeout"
}
## Kullanılan Teknolojiler
Python 3.10, FastAPI, scikit-learn

React.js, Chart.js, DaisyUI

PostgreSQL, Kafka, Docker

TF-IDF, Isolation Forest (AI modeli)

## Geliştirici
Berkan Parlak
Bilgisayar Mühendisliği Bitirme Projesi – 2025
