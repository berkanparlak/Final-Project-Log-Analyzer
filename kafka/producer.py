import os
import time
import csv
import json
from kafka import KafkaProducer

PRODUCER_TOPIC = 'hdfs-traces'
KAFKA_BOOTSTRAP = 'kafka:9092'

print("Producer başlatıldı")

# Kafka'ya bağlanmak için 10 kez dene
producer = None
for i in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode()
        )
        print("Kafka'ya bağlandı")
        break
    except Exception as e:
        print(f"Kafka hazır değil ({i+1}/10). Bekleniyor...")
        time.sleep(5)

if not producer:
    print("Kafka bağlantısı başarısız. Çıkılıyor.")
    exit(1)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRACE_PATH = os.path.join(BASE_DIR, 'data/raw/Event_traces.csv')
LABEL_PATH = os.path.join(BASE_DIR, 'data/raw/anomaly_label.csv')


def load_traces(trace_csv, label_csv):
    print("📂 Dosyalar yükleniyor...")
    traces = {}
    with open(trace_csv) as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if len(row) >= 3:
                tid, ts, event = row[:3]
                traces.setdefault(tid, []).append({'ts': ts, 'event': event})
    print(f"Trace satırları yüklendi: {len(traces)}")

    labels = {}
    with open(label_csv) as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if len(row) >= 2:
                tid, label = row[:2]
                labels[tid] = label
    print(f"Label satırları yüklendi: {len(labels)}")

    return [(tid, traces[tid], labels.get(tid, '0')) for tid in traces]

def run():
    all_traces = load_traces(TRACE_PATH, LABEL_PATH)
    print(f"🧪 Toplam trace: {len(all_traces)}")

    for i, (tid, events, label) in enumerate(all_traces):
        if i >= 100:
            break
        for evt in events:
            msg = {
                'trace_id': tid,
                'ts': evt['ts'],
                'event': evt['event'],
                'label': label
            }
            producer.send(PRODUCER_TOPIC, msg)
            print(f"📤 Kafka'ya gönderildi: {msg}")
            time.sleep(0.01)

    print("İlk 100 trace Kafka'ya gönderildi.")

if __name__ == "__main__":
    run()
