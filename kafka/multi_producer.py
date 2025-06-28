import os
import time
import csv
import json
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = 'kafka:9092'
BASE_PATH = '/app/data/raw'

LOG_CONFIG = {
    'hdfs': {
        'file': 'Event_traces.csv',
        'label_file': 'anomaly_label.csv',
        'topic': 'hdfs-traces',
        'label_required': True
    },
    'spark': {
        'file': 'Spark_2k.log_structured.csv',
        'topic': 'spark-traces'
    },
    'bgl': {
        'file': 'BGL_2k.log_structured.csv',
        'topic': 'bgl-traces'
    },
    'openstack': {
        'file': 'OpenStack_2k.log_structured.csv',
        'topic': 'openstack-traces'
    },
    'android': {
        'file': 'Android_2k.log_structured.csv',
        'topic': 'android-traces'
    },
    'zookeeper': {
        'file': 'Zookeeper_2k.log_structured.csv',
        'topic': 'zookeeper-traces'
    }
}

def connect_producer():
    for i in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode()
            )
            print("✅ Kafka'ya bağlanıldı.")
            return producer
        except Exception as e:
            print(f"⏳ Kafka bekleniyor... ({i+1}/10)")
            time.sleep(5)
    raise Exception("❌ Kafka bağlantısı sağlanamadı.")

def send_hdfs(producer):
    trace_path = os.path.join(BASE_PATH, LOG_CONFIG['hdfs']['file'])
    label_path = os.path.join(BASE_PATH, LOG_CONFIG['hdfs']['label_file'])

    traces = {}
    with open(trace_path) as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if len(row) >= 3:
                tid, ts, event = row[:3]
                traces.setdefault(tid, []).append({'ts': ts, 'event': event})

    labels = {}
    with open(label_path) as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if len(row) >= 2:
                tid, label = row[:2]
                labels[tid] = label

    for tid, events in list(traces.items())[:100]:
        for evt in events:
            msg = {
                'trace_id': tid,
                'ts': evt['ts'],
                'event': evt['event'],
                'template': "",         
                'level': "UNKNOWN",
                'component': "hdfs",
                'label': labels.get(tid, '0')
            }
            producer.send(LOG_CONFIG['hdfs']['topic'], msg)
            print(f"📤 HDFS gönderildi: {msg}")
            time.sleep(0.01)

def send_generic_csv(producer, log_key):
    config = LOG_CONFIG[log_key]
    path = os.path.join(BASE_PATH, config['file'])

    with open(path) as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            msg = {
                'trace_id': f"{log_key}_{i}",
                'ts': row.get('Time', '') or row.get('Timestamp', ''),
                'event': row.get('EventId', ''),
                'template': row.get('EventTemplate', ''),
                'level': row.get('Level', 'UNKNOWN'),
                'component': row.get('Component', 'unknown'),
                'label': '0'
            }
            producer.send(config['topic'], msg)
            print(f"📤 {log_key.upper()} gönderildi: {msg}")
            time.sleep(0.0001)

if __name__ == "__main__":
    producer = connect_producer()
    send_hdfs(producer)
    for key in ['spark', 'bgl', 'openstack', 'android', 'zookeeper']:
        send_generic_csv(producer, key)
    print("Tüm loglar Kafka'ya gönderildi.")
