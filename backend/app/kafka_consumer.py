import time
import json
import psycopg2
from kafka import KafkaConsumer
from model import detect_anomaly_from_log, get_recommendation
from templates_loader import get_event_info

CONN_INFO = "dbname=loganalyzer user=postgres password=pass host=db"

def start_consumer(topic: str):
    while True:
        try:
            print(f"Kafka'ya bağlanıyor... ({topic})")
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers='kafka:9092',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=f'{topic}-consumer-group',
                api_version=(2, 7, 0)
            )
            print(f"Kafka bağlantısı kuruldu. ({topic})")

            conn = psycopg2.connect(CONN_INFO)
            cur = conn.cursor()

            for msg in consumer:
                try:
                    rec = msg.value
                    trace_id = rec.get('trace_id', 'N/A')
                    ts = rec.get('ts', '')
                    event_raw = rec.get('event', '').strip()

                    if not event_raw:
                        continue

                    is_anom, score = detect_anomaly_from_log({
                        "EventTemplate": rec.get("template", ""),
                        "timestamp": ts,
                        "log_level": rec.get("level", ""),
                        "component": rec.get("component", "")
                    })

                    rec_text = get_recommendation(score)
                    reason, err_type = get_event_info(topic, event_raw)

                    if err_type.lower() != "normal" or is_anom:
                        cur.execute(
                            "INSERT INTO anomalies (log_type, trace_id, ts, event, score, rec, reason, type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                            (topic, trace_id, ts, event_raw, score, rec_text, reason, err_type)
                        )
                        conn.commit()
                        print(f"Anomali kaydedildi: {trace_id} / {event_raw} ({topic})")

                except Exception as inner_e:
                    print(f"⚠️ Mesaj işlenemedi: {inner_e}")
                    conn.rollback()

            consumer.close()
            cur.close()
            conn.close()

        except Exception as e:
            print(f"Consumer bağlantı hatası ({topic}): {e}")
            time.sleep(5)
