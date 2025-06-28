import time
import json
import threading
import psycopg2
from kafka import KafkaConsumer
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
from app.templates_loader import get_event_info
from app.model import detect_anomaly_from_log, get_recommendation

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "loganalyzer")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pass")

def start_consumer(topic):
    while True:
        try:
            print(f"Kafka'ya bağlanıyor... ({topic})")
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers='kafka:9092',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=f'{topic}-consumer-group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print(f"Kafka bağlantısı kuruldu. ({topic})")

            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            cur = conn.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS anomalies (
                    id SERIAL PRIMARY KEY,
                    log_type TEXT,
                    trace_id TEXT,
                    ts TEXT,
                    event TEXT,
                    score REAL,
                    rec TEXT,
                    reason TEXT,
                    type TEXT
                );
            """)
            conn.commit()

            for msg in consumer:
                try:
                    rec = msg.value
                    event_raw = rec.get('event', '').strip()
                    if not event_raw:
                        continue

                    info = get_event_info(topic.split('-')[0], event_raw)
                    is_anom, score = detect_anomaly_from_log({
                        "EventTemplate": rec.get("template", ""),
                        "timestamp": rec.get("ts", ""),
                        "log_level": rec.get("level", ""),
                        "component": rec.get("component", "")
                    })
                    recommendation = get_recommendation(score)

                    if info['type'].lower() != "normal" or is_anom:
                        cur.execute(
                            """INSERT INTO anomalies(log_type, trace_id, ts, event, score, rec, reason, type)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                            (topic, rec['trace_id'], rec['ts'], event_raw, float(score), recommendation, info['reason'], info['type'])
                        )
                        conn.commit()
                        print(f"⚠️ Anomali tespit edildi: {rec['trace_id']} / {event_raw} ({topic})")
                except Exception as e:
                    print(f"⚠️ Mesaj işlenemedi: {e}")
                    conn.rollback()

        except Exception as e:
            print(f"Consumer bağlantı hatası ({topic}): {e}")
            time.sleep(5)

TOPICS = [
    "hdfs-traces", "spark-traces", "bgl-traces",
    "android-traces", "zookeeper-traces", "openstack-traces"
]

for topic in TOPICS:
    threading.Thread(target=start_consumer, args=(topic,), daemon=True).start()

@app.get("/anomalies/{log_type}")
def get_anomalies_by_type(log_type: str):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()
        cur.execute(
            "SELECT log_type, trace_id, ts, event, score, rec, reason, type FROM anomalies WHERE log_type = %s ORDER BY id DESC LIMIT 100",
            (log_type,)
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [
            {
                "log_type": r[0],
                "trace_id": r[1],
                "ts": r[2],
                "event": r[3],
                "score": r[4],
                "rec": r[5],
                "reason": r[6],
                "type": r[7]
            } for r in rows
        ]
    except Exception as e:
        return {"error": str(e)}
