from kafka import KafkaProducer

try:
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    print("Kafka'ya bağlandı.")
except Exception as e:
    print("Kafka bağlantı hatası:", e)
