from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "news_rss_topic",
    bootstrap_servers="localhost:29092",
    auto_offset_reset="earliest",
    consumer_timeout_ms=5000  # 5초 동안 메시지를 기다림
)

for msg in consumer:
    print(msg.value.decode("utf-8"))

print("✅ Kafka 연결 테스트 완료")
