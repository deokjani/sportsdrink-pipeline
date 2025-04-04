from kafka.admin import KafkaAdminClient, NewTopic

KAFKA_BROKER = "localhost:29092"
KAFKA_TOPIC = "news_rss_topic"

# ✅ Kafka AdminClient 생성
admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)

# ✅ 기존 토픽 삭제
try:
    admin_client.delete_topics([KAFKA_TOPIC])
    print(f"🔥 Kafka 토픽 삭제 완료: {KAFKA_TOPIC}")
except Exception as e:
    print(f"⚠️ 토픽 삭제 중 오류 발생: {e}")

# ✅ AdminClient 종료
admin_client.close()
