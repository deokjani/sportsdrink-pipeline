FROM bitnami/kafka:3.5.1

# telnet 설치 (테스트 및 네트워크 확인을 위해)
USER root
RUN apt update && apt install -y telnet && rm -rf /var/lib/apt/lists/*

# Kafka 실행
CMD ["/opt/bitnami/scripts/kafka/run.sh"]
