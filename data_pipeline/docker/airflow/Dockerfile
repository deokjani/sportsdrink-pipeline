FROM apache/airflow:2.7.3

# ✅ root 권한으로 실행
USER root

# ✅ AWS CLI 설치
RUN apt update && apt install -y awscli

# ✅ 디렉토리 생성 및 권한 설정
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/config /opt/airflow/scripts && \
    chmod -R 777 /opt/airflow/dags /opt/airflow/logs /opt/airflow/config /opt/airflow/scripts && \
    chown -R airflow /opt/airflow  # ✅ 그룹을 명시하지 않고 사용자만 변경

# ✅ airflow 사용자로 변경
USER airflow
