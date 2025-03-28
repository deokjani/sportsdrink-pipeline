FROM apache/airflow:2.7.3

# ✅ root 권한으로 실행
USER root

# ✅ AWS CLI 설치
RUN apt update && apt install -y awscli

# ✅ 디렉토리 생성 및 권한 설정
RUN mkdir -p /opt/airflow/logs \
    && mkdir -p /opt/airflow/dags \
    && mkdir -p /opt/airflow/data_pipeline \
    && mkdir -p /opt/airflow/data/scripts \
    && mkdir -p /opt/airflow/config \  
    && chown -R airflow: /opt/airflow \
    && chmod -R 755 /opt/airflow

# ✅ airflow 사용자로 변경
USER airflow
