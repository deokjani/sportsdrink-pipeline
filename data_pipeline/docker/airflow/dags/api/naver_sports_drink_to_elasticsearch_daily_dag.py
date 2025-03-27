from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
import subprocess
import os
import logging
import requests  # ✅ Slack 메시지 전송을 위한 requests 라이브러리 추가

# ✅ 로그 설정
log = logging.getLogger(__name__)

# ✅ 환경 변수에서 BASE_PATH 및 SLACK_WEBHOOK_URL 가져오기
BASE_PATH = os.getenv("BASE_PATH", "/opt/airflow/data_pipeline")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# ✅ DAG 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


# ✅ Slack 메시지 전송 함수
def send_slack_message(message):
    if not SLACK_WEBHOOK_URL:
        log.error("🚨 SLACK_WEBHOOK_URL이 설정되지 않았습니다.")
        return

    payload = {"text": message}
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        if response.status_code != 200:
            log.error(f"⚠️ Slack 메시지 전송 실패: {response.status_code}, {response.text}")
    except Exception as e:
        log.error(f"🚨 Slack 메시지 전송 중 오류 발생: {e}")


# ✅ DAG 정의
dag = DAG(
    "naver_sports_drink_to_elasticsearch_daily_dag",
    default_args=default_args,
    description="네이버 검색 API 데이터 수집 및 Elasticsearch 적재",
    schedule_interval="0 8 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
)


# ✅ PythonOperator로 ETL 실행 및 Slack 알림 추가
def run_script():
    script_path = os.path.join(BASE_PATH, "etl/extract/naver_sports_drink_to_elasticsearch.py")
    log.info(f"🚀 실행 시작: {script_path}")
    send_slack_message(f"🚀 DAG 실행 시작: `{script_path}`")

    try:
        result = subprocess.run(
            ["python", script_path],
            capture_output=True, text=True, check=True
        )

        log.info(f"✅ 실행 성공: {script_path}")
        log.info(f"📜 STDOUT:\n{result.stdout}")
        if result.stderr:
            log.error(f"⚠️ STDERR:\n{result.stderr}")

        send_slack_message(f"✅ 성공: `{script_path}`\n📜 로그:```{result.stdout}```")
    except subprocess.CalledProcessError as e:
        log.error(f"❌ 실행 실패: {script_path}")
        log.error(f"🚨 오류 메시지: {e}")
        log.error(f"📜 STDOUT:\n{e.stdout}")
        log.error(f"⚠️ STDERR:\n{e.stderr}")

        # ✅ Slack으로 실패 메시지 전송
        send_slack_message(
            f"❌ 실패: `{script_path}`\n🚨 오류 메시지: {e}\n📜 로그:```{e.stderr if e.stderr else e.stdout}```"
        )


run_task = PythonOperator(
    task_id="naver_sports_drink_to_elasticsearch",
    python_callable=run_script,
    dag=dag,
)

run_task
