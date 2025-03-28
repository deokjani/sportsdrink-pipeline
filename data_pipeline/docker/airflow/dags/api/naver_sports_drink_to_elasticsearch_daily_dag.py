from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import subprocess
import os
import logging
import requests

log = logging.getLogger(__name__)
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "naver_sports_drink_to_elasticsearch_daily_dag",
    default_args=default_args,
    description="네이버 검색 → Elasticsearch 적재",
    schedule_interval="0 9 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
)

def send_slack_message(message):
    if not SLACK_WEBHOOK_URL:
        log.warning("❌ SLACK_WEBHOOK_URL이 설정되지 않았습니다.")
        return
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": message})
    except Exception as e:
        log.error(f"🚨 Slack 전송 실패: {e}")

def run_script():
    script_path = "/opt/airflow/data_pipeline/scripts/naver_sports_drink_to_elasticsearch.py"
    log.info(f"🚀 실행: {script_path}")
    send_slack_message(f"🚀 DAG 실행 시작: `{script_path}`")

    try:
        result = subprocess.run(["python", script_path], capture_output=True, text=True, check=True)
        log.info(f"✅ 실행 성공:\n{result.stdout}")
        send_slack_message(f"✅ 성공: `{script_path}`\n```{result.stdout}```")
    except subprocess.CalledProcessError as e:
        log.error(f"❌ 실행 실패: {e.stderr or e.stdout}")
        send_slack_message(f"❌ 실패: `{script_path}`\n```{e.stderr or e.stdout}```")
        raise

run_task = PythonOperator(
    task_id="load_to_elasticsearch",
    python_callable=run_script,
    dag=dag,
)
