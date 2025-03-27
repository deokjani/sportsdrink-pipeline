from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os
import requests  # ✅ Slack 메시지 전송을 위한 requests 모듈

# ✅ 환경 변수 설정
BASE_PATH = os.getenv("BASE_PATH", "/opt/airflow/data_pipeline")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# ✅ Slack 알림 함수
def send_slack_alert(context):
    """Slack으로 DAG 실패 또는 성공 알림을 보내는 함수"""
    if not SLACK_WEBHOOK_URL:
        print("❌ SLACK_WEBHOOK_URL 환경 변수가 설정되지 않았습니다.")
        return

    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")
    log_url = task_instance.log_url

    status = context.get("state")  # 'success' 또는 'failed'
    emoji = ":white_check_mark:" if status == "success" else ":x:"
    color = "good" if status == "success" else "danger"

    message = {
        "attachments": [
            {
                "color": color,
                "title": f"{emoji} Airflow DAG Alert",
                "fields": [
                    {"title": "DAG ID", "value": dag_id, "short": True},
                    {"title": "Task ID", "value": task_id, "short": True},
                    {"title": "Execution Date", "value": str(execution_date), "short": False},
                    {"title": "Status", "value": status.upper(), "short": True},
                    {"title": "Logs", "value": f"<{log_url}|View Logs>", "short": False},
                ],
            }
        ]
    }

    response = requests.post(SLACK_WEBHOOK_URL, json=message)

    if response.status_code == 200:
        print("✅ Slack 알림 전송 성공")
    else:
        print(f"❌ Slack 알림 전송 실패: {response.text}")

# ✅ DAG 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 7),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_slack_alert,  # DAG 실패 시 Slack 알림
}

# ✅ DAG 정의
dag = DAG(
    "naver_sports_drink_etl_daily_dag",
    default_args=default_args,
    description="네이버 검색 API 데이터 수집 → Parquet 처리 → S3 저장",
    schedule_interval="0 8 * * *",  # 매일 아침 8시 실행
    catchup=False,
    max_active_runs=1,
)

# ✅ Python 파일 실행 함수
def run_script(script_folder, script_name, **kwargs):
    """Python 스크립트를 실행하는 함수"""
    script_path = os.path.join(BASE_PATH, script_folder, script_name)
    print(f"🚀 실행 시작: {script_path}")

    try:
        result = subprocess.run(["python", script_path], check=True, text=True, capture_output=True)
        print(f"✅ {script_name} 실행 결과:\n{result.stdout}")

        if result.stderr:
            print(f"⚠️ {script_name} 실행 중 오류:\n{result.stderr}")

        # ✅ 성공 시 Slack 메시지 전송
        send_slack_alert({
            "task_instance": kwargs.get("task_instance"),
            "dag": kwargs.get("dag"),
            "execution_date": kwargs.get("execution_date"),
            "state": "success",
        })
    except subprocess.CalledProcessError as e:
        print(f"❌ 스크립트 실행 실패: {script_name}\n{e.stderr}")

        # ✅ 실패 시 Slack 메시지 전송
        send_slack_alert({
            "task_instance": kwargs.get("task_instance"),
            "dag": kwargs.get("dag"),
            "execution_date": kwargs.get("execution_date"),
            "state": "failed",
        })
        raise

# ✅ Task 1: 네이버 검색 API 데이터 수집 (PostgreSQL 저장)
task_naver_sports_drink_to_postgresql = PythonOperator(
    task_id="naver_sports_drink_to_postgresql",
    python_callable=run_script,
    op_kwargs={"script_folder": "etl/extract", "script_name": "naver_sports_drink_to_postgresql.py"},
    dag=dag,
    on_success_callback=send_slack_alert,  # 성공 시 Slack 알림
    on_failure_callback=send_slack_alert,  # 실패 시 Slack 알림
)

# ✅ Task 2: PostgreSQL 데이터 Parquet로 변환
task_naver_sports_drink_postgresql_to_parquet = PythonOperator(
    task_id="naver_sports_drink_postgresql_to_parquet",
    python_callable=run_script,
    op_kwargs={"script_folder": "etl/transform", "script_name": "naver_sports_drink_postgresql_to_parquet.py"},
    dag=dag,
    on_success_callback=send_slack_alert,  # 성공 시 Slack 알림
    on_failure_callback=send_slack_alert,  # 실패 시 Slack 알림
)

# ✅ Task 3: Parquet 변환 및 S3 업로드
task_naver_sports_drink_upload_parquet_to_s3 = PythonOperator(
    task_id="naver_sports_drink_upload_parquet_to_s3",
    python_callable=run_script,
    op_kwargs={"script_folder": "etl/load", "script_name": "naver_sports_drink_upload_parquet_to_s3.py"},
    dag=dag,
    on_success_callback=send_slack_alert,  # 성공 시 Slack 알림
    on_failure_callback=send_slack_alert,  # 실패 시 Slack 알림
)

# ✅ 실행 순서 정의
task_naver_sports_drink_to_postgresql >> task_naver_sports_drink_postgresql_to_parquet >> task_naver_sports_drink_upload_parquet_to_s3
