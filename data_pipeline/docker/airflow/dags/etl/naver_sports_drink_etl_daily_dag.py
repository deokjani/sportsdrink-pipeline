from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
import subprocess
import os
import requests
from dotenv import load_dotenv

# ✅ Slack 알림 함수 (에러 시만 호출)
def send_slack_alert(context):
    slack_url = os.getenv("SLACK_WEBHOOK_URL")
    if not slack_url:
        print("❌ SLACK_WEBHOOK_URL 미설정")
        return

    ti = context.get("task_instance")
    message = {
        "attachments": [
            {
                "color": "danger",
                "title": "Airflow DAG 실패 알림 🚨",
                "fields": [
                    {"title": "DAG", "value": ti.dag_id, "short": True},
                    {"title": "Task", "value": ti.task_id, "short": True},
                    {"title": "Execution Time", "value": str(context.get('execution_date')), "short": False},
                    {"title": "Log", "value": f"<{ti.log_url}|로그 확인하기>", "short": False},
                ],
            }
        ]
    }
    requests.post(slack_url, json=message)

# ✅ 실행할 Python 스크립트를 호출하는 함수
def run_etl_script(script_name, **kwargs):
    script_path = f"/opt/airflow/scripts/{script_name}"

    # ✅ Airflow 내에서는 docker-compose에서 .env가 주입되지만
    # subprocess에서는 load_dotenv로 강제 로드 필요할 수 있음
    load_dotenv(dotenv_path="/opt/airflow/.env")

    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    print("✅ STDOUT:\n", result.stdout)
    if result.stderr:
        print("⚠️ STDERR:\n", result.stderr)

    if result.returncode != 0:
        raise Exception(f"❌ {script_name} 실패: {result.stderr}")

# ✅ 기본 DAG 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 7),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_slack_alert,

}

with DAG(
    dag_id="naver_sports_drink_etl_daily_dag",
    default_args=default_args,
    schedule_interval="0 8 * * *",  # 매일 오전 8시
    catchup=False,  # 과거 실행 방지
    description="ETL: Naver → PostgreSQL → Parquet → S3",
    tags=["sports_drink", "naver"],
) as dag:

    task_naver_to_pg = PythonOperator(
        task_id="naver_sports_drink_to_postgresql",
        python_callable=run_etl_script,
        op_kwargs={"script_name": "naver_sports_drink_to_postgresql.py"},
    )

    task_pg_to_parquet = PythonOperator(
        task_id="naver_sports_drink_postgresql_to_parquet",
        python_callable=run_etl_script,
        op_kwargs={"script_name": "naver_sports_drink_postgresql_to_parquet.py"},
    )

    task_upload_to_s3 = PythonOperator(
        task_id="naver_sports_drink_upload_parquet_to_s3",
        python_callable=run_etl_script,
        op_kwargs={"script_name": "naver_sports_drink_upload_parquet_to_s3.py"},
    )

    task_naver_to_pg >> task_pg_to_parquet >> task_upload_to_s3
