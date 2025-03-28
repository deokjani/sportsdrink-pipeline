from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os
import requests

# ✅ Slack Webhook URL
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# ✅ Slack 알림 함수
def send_slack_alert(context):
    if not SLACK_WEBHOOK_URL:
        print("❌ SLACK_WEBHOOK_URL 환경 변수가 없습니다.")
        return

    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")
    log_url = task_instance.log_url
    status = context.get("state")
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
        print(f"❌ Slack 알림 실패: {response.text}")


# ✅ DAG 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 7),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_slack_alert,
}

dag = DAG(
    dag_id="naver_sports_drink_etl_daily_dag",
    default_args=default_args,
    description="ETL: Naver → PostgreSQL → Parquet → S3",
    schedule_interval="0 8 * * *",
    catchup=False,
    max_active_runs=1,
)


def run_script(script_name, **kwargs):
    script_path = f"/opt/airflow/data_pipeline/scripts/{script_name}"
    try:
        result = subprocess.run(["python", script_path], check=True, text=True, capture_output=True)
        print(f"✅ {script_name} STDOUT:\n{result.stdout}")
        if result.stderr:
            print(f"⚠️ STDERR:\n{result.stderr}")

        send_slack_alert({
            "task_instance": kwargs.get("task_instance"),
            "dag": kwargs.get("dag"),
            "execution_date": kwargs.get("execution_date"),
            "state": "success",
        })
    except subprocess.CalledProcessError as e:
        print(f"❌ {script_name} 실패: {e}")
        send_slack_alert({
            "task_instance": kwargs.get("task_instance"),
            "dag": kwargs.get("dag"),
            "execution_date": kwargs.get("execution_date"),
            "state": "failed",
        })
        raise

task_naver_to_pg = PythonOperator(
    task_id="naver_sports_drink_to_postgresql",
    python_callable=run_script,
    op_kwargs={"script_name": "naver_sports_drink_to_postgresql.py"},
    dag=dag,
)

task_pg_to_parquet = PythonOperator(
    task_id="naver_sports_drink_postgresql_to_parquet",
    python_callable=run_script,
    op_kwargs={"script_name": "naver_sports_drink_postgresql_to_parquet.py"},
    dag=dag,
)

task_upload_to_s3 = PythonOperator(
    task_id="naver_sports_drink_upload_parquet_to_s3",
    python_callable=run_script,
    op_kwargs={"script_name": "naver_sports_drink_upload_parquet_to_s3.py"},
    dag=dag,
)

task_naver_to_pg >> task_pg_to_parquet >> task_upload_to_s3
