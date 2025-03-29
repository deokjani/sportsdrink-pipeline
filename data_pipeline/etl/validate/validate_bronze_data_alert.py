import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws
from dotenv import load_dotenv

# ✅ 1. 환경 변수 로드
env_path = "C:\\project\\sportsdrink-pipeline-spark-airflow\\data_pipeline\\docker\\.env"
load_dotenv(env_path)

env = os.getenv("ENV", "dev")

# ✅ 2. Spark 세션 생성
spark = (
    SparkSession.builder
    .appName("Bronze Iceberg Data Quality Validation")
    .config("spark.jars.packages", ",".join([
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3"
    ]))
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.hadoop_hms", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hadoop_hms.catalog-impl", "org.apache.iceberg.hive.HiveCatalog")
    .config("spark.sql.catalog.hadoop_hms.uri", "thrift://localhost:9083")
    .config("spark.sql.catalog.hadoop_hms.warehouse", f"s3a://deokjin-test-datalake/data/{env}/")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ✅ Slack 알림 함수
def send_quality_summary_to_slack(table_name, null_issues, dup_issues, row_issue):
    slack_url = os.getenv("SLACK_WEBHOOK_URL")
    if not slack_url:
        print("❌ SLACK_WEBHOOK_URL 환경변수 없음")
        return

    status = "✅ PASS" if not (null_issues or dup_issues or row_issue) else "❌ FAIL"

    message = {
        "attachments": [
            {
                "color": "good" if status == "✅ PASS" else "danger",
                "title": f"📊 데이터 품질 검증 결과: `{table_name}` → {status}",
                "fields": [
                    {"title": "Null 이슈", "value": null_issues or "없음", "short": True},
                    {"title": "PK 중복", "value": dup_issues or "없음", "short": True},
                    {"title": "Row 수 기준", "value": row_issue or "충족", "short": False},
                ]
            }
        ]
    }

    response = requests.post(slack_url, json=message)
    if response.status_code != 200:
        print(f"❌ Slack 전송 실패: {response.text}")
    else:
        print("✅ Slack 품질 요약 전송 완료")

# ✅ 검증 함수
def validate_data(df, table_name, pk_col, null_cols, min_rows=1000):
    total = df.count()

    null_issues = ""
    for col_name in null_cols:
        null_count = df.filter(col(col_name).isNull()).count()
        ratio = null_count / total if total > 0 else 0
        if ratio > 0.05:
            null_issues += f"{col_name}: {ratio:.1%} "

    dup_count = df.groupBy(pk_col).count().filter("count > 1").count()
    dup_issues = f"{dup_count}건 중복" if dup_count > 0 else ""

    row_issue = f"{total}건 < {min_rows} 기준" if total < min_rows else ""

    # ✅ Slack 알림 전송
    send_quality_summary_to_slack(table_name, null_issues, dup_issues, row_issue)

# ✅ Iceberg 테이블 불러오기
namespace = "sportsdrink_youtube_search_daily_bronze"
channel_df = spark.read.format("iceberg").load(f"hadoop_hms.{namespace}.channel_info")
video_df = spark.read.format("iceberg").load(f"hadoop_hms.{namespace}.video_info")
comments_df = spark.read.format("iceberg").load(f"hadoop_hms.{namespace}.comments_info")

# ✅ comment_id 생성 (4개 컬럼 조합 기반 고유 ID)
comments_df = comments_df.withColumn(
    "comment_id",
    sha2(concat_ws("||", "video_id", "author", "published_at", "comment_text"), 256)
)

# ✅ 품질 검증 실행
validate_data(channel_df, "channel_info", "channel_id", ["channel_id", "channel_title"])
validate_data(video_df, "video_info", "video_id", ["video_id", "title"])
validate_data(comments_df, "comments_info", "comment_id", ["comment_id", "comment_text"])

print("\n🎉 모든 품질 검증 및 Slack 알림 완료!")
spark.stop()
