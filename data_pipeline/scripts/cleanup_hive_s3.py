import os
import boto3
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# ✅ 환경 변수 로드
env_path = "C:/project/sportsdrink-pipeline-spark-airflow/data_pipeline/docker/.env"
load_dotenv(env_path)
env = os.getenv("ENV", "dev")

# ✅ Spark 세션 생성 (Hive 메타스토어 연결용)
spark = (
    SparkSession.builder
    .appName("Drop Hive + S3 Tables")
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

# ✅ 삭제할 네임스페이스 및 테이블 정의
namespace = "sportsdrink_youtube_search_daily_bronze"
tables = ["channel_info", "video_info", "comments_info"]

# ✅ Hive 테이블 먼저 삭제
for table in tables:
    full_table_name = f"hadoop_hms.{namespace}.{table}"
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        print(f"🧹 Hive 테이블 삭제 완료: {full_table_name}")
    except Exception as e:
        print(f"❌ Hive 테이블 삭제 실패: {full_table_name} | {str(e)}")

# ✅ 이후 S3 경로 삭제
try:
    s3 = boto3.resource("s3")
    bucket_name = "deokjin-test-datalake"
    prefix = f"data/{env}/{namespace}.db/"

    bucket = s3.Bucket(bucket_name)
    deleted = bucket.objects.filter(Prefix=prefix).delete()
    print(f"🧹 S3 경로 삭제 완료: s3://{bucket_name}/{prefix}")
except Exception as e:
    print(f"❌ S3 삭제 실패: {str(e)}")

# ✅ Spark 세션 종료
spark.stop()
