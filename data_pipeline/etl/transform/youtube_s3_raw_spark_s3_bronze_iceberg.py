import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date, expr, length, concat_ws
from dotenv import load_dotenv
from datetime import datetime

# ✅ 1) 환경 변수 로드
load_dotenv(r"C:/project/sportsdrink-pipeline-spark-airflow/data_pipeline/docker/.env")
env = os.getenv("ENV", "dev")  # 기본값은 dev

# ✅ 2) Spark 세션 생성 (Iceberg + S3 연동)
spark = (
    SparkSession.builder
    .appName("YouTube S3 Raw Transform to Iceberg")
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

# ✅ 3) 네임스페이스 생성
def create_namespace_if_not_exists(namespace):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS hadoop_hms.{namespace}")
    print(f"✅ 네임스페이스 '{namespace}' 생성 완료!")

# ✅ 4) 공통 클렌징 함수 (결측치, 중복, 공백, 배열 → 문자열)
def clean_and_add_date_column(df):
    for c, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(c, trim(col(c)))
        elif dtype.startswith("array"):
            df = df.withColumn(c, concat_ws(",", col(c)))
    df = df.dropna().dropDuplicates()
    df = df.withColumn("crawled_date", expr(f"'{datetime.today().strftime('%Y-%m-%d')}'"))
    return df

# ✅ 5) 이상치 제거
def remove_outliers(df, conditions_dict):
    for column_name, condition in conditions_dict.items():
        df = df.filter(expr(f"{column_name} {condition}"))
    return df

# ✅ 6) 짧은 텍스트 제거
def remove_short_text(df, column_name, min_length=3):
    return df.filter(length(col(column_name)) >= min_length)

# ✅ 7) 날짜 파티션 컬럼 변환
def add_partition_columns(df):
    return df.withColumn("crawled_date", to_date(col("crawled_date")))

# ✅ 8) Iceberg 테이블 저장 함수
def save_to_iceberg_table(df, namespace, table_name):
    df.writeTo(f"hadoop_hms.{namespace}.{table_name}") \
      .using("iceberg") \
      .tableProperty("format-version", "2") \
      .createOrReplace()
    print(f"✅ Iceberg 테이블 '{namespace}.{table_name}' 저장 완료!")

# ✅ 9) 대상 날짜 및 서비스명 설정
target_date = "20250322"
service_folder = "sportsdrink_youtube_search_daily"
base_path = f"s3a://deokjin-test-datalake/data/raw/{target_date}/{service_folder}"

# ✅ 10) 데이터 로드
channel_info_df = spark.read.parquet(f"{base_path}/channel_info/data.parquet")
video_info_df = spark.read.parquet(f"{base_path}/video_info/data.parquet")
comments_info_df = spark.read.parquet(f"{base_path}/comments/data.parquet")

# ✅ 11) 데이터 정제
channel_info_clean = add_partition_columns(remove_outliers(clean_and_add_date_column(channel_info_df), {
    "subscriber_count": ">= 0", "total_views": ">= 0", "video_count": ">= 0"
}))

video_info_clean = add_partition_columns(remove_outliers(remove_short_text(
    clean_and_add_date_column(video_info_df), "title", min_length=3), {
    "view_count": ">= 0", "like_count": ">= 0", "comment_count": ">= 0"
}))

comments_info_clean = add_partition_columns(remove_outliers(remove_short_text(
    clean_and_add_date_column(comments_info_df), "comment_text", min_length=3), {
    "like_count": ">= 0"
}))

# ✅ 12) Iceberg 저장
namespace = f"{service_folder}_bronze"
create_namespace_if_not_exists(namespace)

save_to_iceberg_table(channel_info_clean, namespace, "channel_info")
save_to_iceberg_table(video_info_clean, namespace, "video_info")
save_to_iceberg_table(comments_info_clean, namespace, "comments_info")

print("\n🎉 모든 Iceberg 테이블 저장 완료!")
spark.stop()
