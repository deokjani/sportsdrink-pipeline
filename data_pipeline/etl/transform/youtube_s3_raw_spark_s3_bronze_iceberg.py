import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date, expr, length, concat_ws
from dotenv import load_dotenv
from datetime import datetime

# ✅ 1) 환경 변수 로드
load_dotenv(r"C:/ITWILL/SportsDrinkForecast/docker-elk/.env")

# ✅ 2) Spark 세션 생성 (Iceberg + S3 연동)
spark = (
    SparkSession.builder
    .appName("YouTube S3 Raw Transform to Iceberg")
    .config("spark.jars.packages",
            ",".join([
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3"
            ]))
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.hadoop_hms", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hadoop_hms.catalog-impl", "org.apache.iceberg.hive.HiveCatalog")
    .config("spark.sql.catalog.hadoop_hms.uri", "thrift://localhost:9083")
    .config("spark.sql.catalog.hadoop_hms.warehouse", "s3a://deokjin-test-datalake/data/")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ✅ 3) 네임스페이스 생성 함수
def create_namespace_if_not_exists(namespace):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS hadoop_hms.{namespace}")
    print(f"✅ 네임스페이스 '{namespace}' 생성 완료!")

# ✅ 4) 날짜 및 서비스명 세팅
target_date = "20250322"
service_folder = "sportsdrink_youtube_search_daily"

# ✅ 5) S3 원본 데이터 경로 세팅
base_path = f"s3a://deokjin-test-datalake/data/raw/{target_date}/{service_folder}"
channel_info_path = f"{base_path}/channel_info/data.parquet"
video_info_path = f"{base_path}/video_info/data.parquet"
comments_info_path = f"{base_path}/comments/data.parquet"

# ✅ 6) 원본 데이터 로드
channel_info_df = spark.read.parquet(channel_info_path)
video_info_df = spark.read.parquet(video_info_path)
comments_info_df = spark.read.parquet(comments_info_path)

# ✅ 7) 공통 클린업 함수
def clean_and_add_date_column(df):
    for c, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(c, trim(col(c)))
        elif dtype.startswith("array"):
            df = df.withColumn(c, concat_ws(",", col(c)))
    df = df.withColumn("crawled_date", expr(f"'{datetime.today().strftime('%Y-%m-%d')}'"))
    return df

# ✅ 8) 날짜 파티션 컬럼 변환
def add_partition_columns(df):
    return df.withColumn("crawled_date", to_date(col("crawled_date")))

# ✅ 9) 이상치 제거 함수
def remove_outliers(df, conditions_dict):
    for column_name, condition in conditions_dict.items():
        df = df.filter(expr(f"{column_name} {condition}"))
    return df

# ✅ 10) 짧은 텍스트 제거 함수
def remove_short_text(df, column_name, min_length=3):
    return df.filter(length(col(column_name)) >= min_length)

# ✅ 11) Iceberg 테이블 저장 함수 (네임스페이스 생성은 외부에서 한 번만!)
def save_to_iceberg_table(df, namespace, table_name):
    df.writeTo(f"hadoop_hms.{namespace}.{table_name}") \
      .using("iceberg") \
      .tableProperty("format-version", "2") \
      .createOrReplace()

    print(f"✅ Iceberg 테이블 '{namespace}.{table_name}' 저장 완료!")

# ✅ 12) 데이터 클린 및 변환
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

# ✅ 네임스페이스 생성 (딱 한 번만!)
namespace = f"{service_folder}_bronze"
create_namespace_if_not_exists(namespace)

# ✅ 테이블 저장
save_to_iceberg_table(channel_info_clean, namespace, "channel_info")
save_to_iceberg_table(video_info_clean, namespace, "video_info")
save_to_iceberg_table(comments_info_clean, namespace, "comments_info")

print("\n🎉 모든 Iceberg 테이블 저장 완료!")

# ✅ 13) Spark 세션 종료
spark.stop()
