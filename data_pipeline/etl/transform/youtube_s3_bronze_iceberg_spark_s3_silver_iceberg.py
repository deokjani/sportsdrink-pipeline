import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, when, lit, current_date
from dotenv import load_dotenv

# ✅ 1) 환경 변수 로드
load_dotenv(r"C:/project/sportsdrink-pipeline-spark-airflow/data_pipeline/docker/.env")
env = os.getenv("ENV", "dev")  # 기본값은 dev

# ✅ 2) Spark 세션 생성 (정적 설정)
spark = (
    SparkSession.builder
    .appName("Bronze → Silver (Feature Store) transform pipeline")
    .config("spark.jars.packages",
            ",".join([
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

# ✅ (공통) 네임스페이스 생성 함수
def create_namespace_if_not_exists(namespace):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS hadoop_hms.{namespace}")
    print(f"✅ 네임스페이스 '{namespace}' 생성 완료!")

# ✅ (공통) feature 테이블 저장 함수 (스키마 관리 + feature_date 추가)
def save_feature_table(df, namespace, table_name):
    full_table_name = f"hadoop_hms.{namespace}.{table_name}"
    df = df.withColumn("feature_date", current_date())

    df.writeTo(full_table_name).createOrReplace()
    print(f"✅ {namespace}.{table_name} 저장 완료!")

# ✅ 3) Bronze (1차 정제 데이터) 테이블 로드
bronze_namespace = "sportsdrink_youtube_search_daily_bronze"
channel_info = spark.read.format("iceberg").load(f"hadoop_hms.{bronze_namespace}.channel_info")
video_info = spark.read.format("iceberg").load(f"hadoop_hms.{bronze_namespace}.video_info")
comments_info = spark.read.format("iceberg").load(f"hadoop_hms.{bronze_namespace}.comments_info")

# ✅ 4) Silver 네임스페이스 생성 (한 번만)
silver_namespace = "sportsdrink_youtube_search_daily_silver"
create_namespace_if_not_exists(silver_namespace)

# ✅ 5) comments_feature 생성 및 저장
comments_features = comments_info \
    .withColumn("comment_length", length(col("comment_text"))) \
    .withColumn("is_highlighted",
                when((col("like_count").cast("int") > 50) & (length(col("comment_text")) > 20), lit(1)).otherwise(lit(0))) \
    .withColumn("sentiment_flag",
                when(col("comment_text").rlike("good|great|nice|awesome|👍"), lit(1)).otherwise(lit(0)))
save_feature_table(comments_features, silver_namespace, "comments_feature")

# ✅ 6) channel_feature 생성 및 저장
channel_features = channel_info.withColumn(
    "is_big_channel",
    when(col("subscriber_count").cast("int") > 100000, lit(1)).otherwise(lit(0))
).withColumn(
    "channel_tier",
    when(col("subscriber_count").cast("int") > 1000000, lit("platinum"))
    .when(col("subscriber_count").cast("int") > 100000, lit("gold"))
    .when(col("subscriber_count").cast("int") > 10000, lit("silver"))
    .otherwise(lit("bronze"))
)
save_feature_table(channel_features, silver_namespace, "channel_feature")

# ✅ 7) video_feature 생성 및 저장
video_features = video_info \
    .withColumn("like_to_view_ratio", (col("like_count").cast("float") / (col("view_count").cast("float") + 1))) \
    .withColumn("comment_to_view_ratio", (col("comment_count").cast("float") / (col("view_count").cast("float") + 1))) \
    .withColumn("engagement_score", (
        (col("like_count").cast("float") * 2 + col("comment_count").cast("float")) / (
            col("view_count").cast("float") + 1)
))
save_feature_table(video_features, silver_namespace, "video_feature")

# ✅ 완료 로그 출력
print("\n🎉 Bronze → Silver (Feature Store) 파이프라인 완료!")

spark.stop()
