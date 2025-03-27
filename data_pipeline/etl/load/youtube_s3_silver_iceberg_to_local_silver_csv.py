import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# ✅ 환경 변수 로드
load_dotenv(r"C:/ITWILL/SportsDrinkForecast/docker-elk/.env")

# ✅ Spark 세션 생성
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

# 조회
df = spark.sql("""
    SELECT *
    FROM hadoop_hms.sportsdrink_youtube_search_daily_silver.video_feature
    LIMIT 1000
""")

# CSV로 저장 (로컬)
df.show(truncate=False)
df.coalesce(1).write.mode("overwrite").option("header", "true").csv("C:/ITWILL/SportsDrinkForecast/data_pipeline/data/processed/sportsdrink_youtube_search_daily_silver/video_feature_csv")
