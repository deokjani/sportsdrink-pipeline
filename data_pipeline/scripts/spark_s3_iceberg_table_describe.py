import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# ✅ 환경 변수 로드
load_dotenv(r"/data_pipeline/docker/.env")
env = os.getenv("ENV", "dev")  # 기본값은 dev

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
    .config("spark.sql.catalog.hadoop_hms.warehouse", f"s3a://deokjin-test-datalake/data/{env}/")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
    .getOrCreate()
)

# Spark 세션은 이미 열려있다고 가정하고
query = """
DESCRIBE hadoop_hms.sportsdrink_youtube_search_daily_bronze.channel_info
"""

result_df = spark.sql(query)
result_df.show(truncate=False)
