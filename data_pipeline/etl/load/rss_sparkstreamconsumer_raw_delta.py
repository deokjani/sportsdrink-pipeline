# Spark Streaming Consumer: Kafka(news_rss_topic) → Delta (Raw News Data)

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime

# 설정
SPARK_TMP_DIR = "C:/ITWILL/SportsDrinkForecast/data_pipeline/sparktmp"
DELTA_RAW_DIR = "C:/ITWILL/SportsDrinkForecast/data_pipeline/data/raw/news_delta"
KAFKA_BROKER = "localhost:29092"
NEWS_RSS_TOPIC = "news_rss_topic"

# 날짜 기반 저장 디렉토리
today_date = datetime.now().strftime('%Y%m%d')
DELTA_RAW_SAVE_DIR = f"{DELTA_RAW_DIR}/News_RSS_Daily_{today_date}"

# Spark 세션 생성
spark = (SparkSession.builder.appName("NewsRSSKafkaToDelta")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.local.dir", SPARK_TMP_DIR)
         .config("spark.jars.packages",
                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# RSS Kafka 데이터 스키마 정의
rss_schema = StructType([
    StructField("source", StringType(), True),
    StructField("title", StringType(), True),
    StructField("link", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("published", StringType(), True)
])

# foreachBatch 함수: 로그 출력 + Delta 저장
def write_to_delta_with_log(batch_df, batch_id):
    print(f"\n📰 [Batch ID: {batch_id}] 수신된 뉴스 데이터:")
    batch_df.show(truncate=False)

    # Delta 저장
    batch_df.write.format("delta").mode("append").save(DELTA_RAW_SAVE_DIR)

# Kafka → Delta Streaming (with 로그)
query = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_BROKER)
         .option("subscribe", NEWS_RSS_TOPIC)
         .option("startingOffsets", "latest")
         .load()
         .selectExpr("CAST(value AS STRING) AS json_data")
         .select(from_json(col("json_data"), rss_schema).alias("data"))
         .select("data.*")
         .writeStream
         .outputMode("append")
         .option("checkpointLocation", f"{SPARK_TMP_DIR}/rss_news_checkpoint")
         .foreachBatch(write_to_delta_with_log)
         .start())

print("📡 Spark Streaming 시작됨: Kafka(news_rss_topic) → Delta 저장 중...\n")
query.awaitTermination()
