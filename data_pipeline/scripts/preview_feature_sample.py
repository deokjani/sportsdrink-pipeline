import os
from pyspark.sql import SparkSession

# ✅ 1. Spark 세션 생성
spark = (
    SparkSession.builder
    .appName("Preview Feature Sample")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ✅ 2. 저장된 Parquet 경로
PARQUET_PATH = r"C:/project/sportsdrink-pipeline-spark-airflow/data_pipeline/data/features/sportsdrink_youtube_search_daily/feature_date=2025-03-30"
SAVE_CSV_PATH = r"C:/project/sportsdrink-pipeline-spark-airflow/data_pipeline/data/features/sample_feature_preview.csv"

# ✅ 3. Parquet 데이터 불러오기
df = spark.read.parquet(PARQUET_PATH)

# ✅ 4. 상위 5개 row 콘솔에 출력
print("\n🧾 Feature 데이터 미리보기 (상위 5개 행):\n")
df.show(5, truncate=False)

# ✅ 5. 일부 샘플만 CSV로 저장 (최대 100개 행)
# df.limit(100).coalesce(1).write.option("header", True).mode("overwrite").csv(SAVE_CSV_PATH)

# ✅ 6. 완료 메시지
# print(f"\n📁 CSV 샘플 저장 완료! → {SAVE_CSV_PATH}")

spark.stop()
