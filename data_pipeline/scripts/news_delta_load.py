# Delta Lake에 저장된 뉴스 데이터를 조회하는 Spark 코드

from pyspark.sql import SparkSession
from datetime import datetime

# 경로 설정 (날짜별 디렉토리)
DELTA_RAW_DIR = "C:/ITWILL/SportsDrinkForecast/data_pipeline/data/raw/news_delta"
today_date = datetime.now().strftime('%Y%m%d')
DELTA_RAW_SAVE_DIR = f"{DELTA_RAW_DIR}/News_RSS_Daily_{today_date}"

# Spark 세션 생성
spark = (SparkSession.builder.appName("ReadNewsFromDelta")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
         .getOrCreate())

# 로그 최소화
spark.sparkContext.setLogLevel("WARN")

# Delta 데이터 읽기
df = spark.read.format("delta").load(DELTA_RAW_SAVE_DIR)

# 상위 10개 출력
print(f"📦 Delta 저장소에서 로드된 뉴스 데이터 (경로: {DELTA_RAW_SAVE_DIR}):")
df.show(10, truncate=False)

# 필요 시 필터링 예시
# df.filter(df["source"].contains("chosun")).show()

# 종료
spark.stop()
