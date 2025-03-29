import os
from datetime import datetime
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pyspark.sql.functions import lit

# ✅ 1. 환경 변수 로드
load_dotenv(r"C:/project/sportsdrink-pipeline-spark-airflow/data_pipeline/docker/.env")
env = os.getenv("ENV", "dev")

# ✅ 2. Spark 세션 생성 + Adaptive Execution 설정
spark = (
    SparkSession.builder
    .appName("YouTube Feature Join (Video + Channel Only)")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ✅ 3. 경로 설정
LOAD_PATH = f"C:/project/sportsdrink-pipeline-spark-airflow/data_pipeline/data/processed/sportsdrink_youtube_search_daily_silver"
SAVE_BASE_PATH = f"C:/project/sportsdrink-pipeline-spark-airflow/data_pipeline/data/features/sportsdrink_youtube_search_daily"

# 오늘 날짜 → feature_date
feature_date = datetime.today().strftime("%Y-%m-%d")
save_path = os.path.join(SAVE_BASE_PATH)

# ✅ 4. CSV 데이터 불러오기
video_df = spark.read.option("header", True).csv(os.path.join(LOAD_PATH, "video_feature.csv"))
channel_df = spark.read.option("header", True).csv(os.path.join(LOAD_PATH, "channel_feature.csv"))

# ✅ 5. 컬럼명 변경 (충돌 방지용 prefix)
rename_map_channel = {
    "channel_title": "channel_title_channel",
    "crawled_date": "crawled_date_channel",
    "feature_date": "feature_date_channel",
}
rename_map_video = {
    "channel_title": "channel_title_video",
    "crawled_date": "crawled_date_video",
    "feature_date": "feature_date_video",
    "like_count": "like_count_video"
}

for old, new in rename_map_channel.items():
    if old in channel_df.columns:
        channel_df = channel_df.withColumnRenamed(old, new)

for old, new in rename_map_video.items():
    if old in video_df.columns:
        video_df = video_df.withColumnRenamed(old, new)

# ✅ 6. 비디오 + 채널 조인
full_feature_df = video_df.join(channel_df, on="channel_id", how="left")

# ✅ 7. 최종 Feature 컬럼 정의
selected_columns = [
    "view_count",                # 영상 조회수
    "like_count_video",          # 영상 좋아요 수
    "like_to_view_ratio",        # 조회수 대비 좋아요 비율
    "comment_to_view_ratio",     # 조회수 대비 댓글 비율
    "engagement_score",          # 종합 참여도 지표
    "subscriber_count",          # 채널 구독자 수
    "total_views",               # 채널 누적 조회수
    "video_count",               # 채널 업로드 수
    "is_big_channel",            # 대형 채널 여부
    "channel_tier",              # 채널 등급
]
existing_columns = [col for col in selected_columns if col in full_feature_df.columns]
feature_df = full_feature_df.select(*existing_columns)

# ✅ 8. feature_date 파티션 컬럼 추가
feature_df = feature_df.withColumn("feature_date", lit(feature_date))

# ✅ 9. 저장 (Parquet, 파티셔닝)
feature_df.write \
    .partitionBy("feature_date") \
    .mode("append") \
    .parquet(save_path)

print("\n✅ 학습용 Feature 데이터 저장 완료 (댓글 제외)")
print(f"📁 저장 위치: {save_path}/feature_date={feature_date}/")
spark.stop()
