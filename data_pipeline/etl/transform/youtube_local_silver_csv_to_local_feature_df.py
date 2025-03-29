import pandas as pd
import os

# ✅ 1. 경로 설정 (실버 CSV 불러오기 경로)
LOAD_PATH = r"C:\project\sportsdrink-pipeline-spark-airflow\data_pipeline\data\processed\sportsdrink_youtube_search_daily_silver"

# ✅ 2. 저장 경로 (최종 학습용 feature CSV 저장 경로)
SAVE_PATH = r"C:\project\sportsdrink-pipeline-spark-airflow\data_pipeline\data\features\sportsdrink_youtube_search_daily"
os.makedirs(SAVE_PATH, exist_ok=True)  # 경로 없으면 자동 생성

# ✅ 3. CSV 파일 불러오기 (비디오 / 채널 / 댓글 feature)
try:
    video_df = pd.read_csv(os.path.join(LOAD_PATH, "video_feature.csv"))
    channel_df = pd.read_csv(os.path.join(LOAD_PATH, "channel_feature.csv"))
    comments_df = pd.read_csv(os.path.join(LOAD_PATH, "comments_feature.csv"))
except FileNotFoundError as e:
    print(f"⚠ 파일 없음: {e.filename}")
    exit(1)

# ✅ 4. 컬럼 네이밍 정리 (조인 시 혼동 방지)
channel_df = channel_df.rename(columns={
    "channel_title": "channel_title_channel",
    "crawled_date": "crawled_date_channel",
    "feature_date": "feature_date_channel"
})
video_df = video_df.rename(columns={
    "channel_title": "channel_title_video",
    "crawled_date": "crawled_date_video",
    "feature_date": "feature_date_video"
})

# ✅ 5. 비디오 + 채널 조인
video_channel_df = pd.merge(video_df, channel_df, on="channel_id", how="left")

# ✅ 6. 비디오+채널 조인 결과와 댓글 데이터 조인
full_feature_df = pd.merge(video_channel_df, comments_df, on="video_id", how="inner")

# ✅ 7. 점유율 예측에 필요한 컬럼 정의
# 👉 점유율(시장 점유율 / 트렌드 예측)을 설명하는 데에 중요한 지표들만 선택
selected_columns = [
    'view_count',              # 영상 조회수 (콘텐츠 인기도)
    'like_count_x',            # 영상 좋아요 수 (콘텐츠에 대한 긍정 반응)
    'comment_count',           # 댓글 수 (사용자 참여도)
    'like_to_view_ratio',      # 조회수 대비 좋아요 비율 (콘텐츠 반응도)
    'comment_to_view_ratio',   # 조회수 대비 댓글 비율 (참여도 지표)
    'engagement_score',        # 종합 참여 지표 (like, comment, view 조합 지수)
    'subscriber_count',        # 채널 구독자 수 (채널 파워)
    'total_views',             # 채널 누적 조회수 (장기적 인지도)
    'video_count',             # 채널 영상 업로드 수 (활동성)
    'is_big_channel',          # 대형 채널 여부 (인지도 영향)
    'channel_tier'             # 채널 티어 (Silver, Gold 등)
]

# ✅ 8. 실제 존재하는 컬럼만 필터링
existing_columns = [col for col in selected_columns if col in full_feature_df.columns]

# ✅ 9. 최종 학습용 Feature DataFrame 생성
training_feature_df = full_feature_df[existing_columns]

# ✅ 10. 미리보기 출력 (상위 5개 샘플)
print("\n🎯 점유율 예측용 최종 학습 feature 데이터 (상위 5개):")
print(training_feature_df.head())

# ✅ 11. CSV 파일로 저장
save_file_path = os.path.join(SAVE_PATH, "training_feature_for_share_forecast.csv")
training_feature_df.to_csv(save_file_path, index=False)
print(f"\n✅ '{save_file_path}' 파일 저장 완료!")

# ✅ 12. 완료 안내
print("\n📊 이 데이터는 향후 LSTM 또는 시계열 모델 학습용으로 바로 사용 가능합니다!")
5