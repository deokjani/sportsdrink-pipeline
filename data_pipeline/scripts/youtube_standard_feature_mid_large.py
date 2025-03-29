import pandas as pd
import os

# ✅ 점유율 예측에 최적화된 스탠다드 피처 (중대형 유튜브 채널 기준)
standard_features_mid_large = {
    'view_count': 50000,                # 평균 단일 콘텐츠 조회수
    'like_count_x': 2000,               # 평균 단일 콘텐츠 좋아요 수
    'comment_count': 150,               # 평균 댓글 수
    'like_to_view_ratio': 0.04,         # 좋아요 비율 (4%)
    'comment_to_view_ratio': 0.003,     # 댓글 비율 (0.3%)
    'engagement_score': 0.05,           # 종합 참여 점수
    'subscriber_count': 300000,         # 채널 구독자 수 (30만)
    'total_views': 30000000,            # 채널 전체 누적 조회수 (3천만)
    'video_count': 800,                 # 총 업로드 영상 수
    'is_big_channel': 1,                # 대형 채널 여부 (1: 대형)
    'channel_tier': 'gold'              # 채널 티어 (gold 등급)
}

# DataFrame 생성
standard_df = pd.DataFrame([standard_features_mid_large])

# ✅ 저장 경로
save_dir = r"C:\project\sportsdrink-pipeline-spark-airflow\data_pipeline\data\features\sportsdrink_youtube_search_daily"
save_path = os.path.join(save_dir, "standard_feature_mid_large.csv")

# 경로 없으면 생성
os.makedirs(save_dir, exist_ok=True)

# CSV 저장
standard_df.to_csv(save_path, index=False)

print(f"✅ 정제된 표준 스탠다드 피처 CSV 저장 완료: {save_path}")
print(standard_df)
