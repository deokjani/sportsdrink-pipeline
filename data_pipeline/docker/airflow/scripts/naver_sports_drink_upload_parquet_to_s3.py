import boto3
import os
from datetime import datetime

# ✅ AWS S3 설정
S3_BUCKET = "deokjin-test-datalake"
S3_REGION = "ap-northeast-2"
S3_KEY_PREFIX = "data/processed/"

# ✅ Parquet 저장 경로 
save_dir = "/opt/airflow/data/processed"
os.makedirs(save_dir, exist_ok=True)

# 파일명 구성 요소
service_name = "SportsDrink"  # 서비스명
data_source = "Naver" # 데이터 출처 (ex: twitter, naver, youtube, instagram 등)
data_type = "Search"  # 데이터 성격 (ex: search, sales, trend, log 등)
batch_cycle = "Daily"  # 배치 주기 (ex: hourly, daily, weekly, monthly)

# 파일명 포맷
file_name = f"{service_name}_{data_source}_{data_type}_{batch_cycle}_{datetime.now().strftime('%Y%m%d')}.parquet"
parquet_file_path = os.path.join(save_dir, file_name)


# ✅ Boto3 세션을 사용하여 AWS 인증 명확히 설정
session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=S3_REGION,
)

# ✅ S3 클라이언트 생성
s3 = session.client("s3")

def upload_to_s3(file_path):
    """Parquet 파일을 S3에 업로드"""
    file_name = os.path.basename(file_path)
    s3_key = f"{S3_KEY_PREFIX}{file_name}"

    try:
        s3.upload_file(file_path, S3_BUCKET, s3_key)
        print(f"✅ S3 업로드 완료: s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"❌ S3 업로드 실패: {e}")

# ✅ 실행
if __name__ == "__main__":
    if os.path.exists(parquet_file_path):
        upload_to_s3(parquet_file_path)
    else:
        print(f"⚠️ 업로드할 파일이 존재하지 않습니다: {parquet_file_path}")
