import boto3
import os
from datetime import datetime

# ✅ AWS S3 설정
S3_BUCKET = "deokjin-test-datalake"  # 🔹 S3 버킷 이름
S3_REGION = "ap-northeast-2"  # 🔹 AWS 리전
S3_KEY_PREFIX = "data/processed/"  # 🔹 S3 저장 경로 (폴더처럼 사용)

# ✅ Parquet 저장 경로
save_dir = "C:/ITWILL/SportsDrinkForecast/data_pipeline/data/processed"
os.makedirs(save_dir, exist_ok=True)

# 파일명 구성 요소
service_name = "SportsDrink"  # 서비스명
data_source = "Naver" # 데이터 출처 (ex: twitter, naver, youtube, instagram 등)
data_type = "Search"  # 데이터 성격 (ex: search, sales, trend, log 등)
batch_cycle = "Daily"  # 배치 주기 (ex: hourly, daily, weekly, monthly)

# 파일명 포맷
file_name = f"{service_name}_{data_source}_{data_type}_{batch_cycle}_{datetime.now().strftime('%Y%m%d')}.parquet"
parquet_file_path = os.path.join(save_dir, file_name)

# ✅ S3 클라이언트 생성
s3 = boto3.client("s3", region_name=S3_REGION)

def upload_to_s3(file_path):
    """ Parquet 파일을 S3에 업로드 """
    file_name = os.path.basename(file_path)
    s3_key = f"{S3_KEY_PREFIX}{file_name}"  # S3에 저장될 경로 설정
    
    try:
        s3.upload_file(file_path, S3_BUCKET, s3_key)
        print(f"✅ S3 업로드 완료: s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"❌ S3 업로드 실패: {e}")

# ✅ 실행
if __name__ == "__main__":
    upload_to_s3(parquet_file_path)
