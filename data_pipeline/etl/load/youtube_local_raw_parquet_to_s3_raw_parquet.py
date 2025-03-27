import boto3
import os
from datetime import datetime
from dotenv import load_dotenv

# ✅ 1) 환경변수(.env)에서 AWS 키 불러오기
load_dotenv(r"C:/ITWILL/SportsDrinkForecast/docker-elk/.env")

# ✅ 2) S3 관련 상수 설정
S3_BUCKET = "deokjin-test-datalake"          # 내 S3 버킷 이름
S3_REGION = "ap-northeast-2"                 # 서울 리전

# ✅ 3) 날짜 경로 지정 (업로드 시 날짜별 폴더에 넣기 위해)
today_date = datetime.now().strftime('%Y%m%d')
S3_KEY_PREFIX = f"data/raw/{today_date}"     # s3://버킷/data/raw/날짜 형식으로 저장

# ✅ 4) boto3 S3 클라이언트 생성
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=S3_REGION
)

# ✅ 5) 서비스 및 데이터 관련 이름 정의 (날짜는 제외)
service_name = "sportsdrink"  # 서비스명
data_source = "youtube"  # 데이터 출처 (ex: Twitter, Naver, YouTube 등)
data_type = "search"  # 데이터 성격 (ex: Search, Sales, Trend, Log 등)
batch_cycle = "daily"  # 배치 주기 (ex: Hourly, Daily, Weekly, Monthly)

# ✅ S3 업로드 시 S3에 들어갈 폴더 이름 (서비스명_출처_타입_주기)
folder_name_pattern = f"{service_name}_{data_source}_{data_type}_{batch_cycle}"

# ✅ 6) 업로드 함수 정의
def upload_file_to_s3(local_file_path, relative_s3_path):
    """
    로컬의 특정 파일을 S3의 지정된 경로에 업로드
    """
    s3_key = f"{S3_KEY_PREFIX}/{relative_s3_path}"
    try:
        s3.upload_file(local_file_path, S3_BUCKET, s3_key)
        print(f"✅ 업로드 완료: s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"🚨 업로드 실패: {e}")

# ✅ 7) 실행
if __name__ == "__main__":
    # ✅ 새로운 로컬 디렉토리 구조:
    # C:/ITWILL/SportsDrinkForecast/data_pipeline/data/raw/{오늘날짜}/{서비스명_출처_타입_주기}
    local_base_path = os.path.join(
        r"C:\ITWILL\SportsDrinkForecast\data_pipeline\data\raw",
        today_date,                     # 날짜 폴더
        folder_name_pattern             # 서비스명_출처_타입_주기 폴더
    )

    if not os.path.exists(local_base_path):
        print(f"🚨 경로 {local_base_path} 가 존재하지 않습니다.")
    else:
        print(f"📂 '{local_base_path}' 경로의 parquet 파일을 S3에 업로드 시작...")

        for root, _, files in os.walk(local_base_path):
            for file in files:
                if file.endswith(".parquet"):
                    local_file_path = os.path.join(root, file)

                    # S3 경로는 상대 경로를 구해서 {서비스명_출처_타입_주기}/ 하위로 업로드
                    relative_s3_path = os.path.relpath(local_file_path, local_base_path)
                    relative_s3_path = os.path.join(folder_name_pattern, relative_s3_path).replace("\\", "/")

                    upload_file_to_s3(local_file_path, relative_s3_path)

        print(f"\n🎉 '{folder_name_pattern}' 경로의 parquet 파일 S3 업로드 완료!")

    print("\n✅ 전체 업로드 작업 종료")
