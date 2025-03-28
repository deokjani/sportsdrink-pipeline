import psycopg2
import pandas as pd
import os
from datetime import datetime

# ✅ PostgreSQL 연결 정보 (Airflow DB 사용)
PG_HOST = "progress-db"  # Docker 네트워크 내 컨테이너 이름
PG_DB = "airflow_db"
PG_USER = "progress"
PG_PASSWORD = "progress"
PG_PORT = 5432

# ✅ Parquet 저장 경로
save_dir = "/opt/airflow/data/processed/"
os.makedirs(save_dir, exist_ok=True)

# 파일명 구성 요소
service_name = "SportsDrink"  # 서비스명
data_source = "Naver" # 데이터 출처 (ex: twitter, naver, youtube, instagram 등)
data_type = "Search"  # 데이터 성격 (ex: search, sales, trend, log 등)
batch_cycle = "Daily"  # 배치 주기 (ex: hourly, daily, weekly, monthly)

# 파일명 포맷
parquet_file_path = os.path.join(save_dir,f"{service_name}_{data_source}_{data_type}_{batch_cycle}_{datetime.now().strftime('%Y%m%d')}.parquet")

# ✅ PostgreSQL 데이터 가져오기
def fetch_data_from_postgresql():
    """ PostgreSQL에서 sports_drink_search 데이터를 가져와 DataFrame으로 변환 """
    conn = psycopg2.connect(
        host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD, port=PG_PORT
    )
    query = "SELECT * FROM sports_drink_search ORDER BY period, gender, age_group, brand;"
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"✅ PostgreSQL에서 {len(df)}개 데이터 로드 완료!")
    return df

# ✅ DataFrame을 Parquet 파일로 저장
def save_to_parquet(df):
    """ Pandas DataFrame을 Parquet 파일로 저장 """
    df.to_parquet(parquet_file_path, engine="pyarrow", index=False)
    print(f"✅ Parquet 파일 저장 완료: {parquet_file_path}")

# ✅ 실행
if __name__ == "__main__":
    df = fetch_data_from_postgresql()  # PostgreSQL 데이터 로드
    save_to_parquet(df)  # Parquet 저장
