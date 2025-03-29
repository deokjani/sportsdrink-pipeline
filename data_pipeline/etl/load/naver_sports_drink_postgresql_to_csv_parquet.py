import psycopg2
import pandas as pd
import os
from datetime import datetime

# ✅ PostgreSQL 연결 정보 (Airflow DB)
PG_HOST = "localhost"
PG_DB = "airflow_db"
PG_USER = "progress"
PG_PASSWORD = "progress"
PG_PORT = 5432

# ✅ 저장 경로
save_dir = "C:/project/sportsdrink-pipeline-spark-airflow/data_pipeline/data/raw/"
os.makedirs(save_dir, exist_ok=True)

# ✅ 파일명 구성 요소
service_name = "SportsDrink"  # 서비스명
data_source = "Naver"         # 데이터 출처 (ex: Twitter, Naver, YouTube 등)
data_type = "Search"          # 데이터 성격 (ex: Search, Sales, Trend, Log 등)
batch_cycle = "Daily"         # 배치 주기 (ex: Hourly, Daily, Weekly, Monthly)
date_str = datetime.now().strftime('%Y%m%d')  # 날짜 포맷

# ✅ 파일 경로 정의 (Parquet + CSV)
parquet_file_path = os.path.join(
    save_dir, f"{service_name}_{data_source}_{data_type}_{batch_cycle}_{date_str}.parquet"
)
csv_file_path = os.path.join(
    save_dir, f"{service_name}_{data_source}_{data_type}_{batch_cycle}_{date_str}.csv"
)

# ✅ PostgreSQL 데이터 가져오기 함수
def fetch_data_from_postgresql():
    """PostgreSQL에서 sports_drink_search 테이블 데이터를 가져와 DataFrame으로 반환"""
    try:
        conn = psycopg2.connect(
            host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD, port=PG_PORT
        )
        query = "SELECT * FROM sports_drink_search ORDER BY period, gender, age_group, brand;"
        df = pd.read_sql(query, conn)
        print(f"✅ PostgreSQL에서 {len(df)}개 데이터 로드 완료!")
    except Exception as e:
        print(f"❌ PostgreSQL 데이터 로드 오류: {e}")
        df = pd.DataFrame()
    finally:
        conn.close()
    return df

# ✅ DataFrame 저장 함수 (Parquet & CSV)
def save_files(df):
    """DataFrame을 Parquet & CSV 파일로 모두 저장"""
    if not df.empty:
        df.to_parquet(parquet_file_path, engine="pyarrow", index=False)
        print(f"✅ Parquet 파일 저장 완료: {parquet_file_path}")

        df.to_csv(csv_file_path, index=False, encoding="utf-8-sig")
        print(f"✅ CSV 파일 저장 완료: {csv_file_path}")
    else:
        print("⚠️ 저장할 데이터가 없습니다.")

# ✅ 실행
if __name__ == "__main__":
    df = fetch_data_from_postgresql()
    save_files(df)
