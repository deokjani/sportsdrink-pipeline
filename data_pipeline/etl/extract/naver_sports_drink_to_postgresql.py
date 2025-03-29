# -*- coding: utf-8 -*-
'''
네이버 검색 API 활용 → 이온음료 점유율 데이터 수집 & PostgreSQL 저장 (최적화 코드)
'''

import urllib.request
import json
import os
import time
import psycopg2  # ✅ PostgreSQL 연결 라이브러리
from dotenv import load_dotenv
from datetime import datetime

# ✅ .env 파일 로드
env_path = "C:\\project\\sportsdrink-pipeline-spark-airflow\\data_pipeline\\docker\\.env"
load_dotenv(env_path)

# ✅ PostgreSQL 연결 설정
DB_HOST = "localhost"  # ✅ Docker 네트워크 내 컨테이너 이름 사용
DB_PORT = "5432"
DB_NAME = "airflow_db"
DB_USER = "progress"
DB_PASSWORD = "progress"

# ✅ 환경 변수에서 API 키 가져오기
client_id = os.getenv("NAVER_CLIENT_ID")
client_secret = os.getenv("NAVER_CLIENT_SECRET")

if not client_id or not client_secret:
    raise ValueError("API 키가 설정되지 않았습니다. .env 파일을 확인하세요.")

# ✅ API URL
url = "https://openapi.naver.com/v1/datalab/search"

# ✅ 현재 날짜 가져오기
today = datetime.now().strftime("%Y-%m-%d")

# ✅ 스포츠 음료 키워드 그룹
sports_drink = [
    {"groupName": "포카리스웨트", "keywords": ["포카리", "포카리스웨트", "Pocari Sweat"]},
    {"groupName": "게토레이", "keywords": ["게토레이", "Gatorade"]},
    {"groupName": "파워에이드", "keywords": ["파워에이드", "Powerade"]},
    {"groupName": "토레타", "keywords": ["토레타", "Toreta"]},
    {"groupName": "링티", "keywords": ["링티", "Lingtea"]}
]

# ✅ 연령대별 매핑
age_group_mapping = {
    "10대": ["2"],
    "20대": ["3", "4"],
    "30대": ["5", "6"],
    "40대": ["7", "8"],
    "50대": ["9", "10"],
    "60대 이상": ["11"]
}

# ✅ PostgreSQL 테이블 초기화 (존재하면 삭제 후 재생성, 없으면 그냥 생성)
def reset_or_create_table():
    """ PostgreSQL 테이블이 존재하면 삭제 후 재생성, 없으면 새로 생성 """
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    cursor = conn.cursor()

    # ✅ 테이블 존재 여부 확인
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'sports_drink_search'
        );
    """)
    table_exists = cursor.fetchone()[0]  # True(존재함) / False(없음)

    if table_exists:
        print("⚠️ 기존 테이블 삭제 후 재생성 중...")
        cursor.execute("DROP TABLE sports_drink_search;")
    else:
        print("✅ 테이블이 존재하지 않아 새로 생성합니다.")

    # ✅ 테이블 생성
    cursor.execute("""
        CREATE TABLE sports_drink_search (
            id SERIAL PRIMARY KEY,
            period DATE NOT NULL,
            gender VARCHAR(10) NOT NULL,
            age_group VARCHAR(20) NOT NULL,
            brand VARCHAR(50) NOT NULL,
            ratio FLOAT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ PostgreSQL 테이블 초기화 완료!")

# ✅ 네이버 API에서 데이터 수집
def fetch_data(gender, ages):
    """ 네이버 API에서 특정 성별 및 연령대의 검색 데이터를 가져옴 """
    results = {}
    for batch in [sports_drink[i:i + 5] for i in range(0, len(sports_drink), 5)]:
        body = json.dumps({
            "startDate": "2024-01-01",
            "endDate": today,
            "timeUnit": "date",
            "keywordGroups": batch,
            "device": "",
            "ages": ages,
            "gender": gender
        })

        request = urllib.request.Request(url)
        request.add_header("X-Naver-Client-Id", client_id)
        request.add_header("X-Naver-Client-Secret", client_secret)
        request.add_header("Content-Type", "application/json")

        try:
            response = urllib.request.urlopen(request, data=body.encode("utf-8"))
            if response.getcode() == 200:
                data = json.loads(response.read().decode('utf-8'))
                for group in data.get("results", []):
                    for entry in group.get("data", []):
                        period, ratio = entry["period"], entry["ratio"]
                        if period not in results:
                            results[period] = {item["groupName"]: 0 for item in sports_drink}
                        results[period][group["title"]] += ratio
        except Exception as e:
            print(f"❌ API 요청 오류: {e}")
            continue
    return results

# ✅ 데이터 정규화 및 수집
def collect_and_normalize_data():
    """ 네이버 API 데이터를 성별 및 연령대별로 정리하고 정규화 """
    aggregated = {"male": {}, "female": {}}
    for group_label, age_codes in age_group_mapping.items():
        aggregated["male"][group_label] = fetch_data("m", age_codes)
        aggregated["female"][group_label] = fetch_data("f", age_codes)

    for gender in aggregated:
        for age_group in aggregated[gender]:
            for period, group_ratios in aggregated[gender][age_group].items():
                total = sum(group_ratios.values())
                if total > 0:
                    aggregated[gender][age_group][period] = {k: round(v / total * 100, 2) for k, v in group_ratios.items()}
    
    return aggregated

# ✅ PostgreSQL에 데이터 저장
def save_to_postgres(aggregated_data):
    """ 수집된 데이터를 PostgreSQL에 저장 """
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    cursor = conn.cursor()

    for gender, age_groups in aggregated_data.items():
        for age_group, periods in age_groups.items():
            for period in sorted(periods.keys()):
                group_ratios = periods[period]
                for brand, ratio in group_ratios.items():
                    cursor.execute("""
                        INSERT INTO sports_drink_search (period, gender, age_group, brand, ratio)
                        VALUES (%s, %s, %s, %s, %s);
                    """, (period, gender, age_group, brand, ratio))

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ PostgreSQL 저장 완료!")

# ✅ 실행
if __name__ == "__main__":
    start_time = time.time()

    reset_or_create_table()  # ✅ 테이블 초기화 (존재하면 삭제 후 재생성)
    aggregated_data = collect_and_normalize_data()  # ✅ 데이터 수집 및 정규화
    save_to_postgres(aggregated_data)  # ✅ PostgreSQL 저장

    elapsed_time = time.time() - start_time
    print(f"\n🚀 전체 실행 완료! ⏳ 총 실행 시간: {elapsed_time:.2f}초")
