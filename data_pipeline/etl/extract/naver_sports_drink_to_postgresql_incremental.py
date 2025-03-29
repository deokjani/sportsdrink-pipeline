# -*- coding: utf-8 -*-
'''
네이버 검색 API 활용 → PostgreSQL 저장 (날짜 기반 증분처리)
'''

import urllib.request
import json
import os
import time
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta

# ✅ 환경 설정
load_dotenv("C:\\project\\sportsdrink-pipeline-spark-airflow\\data_pipeline\\docker\\.env")
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "airflow_db"
DB_USER = "progress"
DB_PASSWORD = "progress"

client_id = os.getenv("NAVER_CLIENT_ID")
client_secret = os.getenv("NAVER_CLIENT_SECRET")
if not client_id or not client_secret:
    raise ValueError("❌ NAVER API 키 누락. .env 파일 확인!")

url = "https://openapi.naver.com/v1/datalab/search"
today = datetime.now().strftime("%Y-%m-%d")

sports_drink = [
    {"groupName": "포카리스웨트", "keywords": ["포카리", "포카리스웨트", "Pocari Sweat"]},
    {"groupName": "게토레이", "keywords": ["게토레이", "Gatorade"]},
    {"groupName": "파워에이드", "keywords": ["파워에이드", "Powerade"]},
    {"groupName": "토레타", "keywords": ["토레타", "Toreta"]},
    {"groupName": "링티", "keywords": ["링티", "Lingtea"]}
]

age_group_mapping = {
    "10대": ["2"], "20대": ["3", "4"], "30대": ["5", "6"],
    "40대": ["7", "8"], "50대": ["9", "10"], "60대 이상": ["11"]
}

# ✅ 테이블 생성 (UNIQUE 추가)
def ensure_table():
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sports_drink_search (
            id SERIAL PRIMARY KEY,
            period DATE NOT NULL,
            gender VARCHAR(10) NOT NULL,
            age_group VARCHAR(20) NOT NULL,
            brand VARCHAR(50) NOT NULL,
            ratio FLOAT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (period, gender, age_group, brand)
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ PostgreSQL 테이블 확인/생성 완료")

# ✅ DB에서 가장 마지막 날짜 조회
def get_last_period():
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(period) FROM sports_drink_search;")
    result = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return result.strftime('%Y-%m-%d') if result else "2024-01-01"

# ✅ API 호출
def fetch_data(gender, ages, start_date, end_date):
    results = {}
    for batch in [sports_drink[i:i + 5] for i in range(0, len(sports_drink), 5)]:
        body = json.dumps({
            "startDate": start_date,
            "endDate": end_date,
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
            print(f"❌ API 오류 ({gender}, {ages}): {e}")
            continue
    return results

# ✅ 데이터 정규화 및 수집
def collect_data(start_date, end_date):
    aggregated = {"male": {}, "female": {}}
    for gender in ["male", "female"]:
        g = "m" if gender == "male" else "f"
        for age_group, ages in age_group_mapping.items():
            data = fetch_data(g, ages, start_date, end_date)
            for period, ratios in data.items():
                total = sum(ratios.values())
                if total > 0:
                    normalized = {k: round(v / total * 100, 2) for k, v in ratios.items()}
                    aggregated[gender].setdefault(age_group, {})[period] = normalized
    return aggregated

# ✅ PostgreSQL 저장 (중복 방지)
def save_to_postgres(aggregated_data):
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    cursor = conn.cursor()
    count = 0

    for gender, age_groups in aggregated_data.items():
        for age_group, periods in age_groups.items():
            for period, ratios in periods.items():
                for brand, ratio in ratios.items():
                    cursor.execute("""
                        INSERT INTO sports_drink_search (period, gender, age_group, brand, ratio)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (period, gender, age_group, brand, ratio))
                    count += 1

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ {count}개 row 저장 완료")

# ✅ 실행
if __name__ == "__main__":
    start_time = time.time()

    ensure_table()
    last_date = get_last_period()
    next_start = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime('%Y-%m-%d')

    print(f"🗓️ 증분 수집 범위: {next_start} ~ {today}")
    if next_start > today:
        print("✅ 최신 데이터까지 이미 저장되어 있습니다.")
    else:
        data = collect_data(next_start, today)
        save_to_postgres(data)

    print(f"🚀 실행 완료! ⏱️ {time.time() - start_time:.2f}초")
