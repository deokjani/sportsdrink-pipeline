# -*- coding: utf-8 -*-
'''
날짜 기반 증분처리 → 이온음료 점유율 데이터 Elasticsearch 벌크 저장 (최적화)
'''

import urllib.request
import json
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers

# ✅ 환경 설정
load_dotenv("C:\\project\\sportsdrink-pipeline-spark-airflow\\data_pipeline\\docker\\.env")
client_id = os.getenv("NAVER_CLIENT_ID")
client_secret = os.getenv("NAVER_CLIENT_SECRET")
if not client_id or not client_secret:
    raise ValueError("❌ NAVER API 키 누락. .env 파일 확인!")

es = Elasticsearch("http://localhost:9200")
index_name = "sports_drink_search"

# ✅ API 기본 설정
url = "https://openapi.naver.com/v1/datalab/search"
today = datetime.today().strftime("%Y-%m-%d")

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

# ✅ Elasticsearch에서 마지막 저장 날짜 확인
def get_latest_period_from_es(index):
    try:
        result = es.search(
            index=index,
            size=1,
            sort="period:desc",
            _source=["period"]
        )
        if result["hits"]["hits"]:
            return result["hits"]["hits"][0]["_source"]["period"]
    except:
        pass
    return "2024-01-01"  # 기본 시작일

# ✅ 네이버 API 호출
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

# ✅ 전체 데이터 수집
def collect_data_incrementally(start_date, end_date):
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

# ✅ Elasticsearch 벌크 저장
def save_to_elasticsearch_bulk(index, aggregated_data):
    actions = []
    for gender, age_groups in aggregated_data.items():
        for age_group, periods in age_groups.items():
            for period in sorted(periods.keys()):
                group_ratios = periods[period]
                for brand, ratio in group_ratios.items():
                    doc_id = f"{period}_{gender}_{age_group}_{brand}"
                    actions.append({
                        "_op_type": "update",
                        "_index": index,
                        "_id": doc_id,
                        "doc": {
                            "period": period,
                            "gender": gender,
                            "age_group": age_group,
                            "brand": brand,
                            "ratio": ratio,
                            "timestamp": datetime.now().isoformat()
                        },
                        "doc_as_upsert": True
                    })

    try:
        helpers.bulk(es, actions)
        print(f"✅ Elasticsearch에 {len(actions)}개 문서 저장 완료")
    except Exception as e:
        print(f"❌ Elasticsearch 저장 실패: {e}")
        raise

# ✅ 실행
if __name__ == "__main__":
    start = time.time()
    latest_date = get_latest_period_from_es(index_name)
    next_start_date = (datetime.strptime(latest_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"🗓️ 최신 수집일: {latest_date} → 새 수집 시작일: {next_start_date}")
    if next_start_date > today:
        print("✅ 최신 데이터까지 이미 수집 완료됨.")
    else:
        data = collect_data_incrementally(next_start_date, today)
        save_to_elasticsearch_bulk(index_name, data)
        print(f"🚀 증분 수집 완료! ⏳ 소요 시간: {time.time() - start:.2f}초")
