
# 🏃‍♂️ SportsDrink Pipeline (End-to-End 데이터 파이프라인 구축 프로젝트)

> **스포츠음료 검색량 기반 실시간 데이터 파이프라인 구축 및 자동화**  
YouTube/Naver API로 수집한 데이터를 S3에 저장하고, Spark로 정제 → Iceberg 저장 →  
Trino 및 Hive Metastore 연동 → GitHub Actions & Airflow 기반 자동화를 구현한 실무형 프로젝트입니다.

---

## 📌 프로젝트 개요

- YouTube/Naver 검색 데이터를 수집하여 실시간 시장 반응을 분석
- Spark 기반 데이터 정제 및 이상치 처리
- Iceberg 포맷으로 저장하여 버전 관리 및 증분 적재 자동화
- Hive Metastore 연동으로 Trino/Spark SQL 활용
- GitHub Actions + Airflow로 CI/CD 및 스케줄링 자동화

---

## 🗂️ 전체 데이터 흐름도

📷 **[ETL 전체 아키텍처 구성도] 삽입 위치**  
> 구성도: NiFi → S3 → Spark → Iceberg → Hive Metastore → Trino/Spark SQL  
> + Airflow & GitHub Actions 자동화 포함

---

## 🧱 주요 기술 스택

| 범주         | 기술                                                         |
|--------------|--------------------------------------------------------------|
| 수집         | YouTube API, Naver API, **Apache NiFi**                      |
| 저장소       | AWS S3, Apache Iceberg, Hive Metastore                       |
| 처리 및 정제 | **Apache Spark 3.5**, PySpark, Spark SQL, Adaptive Execution |
| 스케줄링     | **Apache Airflow**, Airflow DAG, PythonOperator              |
| 배포 자동화  | GitHub Actions, EC2 SSH 배포                                 |
| 모니터링     | Slack Webhook, Filebeat → Logstash → Elasticsearch → Kibana  |

---

## 🔄 자동화 흐름 (Airflow + GitHub Actions)

📷 **[Airflow DAG 흐름도] 삽입 위치**

- DAG 별로 수집/정제/저장 작업 구성
- `.env` 자동 로드 및 Slack 실패 알림 포함
- GitHub Actions로 코드 Push 시 EC2 서버에 DAG 자동 복사

---

## 🧼 Spark 정제 단계 (Bronze → Silver)

📷 **[1차/2차 클렌징 정제 기준 이미지] 삽입 위치**

- 결측치 제거 (`dropna`)
- 중복 제거 (`dropDuplicates`)
- 공백 제거 (`trim`)
- 텍스트 필터링 (`length ≥ 3`)
- 이상치 제거 (`count >= 0`)
- 날짜 파티셔닝: `feature_date`

---

## ❄ Iceberg 테이블 저장 구조

- Spark `.writeTo().using("iceberg")` 방식으로 저장
- 저장과 동시에 Hive Metastore에 자동 등록
- `catalog.hadoop_hms` 방식으로 트리노/스파크 SQL 통합 조회 가능
- `feature_date` 기반 파티셔닝 → 성능 최적화

---

## 🔁 증분 적재 전략

📷 **[증분 처리 흐름도] 삽입 위치**

- PostgreSQL, Elasticsearch 모두에 날짜 기반 증분 처리 적용
- `최신 날짜 ~ 오늘 날짜`만 API 호출하여 분석
- PostgreSQL은 `ON CONFLICT DO UPDATE` 방식
- Elasticsearch는 `_op_type=update` + `doc_as_upsert=True` 방식

---

## 📊 데이터 품질 검증 (Slack 알림 포함)

📷 **[Slack 품질 알림 예시 이미지] 삽입 위치**

- Spark로 null/중복/row 수 검증
- Slack Webhook으로 알림 전송
- 영상별 row 기준 1000건 이상 검증 통과 시 PASS

---

## 🧠 Spark 성능 최적화

📷 **[Adaptive Execution / Broadcast Join 이해 이미지] 삽입 위치**

- `spark.sql.adaptive.enabled=true` → 실행 시 자동 조인 방식 결정
- 댓글이 많은 경우, `Broadcast Join`으로 적은 테이블만 메모리에 적재
- Skew Join 시 자동 salting 처리도 가능

---

## 💾 학습용 Feature 생성 (1 Row per Video)

- 영상 + 채널 + 댓글 수 데이터를 조인하여 한 줄로 구성
- `like_to_view_ratio`, `comment_to_view_ratio`, `engagement_score` 등 주요 feature 포함

📷 **[Feature 저장 샘플 (Parquet or CSV)] 삽입 위치**

```text
training_feature_for_share_forecast.csv
- view_count
- like_count
- comment_count
- subscriber_count
- engagement_score
```

---


## 📈 향후 확장 계획

- Prefect로 DAG 이관 및 태스크 간 의존성 관리
- MLflow 연동하여 모델 버전 관리 및 서빙 구성
- Redis 기반 Feature Store 구성
- Slack 알림 → Discord Webhook으로 이중화 예정

---

## 👨‍💻 개발자 Note

- 실무와 유사한 S3 기반 Iceberg 테이블 설계 경험
- Trino + Hive Catalog 연동 및 쿼리 최적화
- GitHub Actions 기반 CI/CD 자동화 및 Airflow 연동 테스트 완료
- 증분 적재, Adaptive Execution 등 실전 Spark 경험 내재화
