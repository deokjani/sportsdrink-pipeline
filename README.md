
# SportsDrink Pipeline: 실시간 검색량 기반 데이터 파이프라인 구축

이 프로젝트는 스포츠음료 키워드에 대한 실시간 검색 데이터를 수집하고, 정제하여 분석 가능한 구조로 저장하는 End-to-End 데이터 파이프라인입니다.  
YouTube, Naver 등 외부 API를 통해 데이터를 수집하고, Spark와 Iceberg를 활용해 효율적으로 관리할 수 있는 데이터 저장 구조를 구현했습니다.  
자동화와 안정성까지 고려해 실무에서 사용할 수 있는 형태로 구성되었습니다.

---

## 프로젝트 개요

- YouTube 및 Naver API를 활용해 스포츠음료 관련 검색 데이터를 수집합니다.
- 수집된 데이터는 S3에 저장되고, Apache Spark를 통해 정제 및 가공 과정을 거칩니다.
- 정제된 데이터는 Iceberg 포맷으로 저장되며, Hive Metastore에 등록되어 Trino 및 Spark SQL로 바로 조회할 수 있습니다.
- GitHub Actions와 Airflow를 이용해 코드 자동 배포 및 ETL 스케줄링을 구성했습니다.

---

## 데이터 흐름 구조

전체 파이프라인은 다음과 같은 흐름으로 구성됩니다.

```
Apache NiFi → S3 (raw 저장) → Spark (정제) → Iceberg (저장 및 버전관리)
                        ↓
         Hive Metastore ↔ Trino / Spark SQL (분석)
                        ↓
             GitHub Actions & Airflow (자동화)
```

---

## 사용 기술

| 구분       | 기술 요소 |
|------------|-----------|
| 수집       | Naver API, YouTube API, Apache NiFi |
| 저장       | AWS S3, Apache Iceberg, Hive Metastore |
| 처리       | Apache Spark, PySpark, Adaptive Execution |
| 분석       | Trino, Spark SQL |
| 자동화     | Apache Airflow, GitHub Actions |
| 모니터링   | Slack Webhook 알림 |
| 로그 수집  | Filebeat → Logstash → Elasticsearch |

---

## 정제 및 품질 관리

데이터는 다음과 같은 단계를 거쳐 가공됩니다.

- 공백 및 결측치 처리
- 중복 제거
- 텍스트 필터링 (너무 짧거나 의미 없는 텍스트 제거)
- 이상치 제거 (ex. 음수 값)
- 날짜 기반 파티셔닝

또한 데이터 품질 검증 결과는 Slack을 통해 실시간으로 알림을 받을 수 있도록 구성했습니다.

---

## Iceberg 저장 및 관리 전략

- Spark에서 `.writeTo(...).using("iceberg")` 형식으로 Iceberg 테이블에 저장합니다.
- 저장과 동시에 Hive Metastore에 자동 등록되어 별도 메타관리 없이 Trino와 Spark SQL에서 접근 가능합니다.
- `feature_date`를 기준으로 파티셔닝해 쿼리 효율을 높였으며, 증분 적재가 가능하도록 설계했습니다.

---

## 증분 처리 전략

- 기존 저장된 데이터 중 마지막 날짜를 기준으로, 이후 데이터만 수집합니다.
- PostgreSQL에서는 기본 키 기반 `ON CONFLICT DO UPDATE` 방식으로 업데이트/삽입 처리
- Elasticsearch에서는 `_id`를 지정해 upsert 방식으로 처리

---

## 자동화 구성

- GitHub Actions를 이용해 코드 Push 시 EC2 서버에 자동 배포
- Airflow DAG을 통해 ETL 작업 스케줄링, 실패 시 Slack 알림 전송
- .env 파일을 환경별로 분리 관리하여 재사용성과 보안성을 확보

---

## 학습용 Feature 구성

최종적으로 생성된 학습용 데이터는 다음과 같은 feature로 구성됩니다.

- view_count
- like_count
- comment_count
- like_to_view_ratio
- engagement_score
- subscriber_count
- is_big_channel
- channel_tier 등

해당 feature는 영상 단위로 집계되어 LSTM 등 시계열 모델에 바로 투입 가능한 형태로 저장됩니다.

---

## 디렉토리 구조 예시

```
data_pipeline/
├── etl/
│   ├── extract/       # API 수집 코드
│   ├── transform/     # Spark 정제 코드
│   └── validate/      # 품질 검증
├── airflow/           # DAG 및 자동화 관련 코드
├── docker/            # Docker 및 환경 설정
├── features/          # 학습용 feature 저장소
└── scripts/           # Spark 실행 스크립트
```

---

## 향후 확장 계획

- Prefect를 활용한 태스크 단위 워크플로우 구성
- MLflow 기반 모델 관리 시스템 구축
- Redis 기반 Feature Store 연동
- Discord 및 Slack을 통한 다중 채널 알림 시스템 도입

---

## 정리하며

이 프로젝트는 실무에서 사용되는 Spark, Iceberg, Airflow, GitHub Actions 등 데이터 엔지니어링 핵심 기술을 하나의 파이프라인 안에서 통합한 예제입니다.  
단순히 데이터를 수집하는 데서 끝나는 것이 아니라, 저장, 정제, 관리, 자동화까지 전 과정을 경험할 수 있도록 구성했습니다.  
포트폴리오 및 실무 대응력을 키우는 데 목적이 있으며, 확장성 있는 구조로 만들어졌습니다.
