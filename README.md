![image](https://github.com/user-attachments/assets/42f1daeb-161f-4a4a-9007-d82d79dd697d)# 📊 SportsDrink 점유율 예측 자동화 ETL 파이프라인

유튜브/네이버 검색 데이터를 기반으로 Spark, Iceberg, NiFi, Airflow 등 실무 환경에서 사용하는 기술을 조합해 **End-to-End 데이터 파이프라인**을 구축했습니다.

---

## ✅ 프로젝트 개요

- **데이터 수집**: 유튜브 API + 네이버 검색어 API
- **데이터 정제 및 저장**: Spark → Iceberg 테이블 (S3 기반)
- **데이터 품질검증**: Null, 중복, Row 수 기준 검증 + Slack 알림
- **자동화 스케줄링**: Airflow DAG 기반 ETL + 로그 수집
- **추후 확장 계획**: ML 예측 및 운영환경 자동화

---

## 📌 전체 데이터 흐름 구성
![image](https://github.com/user-attachments/assets/a1dcd9a5-e8f6-4f38-b693-bee880720f02)

![image](https://github.com/user-attachments/assets/fbd89b35-489e-4a04-8f1c-0cd78c14088c)
![image](https://github.com/user-attachments/assets/d813cc1c-ee35-4dc1-b7f8-08a2bb3b1ca7)
![image](https://github.com/user-attachments/assets/f70ffdfc-c584-4153-8b49-b8908c5be005)
![image](https://github.com/user-attachments/assets/81aec635-dc8a-450a-832b-de8ad3dca7ac)
![image](https://github.com/user-attachments/assets/4909ef7b-ec92-4515-a15f-376fe18d2cbd)
![image](https://github.com/user-attachments/assets/9229dacd-2942-4565-9b4c-81b648f51f71)
![image](https://github.com/user-attachments/assets/fbe0f23c-505c-455a-b304-edb506c4059e)
