# ✅ 최신 PySpark & Jupyter Notebook 이미지 사용
FROM jupyter/pyspark-notebook:spark-3.5.0

# ✅ 필수 패키지 설치
RUN pip install --no-cache-dir \
    kafka-python \
    findspark \
    pyspark

# ✅ tini 수동 설치 (이 부분 추가)
USER root
RUN apt-get update && apt-get install -y tini
USER jovyan

# ✅ 실행 커맨드 수정 (`tini`를 명확하게 지정)
ENTRYPOINT ["/usr/bin/tini", "--"]

# ✅ 실행 명령어 (Jupyter Notebook 실행)
CMD ["start-notebook.sh"]
