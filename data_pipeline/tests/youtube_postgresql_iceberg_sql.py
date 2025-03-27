import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# ✅ 환경 변수 로드
load_dotenv(r"C:/ITWILL/SportsDrinkForecast/docker-elk/.env")

# ✅ Spark 세션 생성 (PostgreSQL 카탈로그 등록 포함)
spark = (
    SparkSession.builder
    .appName("Iceberg PostgreSQL Catalog E2E Example")
    .config("spark.jars.packages",
            ",".join([
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3",
                "org.postgresql:postgresql:42.6.0"
            ]))
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.postgres_prod", "org.apache.iceberg.jdbc.JdbcCatalog")
    .config("spark.sql.catalog.postgres_prod.uri", f"jdbc:postgresql://localhost:5432/progress")
    .config("spark.sql.catalog.postgres_prod.jdbc.user", os.getenv("POSTGRES_USER", "progress"))
    .config("spark.sql.catalog.postgres_prod.jdbc.password", os.getenv("POSTGRES_PASSWORD", "progress"))
    .config("spark.sql.catalog.postgres_prod.jdbc.driver", "org.postgresql.Driver")
    .config("spark.sql.catalog.postgres_prod.warehouse", "s3a://deokjin-test-datalake/data/processed/iceberg/postgres_catalog")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ✅ 1. 네임스페이스 생성
spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS postgres_prod.sportsdrink_youtube_search_daily
""")
print("[INFO] Namespace 생성 완료")

# ✅ 2. Iceberg 테이블 생성
spark.sql("""
    CREATE TABLE IF NOT EXISTS postgres_prod.sportsdrink_youtube_search_daily.comments_feature (
        video_id STRING,
        author STRING,
        comment_text STRING,
        published_at STRING,
        like_count STRING,
        comment_length INT,
        sentiment_flag INT
    )
    USING iceberg
""")
print("[INFO] Iceberg 테이블 생성 완료")

# ✅ 3. 임시 데이터 삽입 (예제)
spark.sql("""
    INSERT INTO postgres_prod.sportsdrink_youtube_search_daily.comments_feature VALUES
    ('abcd1234', '@user1', 'Great video!', '2024-03-22T10:00:00Z', '12', 11, 1),
    ('abcd5678', '@user2', 'Not bad at all', '2024-03-21T15:30:00Z', '5', 14, 1)
""")
print("[INFO] 예제 데이터 삽입 완료")

# ✅ 4. 데이터 조회
spark.sql("""
    SELECT * FROM postgres_prod.sportsdrink_youtube_search_daily.comments_feature
""").show(truncate=False)

# ✅ 5. PostgreSQL 메타데이터 테이블 확인 (PostgreSQL 직접 쿼리 추천)
print("\n[INFO] PostgreSQL 'tables' 메타데이터 테이블에는 테이블과 최신 메타정보가 자동으로 기록됨!")

spark.stop()
print("\n🎉 PostgreSQL Catalog End-to-End Iceberg 관리 완료!")
