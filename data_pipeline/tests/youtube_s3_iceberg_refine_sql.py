import os
import argparse
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# ✅ 1. 환경 변수 로드
load_dotenv(r"C:/ITWILL/SportsDrinkForecast/docker-elk/.env")

# ✅ 2. Spark 세션 생성 함수
def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Iceberg SQL Query Viewer")
        .config("spark.jars.packages",
                ",".join([
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3",
                    "software.amazon.awssdk:glue:2.20.66",
                    "software.amazon.awssdk:s3:2.20.66",
                    "software.amazon.awssdk:sts:2.20.66"
                ]))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.hadoop_prod_refined", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hadoop_prod_refined.type", "hadoop")
        .config("spark.sql.catalog.hadoop_prod_refined.warehouse",
                "s3a://deokjin-test-datalake/data/processed/iceberg/refined/sportsdrink_youtube_search_daily")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ✅ 3. 쿼리 함수 모듈
class IcebergQueryViewer:
    def __init__(self, spark):
        self.spark = spark

    def show_top_videos(self, limit=10):
        query = f"""
            SELECT video_id, title, view_count, like_count, comment_count
            FROM hadoop_prod_refined.video_info
            ORDER BY CAST(view_count AS INT) DESC
            LIMIT {limit}
        """
        self.spark.sql(query).show(truncate=False)

    def show_big_channels(self):
        query = """
            SELECT channel_id, channel_title, subscriber_count
            FROM hadoop_prod_refined.channel_info
            WHERE CAST(subscriber_count AS INT) > 100000
            ORDER BY CAST(subscriber_count AS INT) DESC
        """
        self.spark.sql(query).show(truncate=False)

    def show_popular_comments(self, limit=5):
        query = f"""
            SELECT comment_text, like_count, published_at
            FROM hadoop_prod_refined.comments_info
            ORDER BY CAST(like_count AS INT) DESC
            LIMIT {limit}
        """
        self.spark.sql(query).show(truncate=False)

# ✅ 4. 메인 실행
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--query_type', type=str, default='top_videos',
                        help='Query type: top_videos / big_channels / popular_comments (default: top_videos)')
    parser.add_argument('--limit', type=int, default=10, help='Limit rows for result')
    args = parser.parse_args()

    spark = create_spark_session()
    viewer = IcebergQueryViewer(spark)

    if args.query_type == 'top_videos':
        viewer.show_top_videos(args.limit)
    elif args.query_type == 'big_channels':
        viewer.show_big_channels()
    elif args.query_type == 'popular_comments':
        viewer.show_popular_comments(args.limit)
    else:
        print("❗ 지원하지 않는 query_type 입니다.")

    spark.stop()
    print("🎉 쿼리 출력 완료!")

# ✅ 5. Entry point
if __name__ == "__main__":
    main()
