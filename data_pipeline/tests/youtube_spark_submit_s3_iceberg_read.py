import os
import pandas as pd
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# ✅ 환경 변수 로드 (.env 경로는 필요에 따라 수정)
load_dotenv(r"C:/ITWILL/SportsDrinkForecast/docker-elk/.env")

# ✅ Spark 세션 생성 함수
def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Iceberg Table Inspector")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.warehouse.dir", "/tmp/never-used")
        .config("spark.sql.catalog.hadoop_prod_refined", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hadoop_prod_refined.type", "hadoop")
        .config("spark.sql.catalog.hadoop_prod_refined.warehouse",
                "s3a://deokjin-test-datalake/data/processed/iceberg/refined")
        .config("spark.sql.catalog.hadoop_prod_feature", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hadoop_prod_feature.type", "hadoop")
        .config("spark.sql.catalog.hadoop_prod_feature.warehouse",
                "s3a://deokjin-test-datalake/data/processed/iceberg/feature_ready")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ✅ Iceberg 테이블 검사 클래스
class IcebergTableInspector:
    def __init__(self, spark):
        self.spark = spark

    def show_table_columns(self, catalog_name, table_name):
        print(f"\n[INFO] {catalog_name}.{table_name} 컬럼 스키마:")
        self.spark.sql(f"DESCRIBE {catalog_name}.{table_name}").show(truncate=False)

    def show_table_preview(self, catalog_name, table_name, limit=20, save_csv=False,
                           save_dir="C:/ITWILL/SportsDrinkForecast/data_pipeline/data/output"):
        print(f"\n[INFO] {catalog_name}.{table_name} 데이터 미리보기 (상위 {limit}개):")
        df = self.spark.read.format("iceberg").load(f"{catalog_name}.{table_name}").limit(limit)

        # pandas 변환
        pandas_df = df.toPandas()
        print("[DEBUG] 가져온 데이터 shape:", pandas_df.shape)
        print("[DEBUG] 컬럼 목록:", pandas_df.columns.tolist())
        print(pandas_df.head())

        if save_csv:
            os.makedirs(save_dir, exist_ok=True)
            output_path = os.path.join(save_dir, f"{table_name.replace('.', '_')}_preview.csv")
            print(f"[DEBUG] 저장 경로: {output_path}")
            pandas_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"[INFO] 미리보기 데이터를 '{output_path}' 파일로 저장 완료.")

# ✅ 메인 실행부
if __name__ == "__main__":
    spark = create_spark_session()
    inspector = IcebergTableInspector(spark)

    catalog = "hadoop_prod_feature"
    table = "sportsdrink_youtube_search_daily.comments_feature"

    inspector.show_table_columns(catalog, table)
    inspector.show_table_preview(catalog, table, limit=20, save_csv=True)

    spark.stop()
    print("\n[INFO] 컬럼 및 미리보기 작업 완료!")
