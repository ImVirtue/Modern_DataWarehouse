from pyspark.sql import SparkSession
from pyspark.sql.functions import col

DATABASE = "DWH"
TABLE = "dim_customer"
MINIO_BUCKET = "datalake"

def create_spark_connection():
    spark = SparkSession.builder \
        .appName("ClickhouseFactOrder") \
        .config("spark.jars.packages", "com.clickhouse:clickhouse-jdbc:0.4.6") \
        .config("spark.jars.repositories", "https://repo1.maven.org/maven2/") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.access.key", "fmbvirtueee") \
        .config("spark.hadoop.fs.s3a.secret.key", "Matkhauchung1@") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    return spark

def read_data_from_minio(spark, path):
    df = spark.read.parquet(path)
    return df

def write_to_clickhouse(spark, dataframe):
    dataframe.write \
        .format("jdbc") \
        .option("url", f"jdbc:clickhouse://clickhouse:8123/{DATABASE}") \
        .option("dbtable", TABLE) \
        .option("user", "admin") \
        .option("password", "Matkhauchung1@") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode("append") \
        .save()

    print(f"Đã ghi dữ liệu vào thành công!")

if __name__ == "__main__":
    input_path = f"s3a://{MINIO_BUCKET}/serving/dimensional_model/{TABLE}"
    spark = create_spark_connection()
    df = read_data_from_minio(spark, input_path)
    write_to_clickhouse(spark, df)

