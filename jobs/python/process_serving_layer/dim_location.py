from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, split
from pyspark.sql.types import StringType
import zlib

BUCKET_NAME = "datalake"
TABLE = "customers"

def create_spark_conn():
    spark = SparkSession.builder \
        .appName("product dimension") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.access.key", "fmbvirtueee") \
        .config("spark.hadoop.fs.s3a.secret.key", "Matkhauchung1@") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    return spark

def generate_location_key(location):
    byte_data = location.encode('utf-8')
    location_key = zlib.crc32(byte_data) & 0xffffffff
    return str(location_key)

def read_location_data(spark, key_function, input_path):
    key_function = udf(key_function, StringType())

    df_oltp_customers = spark.read.parquet(input_path)
    df_dim_location = df_oltp_customers.alias("cus")\
        .withColumn("location_key", key_function(col("cus.location"))) \
        .withColumn("country", split(col("cus.location"), "_").getItem(0)) \
        .withColumn("city", split(col("cus.location"), "_").getItem(1)) \
        .withColumn("district", split(col("cus.location"), "_").getItem(2))\
            .select(
            col("location_key"),
            col("country"),
            col("city"),
            col("district"),
         )

    return df_dim_location

def write_location_data(df_dim_location, output_path):
    df_dim_location.write\
        .mode("overwrite")\
            .save(output_path)


if __name__ == '__main__':
    input_path = f"s3a://{BUCKET_NAME}/processed/oltp_db/{TABLE}"
    output_path = f"s3a://{BUCKET_NAME}/serving/dimensional_model/dim_location"

    spark_conn = create_spark_conn()
    df_dim_location = read_location_data(spark_conn,generate_location_key, input_path)
    write_location_data(df_dim_location, output_path)
