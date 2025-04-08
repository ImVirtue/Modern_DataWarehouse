from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Initialize Spark Session with S3 support
spark = SparkSession.builder \
    .appName("MinIO CSV Data Write") \
    .config("spark.hadoop.fs.s3a.access.key", "fmbvirtueee") \
    .config("spark.hadoop.fs.s3a.secret.key", "Matkhauchung1@") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

# Input CSV file path (local or any supported filesystem)
# Read CSV file (schema inference example)
data = [("Alex TPD", 25), ("Nguyen Tuan Duc", 30), ("Trung Duong Leo", 35)]
df = spark.createDataFrame(data, ["name", "age"])
df.show()

# Method 2: With explicit schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
df = spark.createDataFrame(data, schema)

# MinIO output configuration
minio_bucket = "datalake"
output_path = f"s3a://{minio_bucket}/output-data/"

# Write to MinIO in Parquet format
df.write\
    .mode("overwrite")\
    .format("parquet")\
    .save(output_path)

print(f"Data successfully written to MinIO at: {output_path}")

# Stop Spark session
spark.stop()