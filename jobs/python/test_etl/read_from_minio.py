from pyspark.sql import SparkSession

# Khởi tạo Spark Session với cấu hình MinIO
spark = SparkSession.builder \
    .appName("Read from MinIO") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.access.key", "fmbvirtueee") \
    .config("spark.hadoop.fs.s3a.secret.key", "Matkhauchung1@") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

# Đường dẫn đến file Parquet trong MinIO
minio_path = f"s3a://datalake/serving/dimensional_model/dim_location"

# Đọc DataFrame từ MinIO
try:
    df = spark.read.parquet(minio_path)
    print("✅ Đọc file thành công từ MinIO")

    # Hiển thị schema
    print("\nSchema của DataFrame:")
    df.printSchema()

    # Hiển thị dữ liệu
    print("\nDữ liệu trong DataFrame:")
    df.show(20)
    print(f"total records: {df.count()}")

except Exception as e:
    print(f"❌ Lỗi khi đọc từ MinIO: {str(e)}")
finally:
    spark.stop()