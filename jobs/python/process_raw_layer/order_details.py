from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

spark = SparkSession.builder \
    .appName("PostgresCount") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.5") \
    .config("spark.hadoop.fs.s3a.access.key", "fmbvirtueee") \
    .config("spark.hadoop.fs.s3a.secret.key", "Matkhauchung1@") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()


df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://main_database:5432/oltp_ecommerce_db") \
    .option("dbtable", "order_details") \
    .option("user", "admin") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .load()


df_with_date_column = df.withColumn("date_folder", date_format("created_time", "yyyy-MM-dd"))
minio_bucket = "datalake"
output_path = f"s3a://{minio_bucket}/raw/oltp_db/order_details"

df_with_date_column.write \
    .partitionBy("date_folder") \
    .mode("overwrite") \
    .format("parquet") \
    .save(output_path)

spark.stop()

