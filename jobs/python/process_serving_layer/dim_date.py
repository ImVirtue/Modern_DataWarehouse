from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, month, quarter, year, date_format, dayofweek, explode, sequence, to_date
import datetime

# Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("CreateDimDateWithStringKey") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.access.key", "fmbvirtueee") \
        .config("spark.hadoop.fs.s3a.secret.key", "Matkhauchung1@") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

start_date = datetime.date(2024, 5, 1)
end_date = datetime.date(2025, 2, 28)

date_range_df = spark.sql(f"""
    SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as full_date
""")

dim_date_df = date_range_df.withColumn("date_key", date_format("full_date", "yyyyMMdd").cast("string")) \
    .withColumn("day", dayofmonth("full_date").cast("tinyint")) \
    .withColumn("month", month("full_date").cast("tinyint")) \
    .withColumn("quarter", quarter("full_date").cast("tinyint")) \
    .withColumn("year", year("full_date").cast("smallint")) \
    .withColumn("day_of_week", date_format("full_date", "EEEE")) \
    .withColumn("is_weekend", (dayofweek("full_date").isin(1, 7)).cast("tinyint")) \
    .select("date_key", "full_date", "day", "month", "quarter", "year", "day_of_week", "is_weekend")


output_path = "s3a://datalake/serving/dimensional_model/dim_date"

dim_date_df.write\
    .mode("overwrite")\
        .format("parquet")\
            .save(output_path)
