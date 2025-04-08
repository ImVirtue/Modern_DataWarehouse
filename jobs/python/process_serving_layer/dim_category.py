from pyspark.sql import SparkSession

BUCKET_NAME = "datalake"
TABLE = "categories"

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

def read_category_data(spark, input_path):
    df_oltp_category = spark.read.parquet(input_path)
    df_dim_category= df_oltp_category \
        .select(
        df_oltp_category['category_id'].alias("category_key"),
        df_oltp_category['category_name'].alias("category_name"),
        df_oltp_category['parent_category_id'].alias("parent_category_id")
    )

    return df_dim_category

def write_category_data(df_dim_category, output_path):
    df_dim_category.write\
        .mode("overwrite")\
            .save(output_path)


if __name__ == '__main__':
    input_path = f"s3a://{BUCKET_NAME}/processed/oltp_db/{TABLE}"
    output_path = f"s3a://{BUCKET_NAME}/serving/dimensional_model/dim_category"

    spark_conn = create_spark_conn()
    df_dim_category = read_category_data(spark_conn,input_path)
    write_category_data(df_dim_category, output_path)
