from pyspark.sql import SparkSession

BUCKET_NAME = "datalake"
TABLE = "vendors"

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

def read_vendor_data(spark, input_path):
    df_oltp_vendor = spark.read.parquet(input_path)
    df_dim_vendors= df_oltp_vendor \
        .select(
        df_oltp_vendor['vendor_id'].alias("vendor_key"),
        df_oltp_vendor['name'].alias("name"),
        df_oltp_vendor['email'].alias("email"),
        df_oltp_vendor['representative_phone'].alias("representative_phone"),
        df_oltp_vendor['type'].alias("type"),
        df_oltp_vendor['expense'].alias("expense"),
        df_oltp_vendor['revenue'].alias("revenue"),
    )

    return df_dim_vendors

def write_vendor_data(df_dim_vendors, output_path):
    df_dim_vendors.write\
        .mode("overwrite")\
            .save(output_path)


if __name__ == '__main__':
    input_path = f"s3a://{BUCKET_NAME}/processed/oltp_db/{TABLE}"
    output_path = f"s3a://{BUCKET_NAME}/serving/dimensional_model/dim_vendors"

    spark_conn = create_spark_conn()
    df_dim_vendors = read_vendor_data(spark_conn,input_path)
    write_vendor_data(df_dim_vendors, output_path)
