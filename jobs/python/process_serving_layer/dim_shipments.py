from pyspark.sql import SparkSession

BUCKET_NAME = "datalake"
TABLE = "shipments"

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

def read_shipment_data(spark, input_path):
    df_oltp_shipments = spark.read.parquet(input_path)
    df_dim_shipments= df_oltp_shipments \
        .select(
        df_oltp_shipments['shipment_id'].alias("shipment_key"),
        df_oltp_shipments['carrier'].alias("carrier"),
        df_oltp_shipments['status'].alias("status")
    )

    return df_dim_shipments

def write_shipment_data(df_dim_shipments, output_path):
    df_dim_shipments.write\
        .mode("overwrite")\
            .save(output_path)


if __name__ == '__main__':
    input_path = f"s3a://{BUCKET_NAME}/processed/oltp_db/{TABLE}"
    output_path = f"s3a://{BUCKET_NAME}/serving/dimensional_model/dim_shipments"

    spark_conn = create_spark_conn()
    df_dim_shipments = read_shipment_data(spark_conn,input_path)
    write_shipment_data(df_dim_shipments, output_path)
