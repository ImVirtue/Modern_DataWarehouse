from pyspark.sql import SparkSession
from pyspark.sql.functions import  col

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

def read_data_from_source(spark):
    customer_path = f"s3a://{BUCKET_NAME}/processed/oltp_db/customers"
    loyalty_path = f"s3a://{BUCKET_NAME}/processed/oltp_db/loyalty_program"

    df_oltp_customer = spark.read.parquet(customer_path)
    df_oltp_loyalty_program = spark.read.parquet(loyalty_path)

    df_dim = df_oltp_customer.alias("cus") \
        .join(df_oltp_loyalty_program.alias("lp"), col("cus.customer_id") == col("lp.customer_id"), "inner")\
            .select(
            col("cus.customer_id").alias("customer_key"),
            col("cus.customer_name").alias("customer_name"),
            col("cus.customer_email").alias("customer_email"),
            col("cus.customer_phone").alias("customer_phone"),
            col("cus.is_member").alias("is_member"),
            col("cus.customer_status").alias("customer_status"),
            col("lp.points").alias("loyalty_points")
        )

    return df_dim

def write_data_to_destiantion(df_dim, output_path):
    df_dim.write\
        .mode("overwrite")\
            .save(output_path)


if __name__ == '__main__':
    # input_path = f"s3a://{BUCKET_NAME}/processed/oltp_db/{TABLE}"
    output_path = f"s3a://{BUCKET_NAME}/serving/dimensional_model/dim_customer"

    spark_conn = create_spark_conn()
    df_dim_category = read_data_from_source(spark_conn)
    write_data_to_destiantion(df_dim_category, output_path)
