from pyspark.sql import SparkSession

BUCKET_NAME = "datalake"
TABLE = "products"

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

def read_product_data(spark, input_path):
    df_oltp_product = spark.read.parquet(input_path)
    df_dim_product= df_oltp_product \
        .select(
        df_oltp_product['product_id'].alias("product_key"),
        df_oltp_product['product_name'].alias("product_name"),
        df_oltp_product['brand'].alias("brand"),
        df_oltp_product['price'].alias("current_price"),
        df_oltp_product['cost_price'].alias("cost"),
        df_oltp_product['stock_quantity'].alias("stock_quantity")
    )

    return df_dim_product

def write_product_data(df_dim_product, output_path):
    df_dim_product.write\
        .mode("overwrite")\
            .save(output_path)


if __name__ == '__main__':
    input_path = f"s3a://{BUCKET_NAME}/processed/oltp_db/{TABLE}"
    output_path = f"s3a://{BUCKET_NAME}/serving/dimensional_model/dim_products"

    spark_conn = create_spark_conn()
    df_dim_product = read_product_data(spark_conn,input_path)
    write_product_data(df_dim_product, output_path)
