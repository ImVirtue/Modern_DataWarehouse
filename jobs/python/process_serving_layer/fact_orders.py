from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import  StringType
import os
import zlib

def create_spark_conn():
    spark = SparkSession.builder \
        .appName("Fact order") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.access.key", "fmbvirtueee") \
        .config("spark.hadoop.fs.s3a.secret.key", "Matkhauchung1@") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    return spark

def load_all_tables_from_processed_layer(spark, tables):
    dataframe = {}
    input_path = "s3a://datalake/processed/oltp_db/"
    for table in tables:
        table_path = os.path.join(input_path, table) + "/"
        try:
            df = spark.read.parquet(table_path)
            dataframe[table] = df
        except Exception as e:
            print(f"Error when reading data from MinIO: {str(e)}")

    return dataframe

def generate_time_key(date_data):
    date_string = str(date_data)
    split = date_string.split('-')
    time_key = split[0] + split[1] + split[2]
    return str(time_key)


def generate_location_key(location):
    byte_data = location.encode('utf-8')
    location_key = zlib.crc32(byte_data) & 0xffffffff
    return str(location_key)


if __name__ == '__main__':
    #Initilization
    tables = ["orders", "order_details", "customers", "products", "categories", "shipments"]
    spark = create_spark_conn()
    dataframe = load_all_tables_from_processed_layer(spark, tables)
    generate_time_key_udf = udf(generate_time_key, StringType())
    generate_location_key_udf = udf(generate_location_key, StringType())

    #get dataframes of table
    od_df = dataframe['orders']
    odt_df = dataframe['order_details']
    cus_df = dataframe['customers']
    prd_df = dataframe['products']
    ct_df = dataframe['categories']
    s_df = dataframe['shipments']

    #Process fact_orders
    fact_orders_df = (
        odt_df.alias("odt")
            .join(od_df.alias("od"), col("odt.order_id") == col("od.order_id"), "inner")
            .join(cus_df.alias("cus"), col("od.customer_id") == col("cus.customer_id"), "inner")
            .join(prd_df.alias("prd"), col("odt.product_id") == col("prd.product_id"), "inner")
            .join(ct_df.alias("ct"), col("prd.category_id") == col("ct.category_id"), "inner")
            .join(s_df.alias("s"), col("od.order_id") == col("s.order_id"), "inner")
                .withColumn("time_id", generate_time_key_udf(col("odt.date_folder")))
                .withColumn("location_id", generate_location_key_udf(col("cus.location")))
                    .select(
                        col("od.order_id"),
                        col("odt.order_detail_id"),
                        col("time_id"),
                        col("location_id"),
                        col("cus.customer_id"),
                        col("prd.product_id"),
                        col("ct.category_id"),
                        col("s.shipment_id"),
                        col("odt.quantity"),
                        col("prd.price"),
                        col("prd.cost_price"),
                        col("od.order_status"),
                        col("od.payment_status")
                    )
    )

    minio_bucket = "datalake"
    output_path = f"s3a://{minio_bucket}/serving/dimensional_model/fact_orders"

    fact_orders_df.write\
        .partitionBy("time_id")\
            .mode("overwrite")\
                .format("parquet")\
                    .save(output_path)









