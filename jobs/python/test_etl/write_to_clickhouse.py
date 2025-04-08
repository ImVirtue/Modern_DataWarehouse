from pyspark.sql import SparkSession
from datetime import datetime

def generate_sample_data(spark):
    data = [
        (1, "Product A", 100.50, datetime.now()),
        (2, "Product B", 200.75, datetime.now()),
        (3, "Product C", 300.25, datetime.now())
    ]
    columns = ["product_id", "product_name", "price", "created_at"]
    return spark.createDataFrame(data, columns)

def main():
    spark = SparkSession.builder \
        .appName("ClickHouseDataLoader") \
        .config("spark.jars.packages", "com.clickhouse:clickhouse-jdbc:0.4.6") \
        .config("spark.jars.repositories", "https://repo1.maven.org/maven2/") \
        .getOrCreate()

    df = generate_sample_data(spark)
    df.show()

    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:clickhouse://clickhouse:8123/data_warehouse") \
        .option("dbtable", "fact_orders") \
        .option("user", "admin") \
        .option("password", "Matkhauchung1@") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("createTableOptions", "ENGINE = MergeTree() ORDER BY tuple()") \
        .mode("append") \
        .save()

    print(f"Đã ghi dữ liệu vào thành công!")

if __name__ == "__main__":
    main()