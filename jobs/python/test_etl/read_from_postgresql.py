from pyspark.sql import SparkSession

# Initialize SparkSession with Maven coordinates
spark = SparkSession.builder \
    .appName("PostgresTest") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.5") \
    .getOrCreate()


# Read from PostgreSQL
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://main_database:5432/oltp_ecommerce_db") \
    .option("dbtable", "categories") \
    .option("user", "admin") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.show()