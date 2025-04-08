import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id = "test_dag",
    default_args = {
        "owner" : "Nguyen Tuan Duc",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)

processing_serving_layer = SparkSubmitOperator(
    task_id = "processing_serving_layer",
    conn_id="spark-conn",
    application="./jobs/python/process_serving_layer/dim_products.py",
    packages = "org.apache.hadoop:hadoop-aws:3.3.4",
    repositories="https://repo1.maven.org/maven2/",
    # packages = "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.5", # ✅ Correct path
    # ✅ Correct path
    executor_memory="1g",
    driver_memory="1g",
    num_executors=1,
    executor_cores=2,
    verbose=True,
    dag=dag
)

processing_dwh = SparkSubmitOperator(
    task_id = "processing_dwh",
    conn_id="spark-conn",
    application="./jobs/python/etl_to_datawarehouse(clickhouse)/dim_products.py",
    packages = "org.apache.hadoop:hadoop-aws:3.3.4,com.clickhouse:clickhouse-jdbc:0.4.6",
    repositories="https://repo1.maven.org/maven2/",
    # packages = "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.5", # ✅ Correct path
    # ✅ Correct path
    executor_memory="1g",
    driver_memory="1g",
    num_executors=1,
    executor_cores=2,
    verbose=True,
    dag=dag
)

processing_serving_layer >> processing_dwh

