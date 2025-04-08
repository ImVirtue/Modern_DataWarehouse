import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id = "spark_clickhouse",
    default_args = {
        "owner": "Yusuf Ganiyu",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

write_to_clickhouse = SparkSubmitOperator(
    task_id="write_to_clickhouse",
    conn_id="spark-conn",
    application="./jobs/python/test_etl/write_to_clickhouse.py",
    packages='com.clickhouse:clickhouse-jdbc:0.4.6',
    repositories="https://repo1.maven.org/maven2/",
    executor_memory="1g",
    driver_memory="1g",
    num_executors=1,
    executor_cores=1,
    verbose=True,
    dag=dag
)

start >> write_to_clickhouse