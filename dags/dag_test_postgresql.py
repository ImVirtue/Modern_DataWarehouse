import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id = "spark_postgresql",
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

read_from_postgresql = SparkSubmitOperator(
    task_id="read_from_postgresql",
    conn_id="spark-conn",
    application="./jobs/python/read_from_postgresql.py",
    packages="org.postgresql:postgresql:42.7.5",  # ✅ Correct path
    executor_memory="1g",
    driver_memory="1g",
    num_executors=1,
    executor_cores=1,
    verbose=True,
    dag=dag
)

start >> read_from_postgresql