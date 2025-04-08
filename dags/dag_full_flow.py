from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "Nguyen Tuan Duc",
    "start_date": days_ago(1)
}

def create_spark_task(task_id, app_path):
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark-conn",
        application=app_path,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.5,com.clickhouse:clickhouse-jdbc:0.4.6",
        repositories="https://repo1.maven.org/maven2/",
        executor_memory="1g",
        driver_memory="1g",
        num_executors=1,
        executor_cores=1,
        verbose=True,
    )

with DAG(
    dag_id="full_flow",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Full pipeline: OLTP → Serving → DWH"
) as dag:

    with TaskGroup(group_id="extract_from_oltp") as extract_from_oltp:
        extract_categories = create_spark_task("extract_categories", "./jobs/python/process_raw_layer/categories.py")
        extract_coupon = create_spark_task("extract_coupon", "./jobs/python/process_raw_layer/coupon.py")
        extract_customer_coupon = create_spark_task("extract_customer_coupon", "./jobs/python/process_raw_layer/customer_coupon.py")
        extract_customer_support = create_spark_task("extract_customer_support", "./jobs/python/process_raw_layer/customer_support.py")
        extract_customers = create_spark_task("extract_customers", "./jobs/python/process_raw_layer/customers.py")
        extract_loyalty = create_spark_task("extract_loyalty", "./jobs/python/process_raw_layer/loyalty.py")
        extract_order_details = create_spark_task("extract_order_details", "./jobs/python/process_raw_layer/order_details.py")
        extract_orders = create_spark_task("extract_orders", "./jobs/python/process_raw_layer/orders.py")
        extract_products = create_spark_task("extract_products", "./jobs/python/process_raw_layer/products.py")
        extract_shipments = create_spark_task("extract_shipments", "./jobs/python/process_raw_layer/shipments.py")
        extract_vendors = create_spark_task("extract_vendors", "./jobs/python/process_raw_layer/vendors.py")

    with TaskGroup(group_id="etl_to_serving_layer") as etl_to_serving_layer:
        dim_category = create_spark_task("dim_category", "./jobs/python/process_serving_layer/dim_category.py")
        dim_customers = create_spark_task("dim_customers", "./jobs/python/process_serving_layer/dim_customers.py")
        dim_date = create_spark_task("dim_date", "./jobs/python/process_serving_layer/dim_date.py")
        dim_location = create_spark_task("dim_location", "./jobs/python/process_serving_layer/dim_location.py")
        dim_products = create_spark_task("dim_products", "./jobs/python/process_serving_layer/dim_products.py")
        dim_shipments = create_spark_task("dim_shipments", "./jobs/python/process_serving_layer/dim_shipments.py")
        dim_vendors = create_spark_task("dim_vendors", "./jobs/python/process_serving_layer/dim_vendors.py")
        fact_orders = create_spark_task("fact_orders", "./jobs/python/process_serving_layer/fact_orders.py")

    with TaskGroup(group_id="etl_to_datawarehouse") as etl_to_dwh:
        dwh_category = create_spark_task("dwh_category", "./jobs/python/etl_to_datawarehouse(clickhouse)/dim_category.py")
        dwh_customers = create_spark_task("dwh_customers", "./jobs/python/etl_to_datawarehouse(clickhouse)/dim_customers.py")
        dwh_date = create_spark_task("dwh_date", "./jobs/python/etl_to_datawarehouse(clickhouse)/dim_date.py")
        dwh_location = create_spark_task("dwh_location", "./jobs/python/etl_to_datawarehouse(clickhouse)/dim_location.py")
        dwh_products = create_spark_task("dwh_products", "./jobs/python/etl_to_datawarehouse(clickhouse)/dim_products.py")
        dwh_shipments = create_spark_task("dwh_shipments", "./jobs/python/etl_to_datawarehouse(clickhouse)/dim_shipments.py")
        dwh_vendors = create_spark_task("dwh_vendors", "./jobs/python/etl_to_datawarehouse(clickhouse)/dim_vendors.py")
        dwh_fact_orders = create_spark_task("dwh_fact_orders", "./jobs/python/etl_to_datawarehouse(clickhouse)/fact_orders.py")

    # Set task flow: raw → serving → dwh
    extract_from_oltp >> etl_to_serving_layer >> etl_to_dwh
