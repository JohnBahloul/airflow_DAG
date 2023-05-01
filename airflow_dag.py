from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import dbt.main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'ecommerce_data_pipeline',
    default_args=default_args,
    description='Extract, load, and transform customer order data for an e-commerce website',
    schedule_interval=timedelta(days=1),
    catchup=False
)

def extract_orders():
    orders = []
    with open('/path/to/orders.csv') as f:
        for line in f:
            fields = line.strip().split(',')
            orders.append(fields)
    return orders

def load_orders():
    orders = extract_orders()
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
    postgres_hook.run("CREATE TABLE IF NOT EXISTS orders (order_id INT, customer_id INT, product_id INT, price DECIMAL, date DATE);")
    for order in orders:
        postgres_hook.run("INSERT INTO orders VALUES (%s, %s, %s, %s, %s);", parameters=order)

def transform_orders():
    dbt.main(["run"])

extract_task = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_orders,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_orders',
    python_callable=load_orders,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_orders',
    python_callable=transform_orders,
    dag=dag,
)

create_orders_table_task = PostgresOperator(
    task_id='create_orders_table',
    postgres_conn_id='postgres_conn_id',
    sql="CREATE TABLE IF NOT EXISTS orders (order_id INT, customer_id INT, product_id INT, price DECIMAL, date DATE);",
    dag=dag
)

extract_task >> load_task
load_task >> create_orders_table_task
create_orders_table_task >> transform_task
