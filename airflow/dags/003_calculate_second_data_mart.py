from datetime import datetime, timedelta
import os
import pandas as pd
from sqlalchemy import create_engine, text
from airflow.configuration import conf
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col, when, lit, to_date, date_format, row_number, monotonically_increasing_id, unix_timestamp, to_timestamp, current_timestamp, split, element_at
from pyspark.sql.window import Window

default_args = {
    'owner': 'teambi',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Определяем DAG
dag = DAG(
    'calculate_items_mart_etl',
    default_args=default_args,
    description='ETL для витрины items_mart с использованием PySpark',
    schedule_interval='@daily',
    catchup=False,
    tags=['calculate mart', 'postgres']
)


def create_items_mart_table():
    main_db_hook = PostgresHook(postgres_conn_id='postgres_main_gp')
    connection = main_db_hook.get_conn()
    cursor = connection.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS items_mart (
                year INT,
                month INT,
                week_in_month INT,
                day INT,
                created_date DATE,
                city TEXT,
                store_id INT,
                store_name TEXT,
                store_address TEXT,
                item_category TEXT,
                item_id INT,
                item_title TEXT,
                items_qty_gross NUMERIC(15,2),
                items_qty_canceled NUMERIC(15,2),
                items_qty_net NUMERIC(15,2),
                orders_with_item INT,
                orders_with_item_cancellation INT,
                revenue_before_discounts NUMERIC(15,2),
                revenue_after_item_discount NUMERIC(15,2),
                revenue_after_order_discount NUMERIC(15,2)
            );
        """)
        connection.commit()
        print(f"Создалась таблица items_mart")
    except Exception as e:
        print(f"Ошибка при создании таблицы items_mart: {e}")
        raise
    finally:
        cursor.close()
        connection.close()
    

def process_items_mart(**context):
    # Hook для основной БД приложения
    main_db_hook = PostgresHook(postgres_conn_id='postgres_main_gp')
    connection = main_db_hook.get_conn()
    cursor = connection.cursor()
    try:
        # Загружаем таблицы
        order_to_item_df = cursor.execute("""
            TRUNCATE TABLE items_mart;
            WITH replacement_ids AS (
                SELECT DISTINCT
                    order_id AS rep_order_id,
                    item_replaced_id AS replacement_item_id
                FROM order_to_item
                WHERE COALESCE(item_replaced_id, 0) != 0
            ),
            order_item_stage AS (
                SELECT 
                    oti.order_id,
                    o.created_at,
                    o.created_at::DATE AS created_date,
                    COALESCE(o.delivery_city, '') AS city,
                    o.store_id,
                    s.store_name,
                    s.store_address,
                    c.item_category,
                    i.item_id,
                    i.item_title,
                    CASE 
                        WHEN r.replacement_item_id IS NOT NULL THEN 0.0 
                        ELSE COALESCE(oti.item_quantity, 0.0) 
                    END AS gross_quantity,
                    COALESCE(oti.item_canceled_quantity, 0.0) AS canceled_quantity,
                    GREATEST(COALESCE(oti.item_quantity, 0.0) - COALESCE(oti.item_canceled_quantity, 0.0), 0.0) AS net_quantity,
                    CASE WHEN COALESCE(oti.item_canceled_quantity, 0) > 0 THEN 1 ELSE 0 END AS has_item_cancellation,
                    (GREATEST(COALESCE(oti.item_quantity, 0.0) - COALESCE(oti.item_canceled_quantity, 0.0), 0.0)) * 
                    COALESCE(i.item_price, 0.0) AS amount_before_discounts,
                    (GREATEST(COALESCE(oti.item_quantity, 0.0) - COALESCE(oti.item_canceled_quantity, 0.0), 0.0)) * 
                    COALESCE(i.item_price, 0.0) * 
                    (1.0 - COALESCE(oti.item_discount, 0.0) / 100.0) AS amount_after_item_discount,
                    (GREATEST(COALESCE(oti.item_quantity, 0.0) - COALESCE(oti.item_canceled_quantity, 0.0), 0.0)) * 
                    COALESCE(i.item_price, 0.0) * 
                    (1.0 - COALESCE(oti.item_discount, 0.0) / 100.0) * 
                    (1.0 - COALESCE(o.order_discount, 0.0) / 100.0) AS amount_after_order_discount
                FROM order_to_item oti
                INNER JOIN orders o ON oti.order_id = o.order_id
                LEFT JOIN items i ON oti.item_id = i.item_id
                LEFT JOIN item_category c ON i.item_category_id = c.item_category_id
                LEFT JOIN store s ON o.store_id = s.store_id
                LEFT JOIN replacement_ids r ON oti.order_id = r.rep_order_id 
                    AND oti.item_id = r.replacement_item_id
            )
            INSERT INTO items_mart
            SELECT 
                EXTRACT(YEAR FROM created_date) AS year,
                EXTRACT(MONTH FROM created_date) AS month,
                FLOOR((EXTRACT(DAY FROM created_date) - 1) / 7) + 1 AS week_in_month,
                EXTRACT(DAY FROM created_date) AS day,
                created_date,
                city,
                store_id,
                store_name,
                store_address,
                item_category,
                item_id,
                item_title,
                SUM(gross_quantity) AS items_qty_gross,
                SUM(canceled_quantity) AS items_qty_canceled,
                SUM(net_quantity) AS items_qty_net,
                COUNT(DISTINCT order_id) AS orders_with_item,
                COUNT(DISTINCT CASE WHEN has_item_cancellation = 1 THEN order_id END) AS orders_with_item_cancellation,
                SUM(amount_before_discounts) AS revenue_before_discounts,
                SUM(amount_after_item_discount) AS revenue_after_item_discount,
                SUM(amount_after_order_discount) AS revenue_after_order_discount
            FROM order_item_stage
            GROUP BY 
                EXTRACT(YEAR FROM created_date),
                EXTRACT(MONTH FROM created_date),
                FLOOR((EXTRACT(DAY FROM created_date) - 1) / 7) + 1,
                EXTRACT(DAY FROM created_date),
                created_date,
                city,
                store_id,
                store_name,
                store_address,
                item_category,
                item_id,
                item_title;
        """)
        connection.commit()
    except Exception as e:
        print(f"Ошибка при получении или записи данных в витрину items_mart: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

# Определение задач DAG
# Создаем задачу
create_items_mart_table = PythonOperator(
    task_id='create_items_mart_table',
    python_callable=create_items_mart_table,
    dag=dag,
)

process_items_mart = PythonOperator(
    task_id='process_items_mart',
    python_callable=process_items_mart,
    dag=dag,
)

create_items_mart_table >> process_items_mart
