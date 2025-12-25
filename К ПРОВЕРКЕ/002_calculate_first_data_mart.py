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
    'calculate_mart_etl',
    default_args=default_args,
    description='ETL для витрины orders_mart с использованием PySpark',
    schedule_interval='@daily',
    catchup=False,
    tags=['calculate mart', 'postgres']
)


def create_mart_table():
    main_db_hook = PostgresHook(postgres_conn_id='postgres_main_gp')
    connection = main_db_hook.get_conn()
    cursor = connection.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders_mart (
                dt DATE,
                year INTEGER,
                month INTEGER,
                day INTEGER,
                city VARCHAR,
                store_id INTEGER,
                store_name VARCHAR,
                turnover DECIMAL(15,2),
                revenue DECIMAL(15,2),
                profit DECIMAL(15,2),
                delivery_cost DECIMAL(15,2),
                orders_created INTEGER,
                orders_delivered INTEGER,
                orders_canceled INTEGER,
                cancels_after_delivery INTEGER,
                cancels_service_side INTEGER,
                buyers INTEGER,
                avg_check DECIMAL(15,2),
                orders_per_buyer DECIMAL(10,2),
                revenue_per_buyer DECIMAL(15,2),
                orders_with_driver_change INTEGER,
                active_couriers INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );   
        """)
        connection.commit()
        print(f"Создалась таблица orders_mart")
    except Exception as e:
        print(f"Ошибка при создании таблицы orders_mart: {e}")
        raise
    finally:
        cursor.close()
        connection.close()
    

def process_orders_mart(**context):
    # Hook для основной БД приложения
    main_db_hook = PostgresHook(postgres_conn_id='postgres_main_gp')
    connection = main_db_hook.get_conn()
    cursor = connection.cursor()
    try:
        # Загружаем таблицы
        order_to_item_df = cursor.execute("""
            TRUNCATE TABLE orders_mart;
            WITH order_items AS (
              SELECT
                oti.order_id,
                o.created_at,
                o.paid_at,
                o.canceled_at,
                o.order_discount,
                o.order_cancellation_reason,
                o.user_id,
                o.store_id,

                -- фактическое кол-во
                GREATEST(
                  COALESCE(oti.item_quantity, 0.0) - COALESCE(oti.item_canceled_quantity, 0.0),
                  0.0
                ) AS qty_true,

                -- фактическая цена
                CASE
                  WHEN COALESCE(oti.item_replaced_id, 0) <> 0 THEN ir.item_price
                  ELSE i.item_price
                END AS unit_price_effective,

                -- скидка на позицию
                COALESCE(oti.item_discount, 0.0) AS item_discount
              FROM order_to_item oti
              JOIN orders o ON o.order_id = oti.order_id
              JOIN items i ON i.item_id = oti.item_id
              LEFT JOIN items ir ON ir.item_id = oti.item_replaced_id
            ),

            order_money AS (
              SELECT
                order_id,
                CAST(created_at AS DATE) AS dt,
                user_id,
                store_id,
                paid_at,
                canceled_at,
                order_cancellation_reason,

                -- Оборот
                SUM(
                  qty_true * unit_price_effective *
                  (1 -COALESCE(item_discount,0.0)/ 100) *
                  (1 - COALESCE(order_discount,0.0) / 100))
                  AS turnover_amount,

                -- Выручка
                SUM(
                  CASE WHEN paid_at IS NOT NULL THEN
                    qty_true * unit_price_effective *
                    (1 -COALESCE(item_discount,0.0)/ 100) *
                    (1 - COALESCE(order_discount,0.0) / 100)
                  ELSE 0 END
                ) AS revenue_amount
              FROM order_items
              GROUP BY 1,2,3,4,5,6,7
            ),

            -- факт доставки + число курьеров в заказе (для смены)
            delivery_agg AS (
              SELECT
                order_id,
                MAX(delivered_at) AS delivered_at,
                COUNT(DISTINCT driver_id) AS drivers_cnt
              FROM delivery
              GROUP BY 1
            ),

            --город и название для разрезов витрины
            store_dim AS (
              SELECT
                store_id,
                store_name,
                store_city AS city
              FROM store
            ),

            -- 5) деньги + доставка и магазин + считаем прибыль
            orders_base AS (
              SELECT
                om.order_id,
                om.dt,
                om.store_id,
                om.user_id,
                om.paid_at,
                om.canceled_at,
                om.order_cancellation_reason,
                da.delivered_at,

                -- расходы на доставку
                COALESCE(o.delivery_cost, 0.0) AS delivery_cost,

                -- сколько курьеров было у заказа
                COALESCE(da.drivers_cnt, 0) AS drivers_cnt,
                sd.store_name,
                sd.city,
                om.turnover_amount,
                om.revenue_amount,

                -- Прибыль
                (om.revenue_amount - COALESCE(o.delivery_cost,0.0)) AS profit_amount
              FROM order_money om
              JOIN orders o ON o.order_id = om.order_id
              LEFT JOIN delivery_agg da ON da.order_id = om.order_id
              LEFT JOIN store_dim sd ON sd.store_id = om.store_id
            ),

            --активные курьеры
            active_couriers AS (
              SELECT
                CAST(o.created_at AS DATE) AS dt,
                o.store_id,
                sd.city AS city,
                COUNT(DISTINCT d.driver_id) AS active_couriers
              FROM orders o
              JOIN delivery d ON d.order_id = o.order_id
              JOIN store_dim sd ON sd.store_id = o.store_id
              WHERE d.delivered_at IS NOT NULL
              GROUP BY 1,2,3
            )
            INSERT INTO orders_mart
            SELECT
              ob.dt AS dt,
              EXTRACT(YEAR FROM ob.dt) AS year,
              EXTRACT(MONTH FROM ob.dt) AS month,
              EXTRACT(DAY FROM ob.dt) AS day,
              ob.city AS city,
              ob.store_id AS store_id,
              ob.store_name AS store_name,

                -- Финансы
              SUM(ob.turnover_amount) AS turnover,
              SUM(ob.revenue_amount) AS revenue,
              SUM(ob.profit_amount) AS profit,
              SUM(ob.delivery_cost) AS delivery_cost,

                -- Статусы заказов
              COUNT(DISTINCT ob.order_id) AS orders_created,
              COUNT(DISTINCT CASE WHEN ob.delivered_at IS NOT NULL THEN ob.order_id END) AS orders_delivered,
              COUNT(DISTINCT CASE WHEN ob.canceled_at IS NOT NULL THEN ob.order_id END) AS orders_canceled,

                -- Отмены после доставки
              COUNT(DISTINCT CASE
                WHEN ob.canceled_at IS NOT NULL AND ob.delivered_at IS NOT NULL AND ob.canceled_at > ob.delivered_at
                THEN ob.order_id END) AS cancels_after_delivery,

                -- Отмены по ошибке сервиса
              COUNT(DISTINCT CASE
                WHEN ob.canceled_at IS NOT NULL AND ob.order_cancellation_reason IN ('Ошибка приложения','Проблемы с оплатой')
                THEN ob.order_id END) AS cancels_service_side,

              COUNT(DISTINCT ob.user_id) AS buyers,

              -- Средний чек
              CASE
                WHEN COUNT(DISTINCT CASE WHEN ob.paid_at IS NOT NULL THEN ob.order_id END) = 0 THEN 0
                ELSE SUM(ob.revenue_amount) /
                     COUNT(DISTINCT CASE WHEN ob.paid_at IS NOT NULL THEN ob.order_id END)
              END AS avg_check,

              -- Заказов на покупателя
              CASE WHEN COUNT(DISTINCT ob.user_id)=0 THEN 0
                   ELSE COUNT(DISTINCT ob.order_id)/COUNT(DISTINCT ob.user_id)
              END AS orders_per_buyer,

              -- Выручка на покупателя
              CASE WHEN COUNT(DISTINCT ob.user_id)=0 THEN 0
                   ELSE SUM(ob.revenue_amount)/COUNT(DISTINCT ob.user_id)
              END AS revenue_per_buyer,

                -- Заказы со сменой курьера
              COUNT(DISTINCT CASE WHEN ob.drivers_cnt > 1 THEN ob.order_id END) AS orders_with_driver_change,
                -- Активные курьеры
              COALESCE(ac.active_couriers, 0) AS active_couriers
            FROM orders_base ob
            LEFT JOIN active_couriers ac
              ON ob.dt = ac.dt
             AND ob.store_id = ac.store_id
             AND ob.city = ac.city
            GROUP BY
              ob.dt, EXTRACT(YEAR FROM ob.dt), EXTRACT(MONTH FROM ob.dt), EXTRACT(DAY FROM ob.dt),
              ob.city, ob.store_id, ob.store_name,
              COALESCE(ac.active_couriers, 0)
            """)
        connection.commit()
    except Exception as e:
        print(f"Ошибка при получении или записи данных в витрину orders_mart: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

# Определение задач DAG
# Создаем задачу
create_mart_table = PythonOperator(
    task_id='create_mart_table',
    python_callable=create_mart_table,
    dag=dag,
)

process_orders_mart = PythonOperator(
    task_id='process_orders_mart',
    python_callable=process_orders_mart,
    dag=dag,
)

create_mart_table >> process_orders_mart
