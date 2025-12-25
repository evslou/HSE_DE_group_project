"""
DAG для инкрементальной загрузки данных из Parquet-файлов в PostgreSQL.
Обрабатывает только новые файлы, имена которых ещё не сохранены в таблице processed_files.
"""
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

# Аргументы DAG по умолчанию
default_args = {
    'owner': 'teambi',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Определение DAG
dag = DAG(
    'parquet_to_postgres_loader',
    default_args=default_args,
    description='Инкрементальная загрузка Parquet в Postgres',
    schedule_interval=timedelta(hours=6),  # Запуск каждые 6 часов
    catchup=False,
    tags=['loader', 'postgres'],
)

# Константы
DATA_DIR = "/opt/airflow/data"  # Директория с parquet-файлами
def get_new_parquet_files(**context):
    """
    Задача 1: Находит новые Parquet-файлы.
    Использует метабазу Airflow (postgres-airflow) для таблицы processed_files
    """
    
    # 1. Получаем строку подключения к метабазе Airflow из конфигурации
    sql_alchemy_conn = conf.get('database', 'sql_alchemy_conn')
    
    # 2. Создаем engine для подключения к метабазе
    engine = create_engine(sql_alchemy_conn, future=True)
    
    try:
        with engine.connect() as connection:
            # Получаем список уже обработанных файлов
            result = connection.execute(text("SELECT file_name FROM processed_files;"))
            processed_files = {row[0] for row in result}
            
            # Получаем список всех parquet-файлов в директории
            all_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.parquet')]
            
            # Определяем новые файлы
            new_files = [f for f in all_files if f not in processed_files]
            
            # Передаём список новых файлов в следующую задачу через XCom
            context['ti'].xcom_push(key='new_files', value=new_files)
            
            print(f"Найдено файлов в директории {DATA_DIR}: {len(all_files)}")
            print(f"Уже обработано: {len(processed_files)}")
            print(f"Новых файлов для обработки: {len(new_files)}")
            
            if not new_files:
                print("Новых файлов для обработки не найдено.")
                
    except Exception as e:
        print(f"Ошибка при работе с метабазой Airflow: {e}")
        raise
        
def process_and_load_data(**context):
    """
    Задача 2: Обрабатывает новые файлы и загружает данные в PostgreSQL.
    Выполняет трансформацию для каждой таблицы и инкрементальную вставку.
    """
    # Получаем список новых файлов из предыдущей задачи
    ti = context['ti']
    new_files = ti.xcom_pull(task_ids='get_new_files', key='new_files')

    if not new_files:
        print("Нет новых файлов для обработки. Задача завершена.")
        return

    spark = SparkSession.builder \
        .appName("AirflowParquetLoader") \
        .config("spark.master", "local") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.network.timeout", "1200s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .getOrCreate()

    # Hook для основной БД приложения
    main_db_hook = PostgresHook(postgres_conn_id='postgres_main_gp')
    
    # Hook для БД Airflow (для обновления processed_files)
    sql_alchemy_conn = conf.get('database', 'sql_alchemy_conn')
    engine = create_engine(sql_alchemy_conn, future=True)
    
    try:
        for file_name in new_files:
            print(f"Начинаем обработку файла: {file_name}")
            file_path = os.path.join(DATA_DIR, file_name)

            # Чтение Parquet-файла
            df = spark.read.parquet(file_path)

            # --- Трансформация данных для каждой таблицы ---
            # 1. Таблица users
            users_df = df.select(
                col("user_id").cast("int"),
                col("user_phone").cast("string")
            ).distinct().orderBy("user_id")
            
            # 2. Таблица driver
            driver_df = df.select(
                col("driver_id").cast("int"),
                col("driver_phone").cast("string")
            ).distinct().orderBy("driver_id")

            # 3. Таблица store
            # Предполагаем, что store_name, store_city можно получить или задать по умолчанию
            store_df = df.select(
                col("store_id").cast("int"),
                col("store_address").cast("string")
            ).distinct()
            # Добавляем недостающие колонки (пример)
            store_df = store_df.withColumn("store_city", element_at(split(col("store_address"), ", "), 2)) \
                               .withColumn("store_name", element_at(split(col("store_address"), ", "), 1))
                               
            # 4. Таблица payment_type
            payment_types = df.select("payment_type").distinct().collect()
            payment_type_dict = []
            payment_id = 1

            for row in payment_types:
                payment_type = row['payment_type']
                # Проверяем, существует ли уже такой тип платежа
                if not any(pt[1] == payment_type for pt in payment_type_dict):
                    payment_type_dict.append((payment_id, payment_type))
                    payment_id += 1

            # Создаём DataFrame для payment_type
            payment_type_df = spark.createDataFrame(
                payment_type_dict,
                ["payment_type_id", "payment_type"]
            )

            # 5. Таблица item_category
            item_category_df = df.select(
                col("item_category").cast("string").alias("item_category")
            ).distinct()
            window_spec = Window.orderBy("item_category")
            item_category_df = item_category_df.withColumn(
                "item_category_id",
                row_number().over(window_spec)
            ).select("item_category_id", "item_category")

            # 6. Таблица orders
            orders_df = df.select(
                col("order_id").cast("int"),
                to_date(col("created_at")).alias("created_at"),
                to_date(col("paid_at")).alias("paid_at"),
                to_date(col("canceled_at")).alias("canceled_at"),
                col("order_discount").cast("float"),
                col("order_cancellation_reason").cast("string"),
                col("delivery_cost").cast("float"),
                col("address_text").cast("string"),
                col("user_id").cast("int"),
                col("store_id").cast("int")
            ).distinct()
            orders_df = orders_df.withColumn("delivery_city", element_at(split(col("address_text"), ", "), 1))
            
            orders_df = orders_df.join(df.select("order_id", "payment_type").distinct(), "order_id")
            orders_df = orders_df.join(payment_type_df, "payment_type").drop("payment_type")

            # 7. Таблица items
            items_df = df.select(
                col("item_id").cast("int"),
                col("item_title").cast("string"),
                col("item_price").cast("float"),
                col("item_category").cast("string")
            ).distinct()

            # Добавляем item_category_id
            items_df = items_df.join(
                item_category_df,
                items_df.item_category == item_category_df.item_category,
                "left"
            ).drop("item_category")

            items_df = items_df.select(
                "item_id", "item_title", "item_price","item_category_id"
            ).orderBy("item_id")

            # 8. Таблица order_to_item
            order_to_item_df = df.select(
                col("order_id").cast("int"),
                col("item_id").cast("int"),
                col("item_quantity").cast("float"),
                col("item_canceled_quantity").cast("float"),
                when(col("item_replaced_id").isNotNull(),
                     col("item_replaced_id").cast("int")).otherwise(lit(0)).alias("item_replaced_id"),
                col("item_discount").cast("float")
            ).distinct()

            # 9. Таблица delivery
            delivery_df = df.select(
                col("order_id").cast("int"),
                col("driver_id").cast("int"),
                to_timestamp(col("delivery_started_at")).alias("delivery_started_at"),
                to_timestamp(col("delivered_at")).alias("delivered_at"),
            ).distinct()

            # --- Загрузка данных в основную БД (postgres-main) ---
            # Получаем параметры подключения для основной БД
            # main_conn = main_db_hook.get_connection("postgres_main_gp")
            # main_jdbc_url = f"jdbc:postgresql://{main_conn.host}:{main_conn.port}/{main_conn.schema}"
            
            # Функция для загрузки DataFrame в PostgreSQL
            # def load_to_postgres(df, table_name):
            #     df.write \
            #         .format("jdbc") \
            #         .option("url", main_jdbc_url) \
            #         .option("dbtable", table_name) \
            #         .option("user", main_conn.login) \
            #         .option("password", main_conn.password) \
            #         .option("driver", "org.postgresql.Driver") \
            #         .mode("append") \
            #         .save()
            def load_to_postgres(spark_df, table_name, primary_key_list):
                pandas_df = spark_df.toPandas()
                pandas_df = pandas_df.where(pandas_df.notna(), None)
                pandas_df = pandas_df.replace({pd.NaT: None})
                
                rows = [tuple(row) for row in pandas_df.to_numpy()]
                columns = list(pandas_df.columns)
                
                main_db_hook.insert_rows(
                     table=table_name
                    ,rows=rows
                    ,target_fields=columns
                    ,commit_every=1000  # Пакетная вставка по 1000 строк
                    ,replace=True
                    ,replace_index=primary_key_list
                )
            
            # Загрузка всех таблиц в основную БД
            tables = [
                (users_df, "users", ['user_id']),
                (driver_df, "driver", ['driver_id']),
                (store_df, "store", ['store_id']),
                (payment_type_df, "payment_type", ['payment_type_id']),
                (item_category_df, "item_category", ['item_category_id']),
                (orders_df, "orders", ['order_id']),
                (items_df, "items", ['item_id', 'validity_datetime_start', 'validity_datetime_end']),
                (order_to_item_df, "order_to_item", ['order_id', 'item_id', 'validity_datetime_start', 'validity_datetime_end']),
                (delivery_df, "delivery", ['driver_id', 'order_id'])
            ]
            
            for df_table, table_name, primary_key_list in tables:
                print(f"Загрузка данных в таблицу {table_name}...")
                load_to_postgres(df_table, table_name, primary_key_list)
                print(f"Таблица {table_name} успешно обновлена.")
            
            # Обновляем таблицу processed_files в БД Airflow
            with engine.connect() as connection:
                insert_query = """
                    INSERT INTO processed_files (file_name)
                    VALUES (:s)
                    ON CONFLICT (file_name) DO NOTHING;
                """
                connection.execute(text(insert_query), {"s": file_name})
                connection.commit()
            
            print(f"Файл {file_name} успешно обработан и добавлен в отслеживаемые.")
            
    except Exception as e:
        print(f"Ошибка при обработке файлов: {e}")
        raise
    finally:
        spark.stop()

# Определение задач DAG
get_new_files_task = PythonOperator(
    task_id='get_new_files',
    python_callable=get_new_parquet_files,
    provide_context=True,
    dag=dag,
)

process_and_load_task = PythonOperator(
    task_id='process_and_load_data',
    python_callable=process_and_load_data,
    provide_context=True,
    dag=dag,
)

# Определение последовательности задач
get_new_files_task >> process_and_load_task

# with DAG(
#     dag_id=DAG_ID,
#     start_date=datetime(2025, 12, 11),
#     schedule_interval="0 * * * *",
#     catchup=False,
#     tags=["crypto", "api", "etl"]
# ) as dag:

#     create_staging_table = PostgresOperator(
#         task_id="create_staging_table",
#         postgres_conn_id="main_postgres",
#         sql="""
#         CREATE TABLE IF NOT EXISTS crypto_staging (
#             timestamp TIMESTAMP,
#             price FLOAT,
#             coin TEXT
#         );
#         """
#     )

#     create_final_table = PostgresOperator(
#         task_id="create_final_table",
#         postgres_conn_id="main_postgres",
#         sql="""
#         CREATE TABLE IF NOT EXISTS crypto_aggregated (
#             coin TEXT PRIMARY KEY,
#             avg_price FLOAT,
#             min_price FLOAT,
#             max_price FLOAT,
#             processed_at TIMESTAMP
#         );
#         """
#     )

#     fetch_staging = PythonOperator(
#         task_id="fetch_crypto_data",
#         python_callable=fetch_crypto_data,
#         retries=3,
#         retry_delay=timedelta(seconds=30)
#     )

#     transform = PythonOperator(
#         task_id="transform_crypto_data",
#         python_callable=transform_crypto_data
#     )

#     load_final = PythonOperator(
#         task_id="load_crypto_to_postgres",
#         python_callable=load_crypto_to_postgres
#     )

#     create_staging_table >> fetch_staging
#     create_final_table >> transform
#     fetch_staging >> transform >> load_final