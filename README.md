# HSE_DE_group_project
## Сначала как развернуть приложение 
Откройте docker desktop и желательно выделить Memory Limit: 8 GB и Swap: 2 GB, чтобы потом DAG загрузки данных не падал из-за недостатка памяти
```
git clone -b dev https://github.com/evslou/HSE_DE_group_project
cd HSE_DE_group_project
docker compose up --build 
```
## Креды для использования
### Хосты 
Airflow - http://localhost:8080/home
PGAdmin - http://localhost:5050/browser/
### Явки пароли
USERNAME=teambi
PASSWORD=teambi2025
EMAIL=teambi@hse.ru

## Описание происходящего, как им пользоваться
Таблицы создаются согласно DDL скриптам во время билда приложения. 
Для заполнения таблиц данными следует запустить в Airflow DAG с названием [parquet_to_postgres_loader]. Изначально при запуске будет 2 процесса параллельно отрабатывать. Один из них надо "убить", чтобы память не переполнилась от двух процессов параллельно. (process_and_load_data квадратик процесса -> "Mark state as..." -> failed). Ожидание отработки процесса зависит от мощности компа, но где-то 10-30 минут. Можно отслежить процесс загрузки через таблицу processed_files в airflow_db (всего должно быть загружено 35 файлов)
Далее можно запускать даги [calculate_mart_etl] и [calculate_items_mart_etl] для подсчета витрин. Витрины лежат в postgres-main-db таблицах orders_mart и items_mart
