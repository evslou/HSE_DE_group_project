#!/usr/bin/env bash
set -e

echo "Waiting for Postgres..."
while ! nc -z postgres-airflow 5432; do
  sleep 1
done
echo "Postgres is available."

echo "Initializing Airflow database..."
airflow db migrate

echo "Checking if admin user exists..."
USER_EXISTS=$(airflow users list | grep -c "admin" || true)

if [ "$USER_EXISTS" -eq "0" ]; then
    echo "Creating admin user..."
    airflow users create \
        --username admin \
        --password admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com
else
    echo "Admin user already exists — skipping creation."
fi

echo "Starting Airflow: airflow $@"
exec airflow "$@"
