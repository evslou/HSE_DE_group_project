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
USER_EXISTS=$(airflow users list | grep -c "${AIRFLOW_DEFAULT_USERNAME}" || true)

if [ "$USER_EXISTS" -eq "0" ]; then
    echo "Creating ${AIRFLOW_DEFAULT_USERNAME} user..."
    airflow users create \
        --username "${AIRFLOW_DEFAULT_USERNAME}" \
        --password "${AIRFLOW_DEFAULT_PASSWORD}" \
        --firstname "${AIRFLOW_DEFAULT_FIRSTNAME}" \
        --lastname "${AIRFLOW_DEFAULT_LASTNAME}" \
        --role Admin \
        --email "${AIRFLOW_DEFAULT_EMAIL}"
else
    echo "${AIRFLOW_DEFAULT_USERNAME} user already exists — skipping creation."
fi

echo "Starting Airflow: airflow $@"
exec airflow "$@"