#!/usr/bin/env bash
# Готовит общие каталоги, базу Airflow и пользователя, затем запускает airflow standalone
# (scheduler + webserver + triggerer в одном контейнере).
set -euo pipefail

mkdir -p /opt/airflow/models /opt/airflow/dq_reports /opt/airflow/training_data \
         /opt/airflow/airflow_state /opt/airflow/logs
# каталоги общие с fraud_detector и data_loader, которые работают от root
chmod 777 /opt/airflow/models /opt/airflow/dq_reports /opt/airflow/training_data \
          /opt/airflow/airflow_state /opt/airflow/logs

run_as_airflow() {
  /usr/sbin/runuser --preserve-environment --user airflow -- "$@"
}

run_as_airflow airflow db migrate
run_as_airflow airflow users create \
  --username "${AIRFLOW_ADMIN_USER:-admin}" --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
  --firstname Admin --lastname User --role Admin --email admin@example.com || true

exec /usr/sbin/runuser --preserve-environment --user airflow -- airflow standalone
