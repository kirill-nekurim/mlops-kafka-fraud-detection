FROM apache/airflow:2.10.5-python3.11

USER airflow
# numpy/pandas зафиксированы: более новые версии ломают Airflow 2.10
RUN pip install --no-cache-dir \
    "numpy==1.26.4" \
    "pandas==2.1.4" \
    "scikit-learn==1.5.2" \
    "catboost==1.2.8" \
    && python -c "import catboost, sklearn, pandas, pyarrow, psycopg2; print('ok', catboost.__version__)"

USER root
COPY entrypoint.sh /custom-entrypoint.sh
RUN chmod +x /custom-entrypoint.sh

ENTRYPOINT ["/custom-entrypoint.sh"]
