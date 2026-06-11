FROM apache/airflow:3.2.2-python3.11

ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor \
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@airflow-postgres:5432/airflow \
    AIRFLOW__CORE__FERNET_KEY= \
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true \
    AIRFLOW__CORE__LOAD_EXAMPLES=false \
    AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true \
    INGESTION_WORKER_URL=http://ingestion-worker:8000 \
    AWS_DEFAULT_REGION=ap-southeast-1 \
    S3_LANDING_PREFIX=landing/douyin/api_raw/json

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER airflow