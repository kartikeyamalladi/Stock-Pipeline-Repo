FROM apache/airflow:2.9.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY --chown=airflow:root airflow/dags/   /opt/airflow/dags/
COPY --chown=airflow:root ingestion/      /opt/airflow/ingestion/
COPY --chown=airflow:root processing/     /opt/airflow/processing/
COPY --chown=airflow:root config/         /opt/airflow/config/

ENV PYTHONPATH=/opt/airflow

WORKDIR /opt/airflow
