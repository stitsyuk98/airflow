FROM apache/airflow:2.9.3
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

ENV AIRFLOW_CONN_CLICKHOUSE='clickhouse://admin:admin123@rc1a-77r1s63sa631tq87.mdb.yandexcloud.net:9440'

USER root
RUN apt-get update \ 
    && apt-get install wget \
    && apt-get install xz-utils
USER airflow
