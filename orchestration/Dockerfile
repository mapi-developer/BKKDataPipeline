FROM apache/airflow:3.2.0-python3.11

USER root

RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=/usr/local/bin/python
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python

USER airflow

RUN pip install --no-cache-dir pyspark==3.5.0 delta-spark==3.1.0 dbt-databricks apache-airflow-providers-fab