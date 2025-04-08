FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow==2.7.1 \
    apache-airflow-providers-apache-spark \
    pyspark==3.4.1\
    apache-airflow-providers-openlineage>=1.8.0