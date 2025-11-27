FROM apache/airflow:2.10.2-python3.12

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY ./spark/entrypoint.sh /opt/spark/entrypoint.sh
COPY ./airflow/requirements.txt /opt/airflow/requirements.txt

RUN chmod +x /opt/spark/entrypoint.sh

COPY --from=apache/spark:3.5.7-scala2.12-java17-python3-ubuntu /opt/spark /opt/spark

USER airflow
RUN pip install -r /opt/airflow/requirements.txt
