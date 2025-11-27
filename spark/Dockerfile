FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu

USER root

RUN apt-get update && \ 
apt-get install -y --no-install-recommends \ 
rsync && \ 
apt-get clean && \ 
rm -rf /var/lib/apt/lists/*

ENV SPARK_CONF_DIR="$SPARK_HOME/conf"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3
ENV SPARK_LOG_DIR=/opt/spark/spark-events
ENV PYTHONPATH="/opt/spark/work-dir:$PYTHONPATH"

RUN mkdir -p /opt/spark/spark-events
COPY ./conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY ./conf/metric.properties /opt/spark/conf/metrics.properties
COPY entrypoint.sh /opt/spark/entrypoint.sh
COPY requirements.txt /opt/spark/requirements.txt


RUN pip install -r /opt/spark/requirements.txt

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

ENTRYPOINT ["/opt/spark/entrypoint.sh"]