FROM apache/airflow:2.9.2

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jdk \
        curl \
        tar \
        netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# Install Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -o /tmp/spark.tgz && \
    mkdir -p /opt/spark && \
    tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 && \
    rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

USER airflow

# Install Python dependencies
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Copy custom scripts
COPY quarto.sh /
RUN bash /quarto.sh

COPY setup_conn.py $AIRFLOW_HOME/

# Install kafka-python
RUN pip install kafka-python

USER airflow
WORKDIR /app
