FROM apache/airflow:2.7.1-python3.9

COPY requirements.txt /opt/airflow/

USER root

RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean && \ 
    apt install nano  
# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64

USER airflow
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

