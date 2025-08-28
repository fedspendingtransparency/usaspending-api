FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3
USER root
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.524
ENV DELTA_SPARK_VERSION=3.3.2
ENV POSTGRES_VERSION=42.7.5

# Install postgres and wget
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    postgresql \
    postgresql-contrib \
    wget

# Install JAR files
RUN mkdir -p /usr/local/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -P /usr/local/spark/jars/ && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -P /usr/local/spark/jars/ && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_SPARK_VERSION}/delta-spark_2.12-${DELTA_SPARK_VERSION}.jar -P /usr/local/spark/jars/ && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_SPARK_VERSION}/delta-storage-${DELTA_SPARK_VERSION}.jar -P /usr/local/spark/jars/ && \
    wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.jar -P /usr/local/spark/jars/

# Install Minio client
Run wget https://dl.min.io/client/mc/release/linux-amd64/mc && \
    chmod +x mc && \
    mv mc /usr/local/bin/
COPY .jupyter /home/jovyan/.jupyter
USER jovyan
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
