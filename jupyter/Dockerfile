FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3
USER root
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.524
# Install postgres and wget
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    postgresql \
    postgresql-contrib \
    wget
# Install JAR files for S3A support
RUN mkdir -p /usr/local/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -P /usr/local/spark/jars/ && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -P /usr/local/spark/jars/
# Install Minio client
Run wget https://dl.min.io/client/mc/release/linux-amd64/mc && \
    chmod +x mc && \
    mv mc /usr/local/bin/
COPY .jupyter /home/jovyan/.jupyter
USER jovyan
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
