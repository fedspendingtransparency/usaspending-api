# Dockerfile to support local developement and tests
FROM public.ecr.aws/emr-serverless/spark/emr-7.12.0:latest

USER root

# Copy UV from uv image
COPY --from=ghcr.io/astral-sh/uv:0.7.19 /uv /uvx /bin/

# Build ARGs
ARG PROJECT_LOG_DIR=/logs
ARG PYTHON_VERSION=3.10.12

# Install dependencies
RUN dnf update \
    && dnf install -y \
        git \
        libffi-devel \
        libpq-devel \
        nodejs \
        npm \
        sqlite-devel \
        wget \
        zlib-devel \
    && dnf clean all

# Download the Postgres JAR
RUN wget -P /usr/lib/spark/jars/ https://jdbc.postgresql.org/download/postgresql-42.7.5.jar

# Install Dredd
RUN npm install --global dredd@13.1.2

WORKDIR /usaspending-api

# Allow git operations to be run inside of the container
# RUN git config --global --add safe.directory /usaspending-api

##### The following ENV vars are optimizations from https://github.com/astral-sh/uv-docker-example/blob/main/Dockerfile
##### and https://docs.astral.sh/uv/guides/integration/docker/#optimizations
# Specify .venv location outside of the project to avoid conflict
ENV UV_PROJECT_ENVIRONMENT=/usr/local/.venv

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Copy relevant parts of the project for package management
COPY pyproject.toml uv.lock /usaspending-api/

# Install Python and dev dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --extra dev --extra spark --locked --no-install-project --python ${PYTHON_VERSION}

# Make sure uv environment is active
ENV PATH="/usr/local/.venv/bin:$PATH"

# Set Python 3.10.12 as the default Python for PySpark
ENV PYSPARK_PYTHON=/usr/local/.venv/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/local/.venv/bin/python3

# Update logging for Spark
RUN sed -i "s|^# spark.eventLog.dir.*$|spark.eventLog.dir                 file:///usaspending-api/$PROJECT_LOG_DIR/spark-events|g" $SPARK_HOME/conf/spark-defaults.conf \
    && sed -i '/spark.eventLog.enabled/s/^# //g' $SPARK_HOME/conf/spark-defaults.conf \
    && echo "spark.history.fs.logDirectory      file:///usaspending-api/$PROJECT_LOG_DIR/spark-events" >> $SPARK_HOME/conf/spark-defaults.conf

# Set default values for Spark with local development
RUN echo "spark.authenticate false" >> $SPARK_HOME/conf/spark-defaults.conf

# Ensure Python STDOUT gets sent to container logs
ENV PYTHONUNBUFFERED=1
