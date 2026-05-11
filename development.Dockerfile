# Dockerfile to support local developement and tests
FROM public.ecr.aws/emr-serverless/spark/emr-7.12.0:latest

USER root

# Copy UV from uv image
COPY --from=ghcr.io/astral-sh/uv:0.7.19 /uv /uvx /bin/

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

WORKDIR /dockermount

##### The following ENV vars are optimizations from https://github.com/astral-sh/uv-docker-example/blob/main/Dockerfile
##### and https://docs.astral.sh/uv/guides/integration/docker/#optimizations
# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Copy the project into the image
COPY . /dockermount

# Install Python and dev dependencies
ARG PYTHON_VERSION=3.10.12
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --extra dev --extra spark --locked --no-install-project --python ${PYTHON_VERSION}

# Make sure uv environment is active
ENV PATH="/dockermount/.venv/bin:$PATH"

# Create a symlink for the python commands
RUN ln -sf .venv/bin/python3 /usr/local/bin/python3 & \
    ln -sf .venv/bin/python /usr/local/bin/python

# Set Python 3.10.12 as the default Python for PySpark
ENV PYSPARK_PYTHON=/dockermount/.venv/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/dockermount/.venv/bin/python3

# Limit Spark logging to WARN level
RUN cat <<'EOF' | sed 's/^[[:space:]]*//' >> $SPARK_HOME/conf/log4j2.properties
	appender.console.type = Console
    appender.console.name = CONSOLE
    appender.console.layout.type = PatternLayout
    appender.console.layout.pattern = [%d{yyyy-MM-dd HH:mm:ss.SSS}][%p] - %m%n
    rootLogger.level = WARN
    rootLogger.appenderRef.0.ref = CONSOLE
    rootLogger.appenderRef.0.level = WARN
EOF

# Set default values for Spark with local development
RUN echo "spark.authenticate false" >> $SPARK_HOME/conf/spark-defaults.conf

# Ensure Python STDOUT gets sent to container logs
ENV PYTHONUNBUFFERED=1
