# Dockerfile for the USAspending Backend API
# When built with docker compose --profile usaspending build,
# it will be built and tagged with the name in the image: key of the docker compose services that use this default Dockerfile

# Since requirements are copied into the image at build-time, this MUST be rebuilt if Python requirements change

# See docker-compose.yml file and README.md for docker compose information

FROM python:3.10.12-slim-bullseye

WORKDIR /dockermount

RUN apt update && apt install -y \
    curl \
    gcc \
    libpq-dev \
    postgresql-13

##### Copy python packaged
COPY . /dockermount

RUN python3 -m pip install -r requirements/requirements.txt && \
    python3 -m pip install -r requirements/requirements-server.txt && \
    python3 -m pip install ansible==2.9.15 awscli==1.34.19

RUN mkdir -p /root/.duckdb/extensions/v1.3.0/linux_amd64 && \
    curl http://extensions.duckdb.org/v1.3.0/linux_amd64/delta.duckdb_extension.gz | gunzip > /root/.duckdb/extensions/v1.3.0/linux_amd64/delta.duckdb_extension && \
    curl http://extensions.duckdb.org/v1.3.0/linux_amd64/aws.duckdb_extension.gz | gunzip > /root/.duckdb/extensions/v1.3.0/linux_amd64/aws.duckdb_extension && \
    curl http://extensions.duckdb.org/v1.3.0/linux_amd64/httpfs.duckdb_extension.gz | gunzip > /root/.duckdb/extensions/v1.3.0/linux_amd64/httpfs.duckdb_extension

##### Ensure Python STDOUT gets sent to container logs
ENV PYTHONUNBUFFERED=1
