# Dockerfile for the USAspending Backend API
# When built with docker compose --profile usaspending build,
# it will be built and tagged with the name in the image: key of the docker compose services that use this default Dockerfile

# Since requirements are copied into the image at build-time, this MUST be rebuilt if Python requirements change

# See docker-compose.yml file and README.md for docker compose information

FROM python:3.10.12-slim-bullseye

WORKDIR /dockermount

##### Install postgres 16
RUN apt-get update && apt-get install -y wget gnupg lsb-release \
 && wget -qO - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
 && echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" \
      > /etc/apt/sources.list.d/pgdg.list \
 && apt-get update \
 && apt-get install -y postgresql-16


RUN apt update && apt install -y \
    curl \
    gcc \
    libpq-dev

COPY . /dockermount

RUN python3 -m pip install -r requirements/requirements.txt && \
    python3 -m pip install -r requirements/requirements-server.txt && \
    python3 -m pip install ansible==2.9.15 awscli==1.34.19 duckdb

# Ensure Python STDOUT gets sent to container logs
ENV PYTHONUNBUFFERED=1
