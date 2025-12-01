# Dockerfile for the USAspending Backend API
# When built with docker compose --profile usaspending build,
# it will be built and tagged with the name in the image: key of the docker compose services that use this default Dockerfile

# Since requirements are copied into the image at build-time, this MUST be rebuilt if Python requirements change

# See docker-compose.yml file and README.md for docker compose information

####################
# Go builder stage #
####################
FROM golang:1.25.4 AS go_build

ENV CGO_ENABLED=1
ENV GOOS=linux

WORKDIR /src
COPY ./utilities /src

RUN make build


##########
# Python #
##########
# Bookworm (at least) is required for a new enough version of GLIBC
FROM python:3.10.12-slim-bookworm

WORKDIR /dockermount

RUN apt update && apt install -y curl gcc

# A repo is needed for Postgres 13 in Debian Bookworm \
RUN install -d /usr/share/postgresql-common/pgdg && \
    curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc && \
    sh -c 'echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list'

RUN apt update && apt install -y libpq-dev postgresql-13

##### Copy python packaged
COPY . /dockermount
RUN python3 -m pip install -r requirements/requirements.txt && \
    python3 -m pip install -r requirements/requirements-server.txt && \
    python3 -m pip install ansible==2.9.15 awscli==1.34.19

##### Ensure Python STDOUT gets sent to container logs
ENV PYTHONUNBUFFERED=1

##### Copy the .so file built from Go in the previous stage
COPY --from=go_build /src/zipper.so /dockermount/utilities/zipper.so
