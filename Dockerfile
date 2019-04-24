# Basic Dockerfile for the USASpendingAPI

## 0) Add your DATABASE_URL on the ENV line below. Use host.docker.internal instead of localhost (overidden with Docker compose)
##
## 1) Init/run order w/Docker compose:
##       docker-compose up usaspending-db (leave running)
##       docker-compose up usaspending-db-migrate
##       docker-compose up usaspending-db-sql
##       docker-compose up usaspending-db-init
##    Then run/re-run using the db you just created:
##       docker-compose up usaspending-api
##
## Optional) Run ad-hoc commands:
#        docker build . -t usaspendingapi
#        docker run -p 127.0.0.1:8000:8000 usaspendingapi <command>

# Rebuild and run when code in /usaspending-api changes

FROM python:3.5

WORKDIR /dockermount

# For "Wrong sources.list entry or malformed file" re: main/binary-amd64/Packages, revisit
RUN printf "deb http://archive.debian.org/debian/ jessie main\ndeb-src http://archive.debian.org/debian/ jessie main\ndeb http://security.debian.org jessie/updates main\ndeb-src http://security.debian.org jessie/updates main" > /etc/apt/sources.list

RUN apt-get update -y

COPY requirements/requirements.txt /dockermount/requirements/requirements.txt
RUN pip install -r requirements/requirements.txt

COPY . /dockermount

# Compose overrides DATABASE_URL
ENV DATABASE_URL postgres://username@host.docker.internal:5432/data_store_api

ENV PYTHONUNBUFFERED=0

EXPOSE 8000
