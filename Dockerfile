# Basic Dockerfile for the USASpendingAPI

## 0) Add your DATABASE_URL on the ENV line below. Use host.docker.internal instead of localhost.
## 1) Run server w/docker-compose, check port 8000:
#        docker-compose up --build
## Optional) Run ad-hoc commands:
#        docker build . -t usaspendingapi
#        docker run -p 127.0.0.1:8000:8000 usaspendingapi <command>

# Rebuild and run when code in /usaspending-api changes

FROM python:3.5.3

WORKDIR /dockermount

# For "Wrong sources.list entry or malformed file" re: main/binary-amd64/Packages, revisit
RUN printf "deb http://archive.debian.org/debian/ jessie main\ndeb-src http://archive.debian.org/debian/ jessie main\ndeb http://security.debian.org jessie/updates main\ndeb-src http://security.debian.org jessie/updates main" > /etc/apt/sources.list

RUN apt-get update -y

RUN apt-get install -y \
	memcached \
	libmemcached-dev

ADD requirements/requirements.txt /dockermount/requirements/requirements.txt
RUN pip install -r requirements/requirements.txt

COPY . /dockermount

ENV DATABASE_URL postgres://willjackson@host.docker.internal:5432/data_store_api

ENV PYTHONUNBUFFERED=0

EXPOSE 8000
