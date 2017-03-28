# Basic Dockerfile for the USASpendingAPI

## 0) Add your DATABASE_URL on the ENV line below
## 1) Build:
#        docker build . -t usaspendingapi
## 2) Run (include -p flag and ports)
#        docker run -p 127.0.0.1:8000:8000 usaspendingapi

# This will forward port 8000 of the container to your localhost:8000 and start a new container for the API.
# Rebuild and run when code in /usaspending-api changes

FROM python:3.5.3

WORKDIR /dockermount

RUN apt-get update -y

RUN apt-get install -y \
	memcached \
	libmemcached-dev

ADD requirements/requirements.txt /dockermount/requirements/requirements.txt

RUN pip install -r requirements/requirements.txt

ADD . /dockermount

ENV DATABASE_URL postgres://user@database_info:5432/db_name

EXPOSE 8000

CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
