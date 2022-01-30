# Basic Dockerfile for the USASpendingAPI

## Add your DATABASE_URL on the ENV line below. Use host.docker.internal instead of localhost (overidden with Docker compose)

## Optional) Run ad-hoc commands:
#        docker build . -t usaspendingapi
#        docker run -p 127.0.0.1:8000:8000 usaspendingapi <command>

# Rebuild and run when code in /usaspending-api changes

# See README.md for docker-compose information

#FROM centos:7
FROM python:3.7.10

WORKDIR /dockermount

#RUN yum -y update && yum clean all
## sqlite-devel added as prerequisite for coverage python lib, used by pytest-cov plugin
#RUN yum -y install wget gcc openssl-devel bzip2-devel libffi libffi-devel zlib-devel sqlite-devel
#RUN yum -y groupinstall "Development Tools"


##### Install PostgreSQL 10 client (psql)
RUN apt-get -y update && apt-get install -y postgresql-client
#RUN yum -y install https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
#RUN yum -y install postgresql10

##### Building python 3.7
#WORKDIR /usr/src
#RUN wget --quiet https://www.python.org/ftp/python/3.7.3/Python-3.7.3.tgz
#RUN tar xzf Python-3.7.3.tgz
#WORKDIR /usr/src/Python-3.7.3
#RUN ./configure --enable-optimizations
#RUN make altinstall
#RUN ln -sf /usr/local/bin/python3.7 /usr/bin/python3
#RUN echo "$(python3 --version)"

##### Copy python packaged
WORKDIR /dockermount
COPY requirements/ /dockermount/requirements/
RUN python3 -m pip install -r requirements/requirements.txt

##### Copy the rest of the project files into the container
COPY . /dockermount

##### Ensure Python STDOUT gets sent to container logs
ENV PYTHONUNBUFFERED=1
