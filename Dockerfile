# Dockerfile for the USAspending Backend API
# When built with docker-compose --profile usaspending build,
# it will be built and tagged with the name in the image: key of the docker-compose services that use this default Dockerfile

# Since requirements are copied into the image at build-time, this MUST be rebuilt if Python requirements change

# See docker-compose.yml file and README.md for docker-compose information

FROM rockylinux:8

# Build ARGs
ARG PYTHON_VERSION=3.10.12

WORKDIR /dockermount

# update to use centos official mirrors only
RUN sed -i '/#baseurl/s/^#//g' /etc/yum.repos.d/Rocky-*
RUN sed -i '/mirrorlist/s/^/#/g' /etc/yum.repos.d/Rocky-*

RUN dnf -y update
# sqlite-devel added as prerequisite for coverage python lib, used by pytest-cov plugin
RUN dnf -y install gcc openssl-devel bzip2-devel libffi-devel zlib-devel wget make
RUN dnf -y groupinstall "Development Tools"

RUN dnf install epel-release -y
RUN dnf --enablerepo=powertools install perl-IPC-Run -y


##### Install PostgreSQL 13 client (psql)
## Import and install not working on local BAH computers
#RUN rpm --import https://download.postgresql.org/pub/repos/yum/keys/RPM-GPG-KEY-PGDG-AARCH64-RHEL8
#RUN dnf -y install https://download.postgresql.org/pub/repos/yum/reporpms/EL-8-x86_64/pgdg-redhat-repo-latest.noarch.rpm

RUN dnf -y module enable postgresql:13
RUN dnf -y install postgresql
RUN dnf -y install postgresql-devel


##### Building python 3.x
WORKDIR /usr/src
RUN wget --quiet https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz
RUN tar xzf Python-${PYTHON_VERSION}.tgz
WORKDIR /usr/src/Python-${PYTHON_VERSION}
RUN ./configure --enable-optimizations
RUN make altinstall
RUN ln -sf /usr/local/bin/python`echo ${PYTHON_VERSION} | awk -F. '{short_version=$1 FS $2; print short_version}'` /usr/bin/python3
RUN echo "$(python3 --version)"

##### Copy python packaged
WORKDIR /dockermount
COPY requirements/ /dockermount/requirements/
RUN export PATH=$PATH:/usr/pgsql-13/bin && python3 -m pip install -r requirements/requirements.txt

RUN python3 -m pip install -r requirements/requirements-server.txt ansible==2.9.15 awscli

##### Copy the rest of the project files into the container
COPY . /dockermount

##### Ensure Python STDOUT gets sent to container logs
ENV PYTHONUNBUFFERED=1
