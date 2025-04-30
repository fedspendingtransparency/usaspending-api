# Dockerfile for the USAspending Backend API
# When built with docker compose --profile usaspending build,
# it will be built and tagged with the name in the image: key of the docker compose services that use this default Dockerfile

# Since requirements are copied into the image at build-time, this MUST be rebuilt if Python requirements change

# See docker-compose.yml file and README.md for docker compose information

<<<<<<< HEAD
FROM rockylinux:8
# Build ARGs
ARG PYTHON_VERSION=3.10.12
WORKDIR /dockermount
# update to use centos official mirrors only
RUN sed -i '/#baseurl/s/^#//g' /etc/yum.repos.d/Rocky-*
RUN sed -i '/mirrorlist/s/^/#/g' /etc/yum.repos.d/Rocky-*
RUN dnf -y update
RUN dnf -y install gcc openssl-devel bzip2-devel libffi-devel zlib-devel wget make
RUN dnf -y groupinstall "Development Tools"
RUN dnf install epel-release -y
RUN dnf --enablerepo=powertools install perl-IPC-Run -y
RUN dnf -y module enable postgresql:13
RUN dnf -y install postgresql postgresql-devel
# Enable powertools repository and install necessary math libraries
RUN dnf config-manager --set-enabled powertools
RUN dnf -y install openblas-devel atlas-devel
# Build Python
WORKDIR /usr/src
RUN wget --quiet https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz
RUN tar xzf Python-${PYTHON_VERSION}.tgz
WORKDIR /usr/src/Python-${PYTHON_VERSION}
RUN ./configure --enable-optimizations
RUN make altinstall
RUN ln -sf /usr/local/bin/python`echo ${PYTHON_VERSION} | awk -F. '{short_version=$1 FS $2; print short_version}'` /usr/bin/python3
RUN echo "$(python3 --version)"
# Upgrade pip and install numpy separately to avoid compilation issues
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install numpy
# Install remaining requirements
WORKDIR /dockermount
COPY requirements/ /dockermount/requirements/
RUN python3 -m pip install -r requirements/requirements.txt
# Install additional requirements
RUN python3 -m pip install -r requirements/requirements-server.txt ansible==2.9.15 awscli
# Copy the rest of the project files into the container
COPY . /dockermount
# Ensure Python STDOUT gets sent to container logs
=======
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

##### Ensure Python STDOUT gets sent to container logs
>>>>>>> master
ENV PYTHONUNBUFFERED=1



















# # Dockerfile for the USAspending Backend API
# # When built with docker-compose --profile usaspending build,
# # it will be built and tagged with the name in the image: key of the docker-compose services that use this default Dockerfile
# # Since requirements are copied into the image at build-time, this MUST be rebuilt if Python requirements change
# # See docker-compose.yml file and README.md for docker-compose information
# FROM rockylinux:8
# # Build ARGs
# ARG PYTHON_VERSION=3.10.12
# WORKDIR /dockermount
# # Update the system and install necessary packages
# RUN dnf -y update && dnf -y install \
#     wget \
#     gcc \
#     openssl-devel \
#     bzip2-devel \
#     libffi \
#     libffi-devel \
#     zlib-devel \
#     sqlite-devel \
#     xz-devel \
#     make \
#     gcc-c++ \
#     && dnf clean all
# # Install PostgreSQL 13 client (psql)
# RUN dnf -y install https://download.postgresql.org/pub/repos/yum/reporpms/EL-8-x86_64/pgdg-redhat-repo-latest.noarch.rpm \
#     && dnf -qy module disable postgresql \
#     && dnf -y install postgresql13 \
#     && dnf clean all
# # Build and install Python
# WORKDIR /usr/src
# RUN wget --quiet https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz \
#     && tar xzf Python-${PYTHON_VERSION}.tgz \
#     && cd Python-${PYTHON_VERSION} \
#     && ./configure --enable-optimizations \
#     && make altinstall \
#     && ln -sf /usr/local/bin/python`echo ${PYTHON_VERSION} | awk -F. '{print $1 "." $2}'` /usr/bin/python3 \
#     && cd /usr/src \
#     && rm -rf Python-${PYTHON_VERSION} Python-${PYTHON_VERSION}.tgz
# # Copy and install Python packages
# WORKDIR /dockermount
# COPY requirements/ /dockermount/requirements/
# RUN python3 -m pip install --upgrade pip \
#     && python3 -m pip install -r requirements/requirements.txt
# # Copy the rest of the project files into the container
# COPY . /dockermount
# # Ensure Python STDOUT gets sent to container logs
# ENV PYTHONUNBUFFERED=1


























# # Dockerfile for the USAspending Backend API
# # When built with docker-compose --profile usaspending build,
# # it will be built and tagged with the name in the image: key of the docker-compose services that use this default Dockerfile

# # Since requirements are copied into the image at build-time, this MUST be rebuilt if Python requirements change

# # See docker-compose.yml file and README.md for docker-compose information

# FROM centos:8

# # Build ARGs
# ARG PYTHON_VERSION=3.8.16

# WORKDIR /dockermount

# RUN yum -y update && yum clean all
# # sqlite-devel added as prerequisite for coverage python lib, used by pytest-cov plugin
# RUN yum -y install wget gcc openssl-devel bzip2-devel libffi libffi-devel zlib-devel sqlite-devel xz-devel
# RUN yum -y groupinstall "Development Tools"

# ##### Install PostgreSQL 13 client (psql)
# RUN yum -y install https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
# RUN yum -y install postgresql13

# ##### Building python 3.x
# WORKDIR /usr/src
# RUN wget --quiet https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz
# RUN tar xzf Python-${PYTHON_VERSION}.tgz
# WORKDIR /usr/src/Python-${PYTHON_VERSION}
# RUN ./configure --enable-optimizations
# RUN make altinstall
# RUN ln -sf /usr/local/bin/python`echo ${PYTHON_VERSION} | awk -F. '{short_version=$1 FS $2; print short_version}'` /usr/bin/python3
# RUN echo "$(python3 --version)"

# ##### Copy python packaged
# WORKDIR /dockermount
# COPY requirements/ /dockermount/requirements/
# RUN python3 -m pip install -r requirements/requirements.txt

# ##### Copy the rest of the project files into the container
# COPY . /dockermount

# ##### Ensure Python STDOUT gets sent to container logs
# ENV PYTHONUNBUFFERED=1