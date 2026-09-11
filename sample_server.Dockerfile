FROM python:3.10.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.7.19 /uv /uvx /bin/

WORKDIR /data-act/backend

RUN apt update && apt install -y \
    build-essential \
    ca-certificates \
    curl \
    gcc \
    libpq-dev \
    lsb-release \
    nginx \
    openssl \
    gnupg2 \
    sudo \
    supervisor

RUN curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg && \
    sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' && \
    apt update && \
    apt install -y postgresql-16

RUN update-ca-certificates

# Create an "ec2-user" to mimic the user expected in supervisord.conf
RUN useradd -m -u 0 -o -g 0 -s /bin/bash ec2-user
USER ec2-user

##### The following ENV vars are optimizations from https://github.com/astral-sh/uv-docker-example/blob/main/Dockerfile
##### and https://docs.astral.sh/uv/guides/integration/docker/#optimizations
# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Use the system Python environment since the container is already isolated
ENV UV_PROJECT_ENVIRONMENT=/usr/local
ENV UV_SYSTEM_PYTHON=1

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --extra server --extra ansible --extra awscli --extra spark --locked --no-install-project --no-dev

# Copy the project into the image
COPY .. /data-act/backend

# Copy nginx config
COPY config/nginx.conf /etc/nginx/nginx.conf

# Copy supervisor config
COPY config/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Sync the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --extra server --extra ansible --extra awscli --extra spark --locked --no-dev

# Port that NGINX is listening on
EXPOSE 80

# Ensure Python STDOUT gets sent to container logs
ENV PYTHONUNBUFFERED=1

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
