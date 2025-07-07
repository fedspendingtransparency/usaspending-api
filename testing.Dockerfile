# Dockerfile for usaspending-test image

# If any changes are made to the usaspending-backend image (Dockerfile) then this image needs to be rebuilt to stay in sync.

# If you do not already have the `usaspending-backend` image locally you can build it by running:
#       docker compose --profile usaspending build
# in the parent usaspending-api directory.

FROM usaspending-backend:latest

# Install dependencies
RUN apt update && apt install -y \
    build-essential \
    coreutils \
    procps \
    wget

# Install Amazon Corretto 8 (a Long-Term Supported (LTS) distribution of OpenJDK 8)
RUN wget -qO - https://apt.corretto.aws/corretto.key | gpg --dearmor -o /usr/share/keyrings/corretto-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/corretto-keyring.gpg] https://apt.corretto.aws stable main" | tee /etc/apt/sources.list.d/corretto.list
RUN apt update && \
    apt install -y java-1.8.0-amazon-corretto-jdk
RUN export JAVA_HOME=/usr/lib/jvm/java-1.8.0-amazon-corretto
RUN export PATH=${JAVA_HOME}/bin:$PATH

# Install dev dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --extra dev --locked

WORKDIR /dockermount