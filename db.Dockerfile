FROM postgres:16-alpine

# Install pgvector extension https://github.com/pgvector/pgvector#installation
RUN apk update && apk add --no-cache --virtual .build-deps \
    git build-base clang19 llvm19 && \
    cd /tmp && \
    git clone --branch v0.8.6 https://github.com/pgvector/pgvector.git && \
    cd pgvector && \
    make && \
    make install && \
    apk del .build-deps
