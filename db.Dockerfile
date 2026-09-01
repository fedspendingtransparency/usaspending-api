FROM postgres:16-alpine3.24

# Install pgvector extension https://github.com/pgvector/pgvector#installation
RUN apk add --no-cache --virtual .build-deps \
    git build-base clang21 llvm21 && \
    cd /tmp && \
    git clone --branch v0.8.6 https://github.com/pgvector/pgvector.git && \
    cd pgvector && \
    make && \
    make install && \
    apk del .build-deps
