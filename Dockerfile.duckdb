# Dockerfile for downloads using DuckDB

FROM usaspending-backend:latest

ENV DUCKDB_VERSION=1.4.3

# Install DuckDB extensions
RUN mkdir -p /root/.duckdb/extensions/v$DUCKDB_VERSION/linux_amd64 && \
    curl http://extensions.duckdb.org/v$DUCKDB_VERSION/linux_amd64/delta.duckdb_extension.gz | gunzip > /root/.duckdb/extensions/v$DUCKDB_VERSION/linux_amd64/delta.duckdb_extension && \
    curl http://extensions.duckdb.org/v$DUCKDB_VERSION/linux_amd64/aws.duckdb_extension.gz | gunzip > /root/.duckdb/extensions/v$DUCKDB_VERSION/linux_amd64/aws.duckdb_extension && \
    curl http://extensions.duckdb.org/v$DUCKDB_VERSION/linux_amd64/httpfs.duckdb_extension.gz | gunzip > /root/.duckdb/extensions/v$DUCKDB_VERSION/linux_amd64/httpfs.duckdb_extension && \
    curl http://extensions.duckdb.org/v$DUCKDB_VERSION/linux_amd64/postgres_scanner.duckdb_extension.gz | gunzip > /root/.duckdb/extensions/v$DUCKDB_VERSION/linux_amd64/postgres_scanner.duckdb_extension
