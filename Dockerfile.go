FROM golang:1.25.4-alpine AS go_build

ENV CGO_ENABLED=1
ENV GOOS=linux

# Install the required build dependencies
RUN apk add --no-cache build-base

WORKDIR /src
COPY ./utilities /src

RUN make build
