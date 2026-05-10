FROM golang:1.23-bookworm AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev pkg-config gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -o /bin/producer ./cmd/producer

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /bin/producer /usr/local/bin/producer

ENTRYPOINT ["producer"]
