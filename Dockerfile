FROM golang:1.26.11-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/controlplane ./cmd/controlplane
RUN CGO_ENABLED=0 go build -o /out/worker ./cmd/worker

FROM alpine:3.20 AS controlplane
RUN adduser -D -u 10001 app
COPY --from=build /out/controlplane /usr/local/bin/controlplane
USER app
ENTRYPOINT ["/usr/local/bin/controlplane"]

FROM alpine:3.20 AS worker
RUN adduser -D -u 10001 app
COPY --from=build /out/worker /usr/local/bin/worker
USER app
ENTRYPOINT ["/usr/local/bin/worker"]
