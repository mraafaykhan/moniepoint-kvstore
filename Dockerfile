FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /kvstore-server ./cmd/server

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /kvstore-server /usr/local/bin/kvstore-server
RUN mkdir -p /data
EXPOSE 9000 9001
ENTRYPOINT ["kvstore-server"]
CMD ["-dir", "/data", "-tcp", ":9000", "-http", ":9001"]
