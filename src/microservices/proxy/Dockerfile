FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod ./
COPY *.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -o proxy-service .

FROM alpine:3.20

RUN apk --no-cache add ca-certificates && adduser -D -u 10001 app

COPY --from=builder /app/proxy-service /usr/local/bin/proxy-service

USER app

EXPOSE 8000

CMD ["proxy-service"]
