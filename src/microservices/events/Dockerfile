FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -o events-service .

FROM alpine:3.20

RUN apk --no-cache add ca-certificates && adduser -D -u 10001 app

COPY --from=builder /app/events-service /usr/local/bin/events-service

USER app

EXPOSE 8082

CMD ["events-service"]
