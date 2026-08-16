FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o taskforge ./cmd/server

FROM alpine:3.20
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app
RUN adduser -D -g '' taskuser && \
    mkdir -p /app/data && \
    chown -R taskuser:taskuser /app
USER taskuser
COPY --from=builder /app/taskforge .
EXPOSE 8080
ENTRYPOINT ["./taskforge"]
