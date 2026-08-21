# Build stage
FROM golang:alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache gcc musl-dev

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build -o /app/server ./cmd/server

# Final runtime stage
FROM alpine:3.19

WORKDIR /app

RUN apk add --no-cache ca-certificates sqlite-libs

COPY --from=builder /app/server /app/server

EXPOSE 8080

CMD ["/app/server"]
