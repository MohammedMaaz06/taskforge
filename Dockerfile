FROM golang:alpine AS builder

WORKDIR /app

# Install build dependencies for CGO if needed
RUN apk add --no-cache gcc musl-dev

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build static binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o taskforge ./cmd/server

FROM alpine:latest

WORKDIR /app

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /app/taskforge .
COPY --from=builder /app/static ./static

EXPOSE 8080

ENTRYPOINT ["./taskforge"]

