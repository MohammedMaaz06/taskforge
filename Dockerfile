FROM golang:alpine AS builder
RUN apk add --no-cache gcc musl-dev
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -o /app/server ./cmd/server

FROM alpine:3.19
WORKDIR /app
RUN apk add --no-cache ca-certificates sqlite-libs
COPY --from=builder /app/server /app/server
COPY web ./web

EXPOSE 8080
CMD ["/app/server"]
