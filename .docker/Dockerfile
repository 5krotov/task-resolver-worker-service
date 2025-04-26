# --- BUILD STAGE ---
FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o worker-service ./cmd/main.go

# --- RUN STAGE ---
FROM alpine:latest AS release

COPY --from=builder /app/worker-service /app/worker-service/
COPY config.yaml /etc/worker-service/

WORKDIR /app/worker-service/

CMD ["./worker-service"]