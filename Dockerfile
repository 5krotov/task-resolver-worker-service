# ===========================
# 1) BUILD STAGE
# ===========================
FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of your source code
COPY . .

# Build the worker binary
# If your main is at cmd/main.go, we'll specify it explicitly:
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/worker-service ./cmd

# ===========================
# 2) RUNTIME STAGE
# ===========================
FROM alpine:3.17

# Create a non-root user (optional, but recommended)
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

# Copy just the compiled binary
COPY --from=builder /app/bin/worker-service /app/worker-service

# (Optional) Copy your default config if you want it bundled
COPY config.yaml /app/config.yaml

# Ensure the binary is executable
RUN chmod +x /app/worker-service

# Switch to non-root user
USER appuser

# Expose port if your worker has an HTTP endpoint
# EXPOSE 8080

# Set the entrypoint
ENTRYPOINT ["/app/worker-service"]
