FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /collector ./cmd/main.go

# Runtime stage
FROM alpine:3.19
WORKDIR /app
COPY --from=builder /collector /app/collector
RUN chmod +x /app/collector  # Ensure executable permissions
USER nobody:nobody
ENTRYPOINT ["/app/collector"]