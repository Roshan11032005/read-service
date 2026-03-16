# ── Builder ───────────────────────────────────────────────────────────────────
FROM golang:1.25 AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go mod tidy

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o app ./cmd/main.go

# ── Runtime ───────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim
WORKDIR /app

COPY --from=builder /app/app .

EXPOSE 8088

CMD ["./app"]