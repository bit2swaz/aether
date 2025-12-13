FROM golang:1.25-alpine AS builder
RUN apk add --no-cache gcc musl-dev sqlite-dev
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -ldflags="-w -s" -o aether ./cmd/aether
FROM alpine:latest
RUN apk add --no-cache sqlite-libs
COPY --from=builder /app/aether /usr/local/bin/aether
EXPOSE 5432 7000 8080
ENTRYPOINT ["aether"]
CMD ["start"]
