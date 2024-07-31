FROM golang:1.22 AS builder
WORKDIR app
COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /app/main .

FROM alpine AS runner
WORKDIR app

COPY --from=builder /app/main /app/main

EXPOSE 8083
ENTRYPOINT ["/app/main"]