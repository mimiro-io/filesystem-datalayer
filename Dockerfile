FROM golang:1.22-alpine AS build_env

# Copy go mod and sum files
COPY go.mod go.sum ./

# Install SSL ca certificates.
# Ca-certificates is required to call HTTPS endpoints.
RUN apk update && apk add --no-cache \
    ca-certificates \
    tzdata && \
    update-ca-certificates

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

FROM build_env AS builder
# Copy the source from the current directory to the Working Directory inside the container
COPY layer.go ./
COPY cmd ./cmd
COPY testconfig ./testconfig

# Build the app binaries
RUN go build -a -o /filesystem-datalayer ./cmd/main.go

FROM alpine:3.21
# Copy the Pre-built binary file from the previous stage
COPY --from=builder /filesystem-datalayer .
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Command to run the executable
CMD ["./filesystem-datalayer"]