FROM golang:1.16.2-alpine AS build

ARG VERSION=dev

WORKDIR /build

# Packages required to build
RUN apk add --no-cache build-base

# Copy project deps first since they don't change often
COPY go.mod go.sum ./
RUN go mod download

# Copy project
COPY . ./

# Build a binaries
RUN go build -o terminus ./cmd/terminus

# lakectl image
FROM alpine:3.12.0 AS terminus
WORKDIR /app
ENV PATH /app:$PATH
COPY --from=build /build/terminus ./
RUN addgroup -S terminus && adduser -S terminus -G terminus
USER terminus
WORKDIR /home/terminus
ENTRYPOINT ["/app/terminus"]
CMD ["run"]
