# Multi-stage ultra-lightweight build
FROM rust:1.80-alpine AS builder

RUN apk add --no-cache musl-dev openssl-dev openssl-libs-static pkgconfig git

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Compile static release binary
ENV RUSTFLAGS="-C target-feature=+crt-static"
RUN cargo build --release

# Final runtime image (Distroless / Scratch for 0ms cold boot and lowest RAM)
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /app/target/release/pxy-rust /usr/local/bin/pxy-rust

ENV PORT=8080
EXPOSE 8080

CMD ["/usr/local/bin/pxy-rust"]
