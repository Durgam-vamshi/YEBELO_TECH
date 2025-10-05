# Rust base image (latest)
FROM rust:latest

WORKDIR /app

# Copy Cargo files
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src

# Build release binary
RUN cargo build --release

# Run the compiled binary
CMD ["./target/release/rsi_service"]
