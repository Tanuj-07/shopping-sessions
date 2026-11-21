# Shopping Sessions: native Rust pipeline.
# Build stage compiles for the scorer's CPU family (Intel Xeon, Cascade Lake or newer: AVX-512).
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV RUSTFLAGS="-C target-cpu=cascadelake"
RUN cargo build --release --locked --bin fast

# Runtime: just the binary on a slim base (same glibc as the builder).
FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/fast /app/fast
CMD ["/app/fast"]
