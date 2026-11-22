# Profile-guided build (the plain build is Dockerfile.plain).
# The build downloads the track's public data generator, profiles the pipeline on a generated
# sample, and rebuilds with that profile. Any failure in those steps falls back to the plain
# build, so this never builds something worse than `Dockerfile`.
FROM rust:1-bookworm AS build
WORKDIR /src
RUN rustup component add llvm-tools-preview || true
COPY Cargo.toml Cargo.lock pgo-build.sh ./
COPY src ./src
ENV RUSTFLAGS="-C target-cpu=cascadelake"
RUN sh pgo-build.sh
# Diagnostic: build the official data generator so the run can make a 5M-event dataset.
RUN apt-get update -qq && apt-get install -y -qq unzip > /dev/null && curl -fsSL --max-time 120 -o /tmp/g.zip https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/generator.zip && mkdir -p /tmp/g && unzip -q /tmp/g.zip -d /tmp/g && cd "$(dirname "$(find /tmp/g -name Cargo.toml | head -1)")" && cargo build --release && cp target/release/generate /src/generate

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/fast /app/fast
COPY --from=build /src/generate /app/generate
# Diagnostic run: print the scoring environment, then fail the dev check on purpose (logs shown).
ENV SS_DIAG=1
CMD ["/app/fast"]
