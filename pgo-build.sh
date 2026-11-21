#!/bin/sh
# Profile-guided build of `fast`, with a plain-build fallback.
# Runs only at image build time (untimed). Every PGO step must succeed or the plain build is used.
set -u
BASE="-C target-cpu=cascadelake"
GEN_URL="https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/generator.zip"

plain() {
    RUSTFLAGS="$BASE" cargo build --release --locked --bin fast
}

pgo() (
    set -e
    apt-get update -qq && apt-get install -y -qq unzip > /dev/null
    curl -fsSL --max-time 120 -o /tmp/gen.zip "$GEN_URL"
    mkdir -p /tmp/gen && unzip -q /tmp/gen.zip -d /tmp/gen
    GDIR=$(dirname "$(find /tmp/gen -name Cargo.toml | head -1)")
    (cd "$GDIR" && cargo build --release)
    "$GDIR/target/release/generate" --seed 777 --events 3000000 --clicks 750000 --output /tmp/pgodata
    # Instrumented build + training run (needs the build machine to run this CPU target).
    RUSTFLAGS="$BASE -C profile-generate=/tmp/pgo-raw" cargo build --release --locked --bin fast --target-dir /tmp/tgen
    DATA_DIR=/tmp/pgodata OUTPUT_DIR=/tmp/pgoout NCPU=8 /tmp/tgen/release/fast
    PROFDATA=$(find "$(rustc --print sysroot)" -name llvm-profdata -type f | head -1)
    "$PROFDATA" merge -o /tmp/pgo.profdata /tmp/pgo-raw
    RUSTFLAGS="$BASE -C profile-use=/tmp/pgo.profdata" cargo build --release --locked --bin fast
)

if pgo; then
    echo "pgo-build: profile-guided build OK"
else
    echo "pgo-build: PGO step failed, using the plain build"
    plain
fi
