# Shopping Sessions: native Rust pipeline

Builders Buildathon, Shopping Sessions track. Reads the click-stream CSVs from `$DATA_DIR` and
writes `user_daily/` and `campaign_daily/` Parquet tables to `$OUTPUT_DIR`, following `rules.md`.

## Layout

- `src/bin/fast.rs`: the submitted pipeline.
  - Input read in parallel 4 MB chunks (Linux: zero-copy read-only mappings, pages populated in
    bulk per chunk); two-pass SIMD line scanner (AVX2, AVX-512 on the scorer) with quote checks
    done on bitmasks.
  - IDs become exact 8-byte keys (base-37 for `[0-9a-z]` IDs, ordered like the bytes; anything
    else falls back to a hashed key whose bytes are kept, so results stay exact).
  - Fast timestamp parsing (date cache, 8-byte SWAR time, 8-digit epoch milliseconds).
  - Linked IDs: partitioned tables + lock-free union-find (root = smallest ID), run alongside
    event parsing.
  - Dedup on `event_id` in cache-sized hash-partitioned buckets; prefetched person lookups.
  - Sessions: per bucket, events/clicks grouped by person with counting passes, sorted only
    within a person; 7-day click window slides forward with live campaign counts.
  - Minimal Parquet writer (`src/pq.rs`): uncompressed, PLAIN encoding.
- `src/bin/reference.rs`: a slow, single-threaded implementation kept as a correctness oracle.
- `Dockerfile`: plain build for the scorer's CPU family. `Dockerfile.pgo` + `pgo-build.sh`: a
  profile-guided variant (falls back to the plain build on any failure).
- `cargo build --release --features profile` enables measurement switches (`SS_EXP`, `SS_SLAB`,
  `SS_PF`); the normal build compiles them out.

## Open-source code used

- [rayon](https://crates.io/crates/rayon): thread pool and parallel iterators (MIT/Apache-2.0)
- [memchr](https://crates.io/crates/memchr): byte search (MIT/Unlicense)
- [rustc-hash](https://crates.io/crates/rustc-hash): FxHash hash maps (MIT/Apache-2.0)
- [libc](https://crates.io/crates/libc): `mmap`/`madvise` on Linux (MIT/Apache-2.0)
- `Dockerfile.pgo` only: at image build time it downloads the track's public data generator
  (`generator.zip` from the track page) to create a training sample for profile-guided
  optimization. Nothing from it is shipped in the image.

No code written before the event is used.

## License

MIT
