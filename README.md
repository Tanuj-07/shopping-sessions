# Shopping Sessions: native Rust pipeline

Builders Buildathon, Shopping Sessions track. Reads the click-stream CSVs from `$DATA_DIR`
and writes `user_daily/` and `campaign_daily/` Parquet tables to `$OUTPUT_DIR`, following `rules.md`.

- `src/bin/fast.rs`: the submitted pipeline (all cores; SIMD line scanning, exact 16-byte ID keys,
  hash-partitioned dedup, lock-free union-find for linked IDs, per-person session buckets).
- `src/bin/reference.rs`: a slow, single-threaded implementation kept as a correctness oracle.
- `src/pq.rs`: a minimal Parquet writer (uncompressed, PLAIN encoding).

## Open-source libraries used

- [rayon](https://crates.io/crates/rayon): thread pool and parallel iterators (MIT/Apache-2.0)
- [memmap2](https://crates.io/crates/memmap2): memory-mapped files (MIT/Apache-2.0)
- [memchr](https://crates.io/crates/memchr): byte search (MIT/Unlicense)
- [rustc-hash](https://crates.io/crates/rustc-hash): FxHash hash maps (MIT/Apache-2.0)

No code written before the event is used.

## License

MIT
