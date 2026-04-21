# graphstream Roadmap

## Phase GraphFjord: Finish hadb-io → hadb-storage migration in tests

> After: Phase GraphForge

Phase Aether dropped the `hadb-io` dependency from the library and the
uploader, but `tests/dst.rs` and `tests/e2e.rs` still import
`hadb_io::ObjectStore` / `hadb_io::S3Backend`. They no longer compile
against current hadb. Finish the migration so the test suite is green
again, and bump to 0.4.0 to track the rest of the ecosystem.

### a. Tests on hadb-storage

- [ ] `tests/dst.rs`: replace the `impl hadb_io::ObjectStore for FaultyObjectStore` with `impl hadb_storage::StorageBackend`. Adapt method signatures (`get`, `put`, `put_if_absent`, `put_if_match`, `delete`, `list`) to the StorageBackend shape, preserving the fault-injection wrappers
- [ ] `tests/e2e.rs`: drop `hadb_io::S3Backend`, construct `hadb_storage_s3::S3Storage` from an `aws-sdk-s3::Client`. Keep the `S3_TEST_BUCKET` / `S3_ENDPOINT` env-var contract
- [ ] `Cargo.toml`: add `hadb-storage` (and `hadb-storage-s3`, `aws-sdk-s3`, `aws-config` for e2e) to `dev-dependencies`. Do not add them to runtime deps — graphstream stays trait-only at the library layer

### b. Verify the tests still test what they used to

- [ ] DST harness still exercises fault injection (transient errors, partial writes, silent corruption) — translate each fault hook rather than dropping it
- [ ] e2e still hits real S3 with `--ignored` (bucket via env). No mock substitution

### c. Version bump

- [ ] `Cargo.toml`: graphstream 0.3.0 → 0.4.0
- [ ] Drop the stale `hadb_io` paragraphs from README.md (they reference a dep we no longer have)

> **Note:** the `hadb-changeset` dep stays at 0.3.2 — it lives in the standalone
> [russellromney/hadb-changeset](https://github.com/russellromney/hadb-changeset)
> repo and is the only crate currently shipping the `journal` feature
> (the in-tree `hadb/hadb-changeset` workspace member at 0.4.0 doesn't yet).
> Re-aligning these is its own future phase, not part of GraphFjord.

---

## Phase Torrent: DST + Property Tests

Close the testing gap with walrust. graphstream has 86 tests but no chaos/property testing.

### a. Property tests
- [ ] Journal write/read round-trip with arbitrary entries (proptest)
- [ ] Chain hash integrity under partial writes (crash mid-segment)
- [ ] Segment rotation correctness (boundary conditions on max_segment_bytes)
- [ ] Upload idempotency (same segment uploaded twice produces identical S3 state)

### b. DST harness
- [ ] `MockObjectStore` with fault injection (disk full, S3 timeout, partial write, silent corruption)
- [ ] Simulate: writer + uploader + downloader under concurrent faults
- [ ] Verify invariants after each simulation run

### c. Core invariants
- [ ] (1) Journal continuity: no sequence gaps after recovery
- [ ] (2) Chain hash validity: every segment's prev_hash matches prior segment's hash
- [ ] (3) Upload completeness: every sealed segment eventually uploaded
- [ ] (4) Recovery from arbitrary segment loss: reader skips missing, resumes from next
- [ ] (5) Compaction preserves all entries (streaming compaction output = input entries)

---

## Deferred from Phase Aether

### Retention (GFS)

`hadb_io::RetentionPolicy` is available via the dependency. Wiring it to replace age-only cache cleanup is deferred; current cleanup works and GFS is more relevant for S3-side segment rotation.

### uploader.rs size

1357 lines (was 1073 pre-migration). Consider splitting `spawn_journal_uploader*` functions into a separate module.
