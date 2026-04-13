# graphstream Roadmap

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
