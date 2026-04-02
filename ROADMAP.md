# graphstream Roadmap

## Deferred from Phase Aether

> After: Phase Aether (completed) · Before: TBD

### Aether-c: ConcurrentUploader migration

hadb-io's `ConcurrentUploader<H>` uses `UploadMessage::Upload(Id) | Shutdown`. graphstream needs `UploadWithAck(PathBuf, oneshot::Sender)` for hakuzu's synchronous replication. Migrate after hadb-io gains ack support.

### Retention (GFS)

`hadb_io::RetentionPolicy` is available via the dependency. Wiring it to replace age-only cache cleanup is deferred; current cleanup works and GFS is more relevant for S3-side segment rotation.

### uploader.rs size

1357 lines (was 1073 pre-migration). Consider splitting `spawn_journal_uploader*` functions into a separate module.
