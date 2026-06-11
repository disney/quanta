# Quanta Status

## Current State

Quanta is currently in active re-engagement and stabilization.

The immediate focus is on:

- Quanta-in-a-Box local development/runtime environment
- startup simplification
- documentation improvements
- reproducible demos
- TPC-H analytical validation
- streaming ingestion demonstrations
- rebuilding OSS community engagement

Recent work has focused on restoring local cluster startup, SQL correctness, shard persistence/reload behavior, and repeatable integration testing.

Current status:

- Local 3-node in-process cluster startup is operational
- Proxy startup is operational
- SQLRunner YAML roadmap suites are operational
- Shard allocation and persistence are operational
- Restart/recovery testing is operational
- TPC-H-oriented query validation is partially operational
- Join correctness issues have been significantly stabilized

QIAB is the preferred development environment, not the intended limit of
production topology.

Deployment models, persistent-storage requirements, and the operational
validation backlog are tracked in [`DEPLOYMENT.md`](DEPLOYMENT.md).

The legacy SQLRunner scripts have been migrated into additive YAML roadmap
suites. The suites validate result values as well as counts, preserve
unsupported behavior as `xfail` goals, and fail on unexpected improvements
until those cases are reviewed and promoted. The legacy line-oriented runner
remains available for lower-level test fragments.

This behavior map is being built without engine reorganization. Once existing
behavior and roadmap gaps are mapped, the project will explicitly decide
between incremental SQL-layer extension and refactoring.
