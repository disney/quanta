# DEVELOPMENT.md

# Quanta Development Guide

## Build

```bash
make build_all
```

---

# Run Tests

```bash
make test
```

---

# Integration Testing

Start Quanta-in-a-Box:

```bash
cd start-local
go run .
```

Then run SQLRunner:

```bash
cd sqlrunner
go run . \
  -suite_file sqltests/joins_sql.yaml \
  -host 127.0.0.1 \
  -user MOLIG004 \
  -db quanta \
  -port 4000
```

---

# Important Development Notes

## Quanta-in-a-Box

The preferred development environment is:

- local
- in-process
- reproducible
- low-friction

Most development should happen against QIAB.

QIAB is a development and conformance topology. Changes that affect node
identity, persistence, networking, lifecycle, or recovery must also be
considered against the deployment models in
[`DEPLOYMENT.md`](DEPLOYMENT.md).

---

## SQL Layer

The SQL layer currently supports a practical analytical SQL subset.

The bitmap execution engine is the primary architectural focus.

Avoid broad SQL/parser rewrites unless necessary.

---

## qlbridge

Quanta currently contains a modified/forked qlbridge implementation.

Known technical debt areas include:

- join logic
- aggregate planning
- GROUP BY behavior
- query edge cases

Stabilization is preferred over broad refactoring.

---

# Current Development Priorities

1. QIAB stabilization
2. TPC-H support
3. SQL correctness
4. Streaming demo restoration
5. Documentation improvements
6. OSS onboarding simplification

---

# Known Technical Debt

- commented-out tests
- dead code cleanup
- startup ergonomics
- ingestion abstraction cleanup
- SQL planner complexity
- qlbridge maintenance burden

---

# Recommended Workflow

```bash
make build_all
make test

cd start-local
go run .

cd sqlrunner
go run . -suite_file sqltests/joins_sql.yaml \
  -host 127.0.0.1 -user MOLIG004 -db quanta -port 4000
```

The goal is to maintain:

- reproducible local startup
- repeatable query validation
- stable integration behavior

The YAML suites under `sqlrunner/sqltests` are the SQL behavior map. Supported
cases protect current behavior; `xfail` cases retain roadmap goals without
blocking incremental engine work. The legacy scripts remain for compatibility
with lower-level test fragments.

## Legacy SQLRunner Compatibility Debt

Some retention, restart, topology, and Docker integration tests still use the
legacy line-oriented scripts under `sqlrunner/sqlscripts`. In particular, they
depend on reusable load, body, and bug-reproduction fragments that are separate
from the primary SQL conformance suites.

For now:

- new SQL behavior and roadmap coverage must use YAML suites
- existing legacy fragments should remain until their owning tests are revised
- new legacy scripts should not be added
- legacy parser and script removal should happen as part of a focused topology
  or test-infrastructure refactor

This is intentional compatibility debt. Migrating these specialized fixtures
now would add risk without materially improving the SQL implementation roadmap.
