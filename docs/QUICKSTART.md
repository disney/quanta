# QUICKSTART.md

# Quanta Quick Start

## Requirements

Recommended environment:

- Linux or WSL2
- Go 1.22+
- Consul

Recommended minimum memory:

- 16 GB RAM minimum
- 32 GB RAM recommended for TPC-H experimentation

---

# Build Quanta

```bash
make build_all
```

---

# Start Consul

```bash
consul agent -dev -client=0.0.0.0
```

---

# Start Quanta-in-a-Box

```bash
cd start-local
go run .
```

This starts:

- 3 local Quanta data nodes
- Query proxy
- Local cluster services

---

# Verify Cluster Status

```bash
./bin/quanta-admin-linux-amd64 status
```

Expected:

- Cluster UP
- 3 active nodes
- shard allocation visible

---

# Run SQLRunner Demo

```bash
cd sqlrunner
go run . \
  -suite_file sqltests/joins_sql.yaml \
  -host 127.0.0.1 \
  -user MOLIG004 \
  -db quanta \
  -port 4000
```

This performs:

- schema creation
- test data loading
- SQL execution
- complete result-set validation

---

# Connect with MySQL Client

```bash
mysql -h 127.0.0.1 -P <proxy-port> -u root
```

---

# Current Focus

Current project priorities:

- Quanta-in-a-Box stabilization
- TPC-H demo coverage
- Streaming ingestion demonstrations
- SQL correctness improvements
- OSS onboarding simplification

This quick start describes the local QIAB development environment. Container
and multi-host operational requirements are documented in
[`DEPLOYMENT.md`](DEPLOYMENT.md).
