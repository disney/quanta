# ARCHITECTURE.md

# Quanta Architecture Overview

## Overview

Quanta is a distributed bitmap-oriented analytical processing engine optimized for:

- high-speed filtering
- distributed joins
- analytical aggregation
- streaming ingestion
- bitmap and BSI processing

---

# Major Components

## Data Nodes

Quanta data nodes:

- own shards
- store bitmap and BSI data
- execute bitmap operations
- participate in distributed query execution

Data nodes are shard-aware and coordinated through Consul.

---

## Query Proxy

The query proxy:

- accepts SQL requests
- performs SQL parsing/planning
- generates logical query plans
- coordinates distributed query execution

The proxy currently exposes a MySQL-compatible interface.

---

## Logical Query Layer

SQL queries are translated into an intermediate logical query representation before execution.

This layer separates:

- SQL syntax/planning
from:
- bitmap execution semantics

---

## Bitmap Execution Engine

The bitmap execution engine is the core architectural focus of Quanta.

Current capabilities include:

- bitmap filtering
- BSI processing
- joins
- aggregate operations
- top-N analytics
- distributed shard execution

---

## Consul Integration

Consul is used for:

- node discovery
- cluster coordination
- shard metadata
- service registration

---

## Ingestion

Quanta supports:

- streaming ingestion
- SQL INSERT
- bulk loading
- Parquet export/import workflows

Current ingestion work is focused on:

- simplified local workflows
- TPC-H loading
- streaming demonstrations

---

# Query Flow

```text
SQL
  ↓
SQL parser/planner
  ↓
Logical query representation
  ↓
Distributed bitmap execution
  ↓
Shard/node aggregation
  ↓
Result assembly
```

---

# Quanta-in-a-Box

Quanta-in-a-Box (QIAB) provides:

- local 3-node cluster startup
- local proxy startup
- integration test environment
- reproducible development workflows

The current implementation uses:

- start-local
- Ensure_Cluster
- in-process node startup

QIAB is the preferred development and conformance environment. It is not the
intended limit of production topology. Containerized and multi-host deployment
requirements are documented in [`DEPLOYMENT.md`](DEPLOYMENT.md).

---

# Current Architectural Priorities

- startup simplification
- query correctness stabilization
- TPC-H analytical validation
- streaming demo workflows
- SQL support expansion
- ingestion cleanup
