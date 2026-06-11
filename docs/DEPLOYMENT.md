# Quanta Deployment Guide

## Purpose

This document describes Quanta deployment models, operational requirements,
and areas that still require validation.

Quanta-in-a-Box (QIAB) is the preferred development and conformance
environment. It is not the intended limit of production topology.

## Deployment Models

### Quanta-in-a-Box

QIAB runs three data nodes and the query proxy in one process.

It is appropriate for:

- local development
- SQL conformance and integration testing
- demonstrations
- small deployments where one process and one host are acceptable

Its shared process and host lifecycle make it less suitable for large-scale
production deployment or independent node maintenance.

### Containerized Cluster

A containerized deployment should run Consul, the proxy, and each data node as
separate services.

Containers may be replaced during normal operation. Quanta node data must
therefore not exist only in a container writable layer.

Required properties:

- Node data directories use external durable volumes.
- Container replacement preserves node identity and storage mapping.
- Each node has an independent data directory.
- Proxy containers remain stateless with respect to shard data.
- Consul state is deployed according to Consul production guidance rather than
  development-agent behavior.

The repository contains component Dockerfiles, but a complete, validated
container orchestration topology is not currently documented.

### Multi-Host Production Cluster

Large-scale production deployment should support independent Quanta data-node
and proxy processes distributed across hosts.

Expected properties:

- independent node process lifecycles
- durable node-local or attached storage
- stable node identity
- explicit network and firewall configuration
- production Consul deployment
- multiple proxy instances where availability requires them
- operational monitoring, backup, recovery, and replacement procedures

This topology is an architectural target. Its complete operational procedure
and failure behavior still require formal validation.

## Service Topology

The main runtime services are:

- Consul for discovery and cluster coordination
- Quanta data nodes for shards, bitmap data, and BSI data
- Query proxy for the MySQL-compatible SQL endpoint
- ingestion processes as required by the deployment

Development defaults currently include:

- Consul HTTP API on port `8500`
- MySQL-compatible proxy on port `4000`

Additional Consul, node, metrics, profiling, and ingestion ports must be
inventoried before publishing a production firewall specification.

## Persistent Storage

Quanta data nodes serialize shard data beneath their configured data
directories. That storage is part of the node's durable state.

Non-negotiable requirements:

- Node data directories must be external durable storage.
- Node-local data must not reside only in an ephemeral container layer.
- Each node must mount the storage associated with its identity.
- Restarting a process must preserve its data directory.
- Replacing a container or host must use an explicit recovery or remapping
  procedure.
- Storage permissions and ownership must permit the Quanta process to read,
  write, and reload serialized shards.

The current QIAB development data is stored under:

```text
test/localClusterData/quanta-node-<n>/data
```

This path is useful for local testing but is not a production storage
recommendation.

## Node Identity

Node identity and durable storage must be managed together.

A node must not accidentally start with:

- another node's data directory
- an empty volume when existing data is expected
- the same writable data directory as another active node

The exact supported procedure for replacing a failed node while retaining or
changing node identity remains to be documented and validated.

## Consul Requirements

QIAB currently uses a local Consul development agent:

```bash
consul agent -dev
```

Consul development mode is not appropriate for production.

A production deployment requires:

- a durable, highly available Consul deployment
- appropriate ACL and transport-security configuration
- documented service-registration and health-check behavior
- backup and recovery procedures for Consul state
- startup sequencing that prevents nodes and proxies from assuming an
  incomplete cluster is ready

## Startup and Shutdown

The development startup order is:

1. Start Consul.
2. Start Quanta data nodes.
3. Wait for the nodes to become active and the cluster to become healthy.
4. Start or expose the query proxy.
5. Start ingestion and client workloads.

Shutdown should stop writes and ingestion before stopping nodes. QIAB performs
node and proxy shutdown through its local harness, but production shutdown and
drain semantics require further validation.

## Restart, Replacement, and Recovery

These are distinct operations:

- **Restart:** the same node identity and durable data directory return.
- **Container replacement:** a new container receives the prior node's identity
  and durable volume.
- **Host replacement:** storage and identity must be deliberately restored or
  reassigned.
- **Node replacement:** a failed node is removed and its shard responsibility
  is recovered or redistributed.

Verified in QIAB:

- a three-node cluster can restart from serialized shard data
- shard counts are restored
- representative SQL results match before and after restart

Not yet formally validated:

- unclean process or host failure
- partial writes during failure
- single-node replacement
- volume restoration on a different host
- rolling restart
- rolling upgrade
- Consul outage and recovery

## Scaling

QIAB uses a fixed three-node development cluster. Production scaling behavior
must be tested independently.

Validation is needed for:

- adding a node
- removing a node
- shard redistribution
- proxy scaling
- ingestion behavior during topology changes
- query correctness while the cluster converges

## Backup and Restore

Backup and restore procedures are not yet formally defined.

Before production use, the project must establish:

- whether node storage can be snapshotted while nodes are active
- whether writes must be quiesced
- consistency requirements across node volumes and Consul
- restoration ordering
- restoration to original or replacement node identities
- post-restore validation

A backup is not considered valid until a restore test has succeeded.

## Observability

Production deployment should capture:

- structured logs from each data node
- proxy logs
- Consul health and membership
- cluster state and active-node count
- shard distribution
- memory consumption
- query errors and latency
- persistence and reload errors
- ingestion lag and failures

QIAB currently combines node and proxy logs in one process. Separate-process
deployments should preserve component identity in every log stream.

## Security

Production security work must include:

- Consul ACLs and transport security
- network exposure and firewall rules
- proxy authentication and authorization
- secret distribution and rotation
- least-privilege filesystem permissions
- TLS requirements for client and service communication
- container and host process isolation

The local development defaults are not a production security profile.

## Deployment Validation

The SQL roadmap and integration suites should be environment-neutral and run
through the proxy against:

- QIAB
- containerized clusters
- multi-host clusters

The same deterministic schemas, data, and expected results should be used
wherever possible. Deployment-specific suites should additionally validate
restart, replacement, persistence, and topology changes.

## Current Limitations

- QIAB shares one process and one host lifecycle.
- Current Docker artifacts do not constitute a complete durable deployment.
- Production storage mapping and node replacement are not formally specified.
- Backup and restore are not formally validated.
- Rolling restart and upgrade behavior are not formally validated.
- Production Consul configuration is not documented.
- A complete port and firewall inventory is not documented.

## Operational Validation Backlog

1. Define container volume and node-identity conventions.
2. Build a reproducible multi-container deployment with durable volumes.
3. Verify clean restart and container replacement.
4. Test unclean node termination and recovery.
5. Test adding and removing nodes.
6. Define and test backup and restore.
7. Validate rolling restart and upgrade behavior.
8. Publish the production port, health-check, and security inventory.

The existing deployment architecture image is available at
[`Quanta_Deployment_Architecture.png`](Quanta_Deployment_Architecture.png).
