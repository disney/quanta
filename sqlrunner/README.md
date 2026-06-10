# SQLRunner

SQLRunner executes SQL against a running Quanta proxy. Its primary validation
format is a YAML roadmap suite that can check complete result sets, column
names, database types, affected rows, and expected errors.

## Roadmap suites

Run from the `sqlrunner` directory:

```bash
go run . \
  -suite_file sqltests/joins_sql.yaml \
  -host 127.0.0.1 \
  -user MOLIG004 \
  -db quanta \
  -port 4000
```

The migrated suites are:

- `sqltests/basic_queries.yaml`
- `sqltests/insert_tests.yaml`
- `sqltests/mutate_tests_body.yaml`
- `sqltests/joins_sql.yaml`

Cases are marked `supported`, `xfail`, or `skip`. A supported case must pass.
An `xfail` case records desired behavior that is not yet correct. If an
`xfail` case begins passing, SQLRunner reports `XPASS` and fails the suite
until its expectations are reviewed and the case is promoted.

See [`roadmap/FORMAT.md`](roadmap/FORMAT.md) for the complete format.

## Legacy scripts

The line-oriented `.sql` runner remains available for compatibility with
existing test fragments:

```bash
go run . \
  -script_file sqlscripts/joins_sql.sql \
  -validate \
  -host 127.0.0.1 \
  -user MOLIG004 \
  -db quanta \
  -port 4000
```

Legacy validation checks row counts using the `@` delimiter. New SQL
conformance and roadmap coverage should use YAML suites.

## Connection options

- `host`: Quanta proxy host. Required.
- `user`: Quanta user. Required.
- `password`: Quanta password.
- `db`: database name; defaults to `quanta`.
- `port`: proxy port; defaults to `4000`.
- `consul`: Consul address; defaults to `127.0.0.1:8500`.
- `log_level`: use `DEBUG` for verbose logging.
- `repeats`: legacy script repetitions; defaults to `1`.

SQLRunner expects Consul and a green three-node local cluster to be running.
