# SQL Roadmap Suite Format

SQL roadmap suites use YAML and describe externally visible behavior. They do
not prescribe how Quanta implements a feature.

```yaml
version: 1
name: joins

tests:
  - id: joins.inner.rows
    status: supported
    kind: query
    order: rowsort
    capabilities: [inner-join]
    sql: |
      select c.cust_id, o.order_id
      from customers_qa as c
      inner join orders_qa as o on o.cust_id = c.cust_id
    expect:
      columns: [cust_id, order_id]
      rows:
        - ["1", "1001"]
        - ["1", "1002"]

  - id: aggregates.multiple
    status: xfail
    kind: query
    capabilities: [multiple-aggregates]
    issue: "roadmap"
    sql: select count(*), max(order_id) from orders_qa
    expect:
      rows:
        - [4, "1004"]
```

## Case status

- `supported`: must pass.
- `xfail`: retained roadmap goal. A failure is reported as `XFAIL`; success is
  reported as `XPASS` and makes the suite fail until the case is promoted.
- `skip`: not executed.

## Case kind

- `query`: validates complete rows and optional column names and database types.
- `statement`: validates success, an expected error, or `affected_rows`.
- `admin`: invokes a Quanta administration command through an executor supplied
  by the launcher.

`kind` may be omitted for statements beginning with `select` or `show`.

## Result ordering

- `exact`: rows must be returned in the listed order.
- `rowsort`: expected and actual rows are sorted before comparison.

Use `exact` when ordering is part of the SQL contract. Use `rowsort` when row
order is intentionally unspecified.

## Values and errors

YAML `null` represents SQL `NULL`. Other values are compared through their
database text representation. Expected errors use a case-insensitive substring:

```yaml
expect:
  error_contains: attribute 'missing' not found
```

During migration from legacy SQLRunner scripts, a query may use:

```yaml
expect:
  row_count: 3
```

`row_count` preserves existing coverage but is weaker than `rows`. New and
expanded tests should prefer complete expected rows.
