# spark-connect-go

> The [datalake-go](https://github.com/datalake-go) fork of Apache's Spark Connect Go client. Adds a `database/sql` driver, generic typed helpers (`Collect[T]`, `Stream[T]`, `First[T]`), and exposed gRPC dial options on top of the upstream gRPC client. Tracks `apache/spark-connect-go`; deltas are intended to upstream.

Spark Connect is Spark's [language-neutral gRPC protocol](https://spark.apache.org/docs/latest/spark-connect-overview.html). The upstream Go client is the official reference implementation; this fork carries the extras the rest of the [datalake-go](https://github.com/datalake-go) ecosystem needs while those patches work their way upstream.

## Why the fork

The upstream client is correct and minimal. Building an ORM and a composed runtime on top of it needs a handful of things that aren't in the upstream surface yet:

- **`database/sql` driver.** A DSN-shaped entrypoint so existing Go tooling (goose, sqlc, pgx-consumer code) can speak Spark Connect without a wrapper layer. Driver name is `spark`; it speaks `sc://host:port` DSNs and supports `$N` positional parameters (rendered client-side into Spark SQL literals — the native parameter proto isn't reliable across every supported Spark version).
- **Generic typed helpers.** `Collect[T]`, `Stream[T]`, `First[T]` — Go generics over the untyped `DataFrame`. The upstream surface is untyped by design; these are thin wrappers over `As[T]` + `DataFrameOf[T]`.
- **Exposed gRPC dial options.** `SparkSessionBuilder.WithDialOptions(opts ...grpc.DialOption)` — auth interceptors, TLS config, observability handlers wire in without subclassing.
- **`IsClusterNotReady` error surface.** A typed error (`sparkerrors.ErrClusterNotReady` / `IsClusterNotReady(err)`) the caller can check instead of string-matching. Databricks serverless clusters take 30–90s to warm, and retry logic up the stack needs a reliable signal.

Every delta is tracked as a PR against this fork and queued for upstream. When a delta lands in `apache/spark-connect-go`, we drop it from the fork. The long-term goal is zero deltas.

## What this repo is

A drop-in replacement for `github.com/apache/spark-connect-go` at the same package names. Swap the import path and existing code compiles; the session API, DataFrame surface, and protobuf stubs are unchanged.

```go
import (
    sparksql "github.com/datalake-go/spark-connect-go/spark/sql"
    _        "github.com/datalake-go/spark-connect-go/spark/sql/driver" // registers "spark" for database/sql
)
```

(The `sparksql` alias avoids collision with stdlib `database/sql` — the actual package name is `sql`.)

You can use this fork without lake-orm or lakehouse. The composed runtime ([lakehouse](https://github.com/datalake-go/lakehouse)) and the ORM ([lake-orm](https://github.com/datalake-go/lake-orm)) depend on this fork's deltas; nothing in this fork requires them.

## Quick start

```go
session, err := sparksql.NewSessionBuilder().
    Remote("sc://spark.internal:15002").
    Build(ctx)
if err != nil { /* ... */ }
defer session.Stop()

df, _ := session.Sql(ctx, "SELECT id, email FROM users WHERE tier = 'gold'")
_ = df.Show(ctx, 20, false)
```

### Typed reads

```go
type User struct {
    ID    string `spark:"id"`
    Email string `spark:"email"`
}

df, _  := session.Sql(ctx, "SELECT id, email FROM users WHERE tier = 'gold'")
users, _ := sparksql.Collect[User](ctx, df)

// or one row:
alice, err := sparksql.First[User](ctx, df)
if errors.Is(err, sparksql.ErrNotFound) { /* 404 */ }

// or streaming:
for row, rerr := range sparksql.Stream[User](ctx, df) {
    if rerr != nil { break }
    // use row
}
```

### `database/sql` driver

```go
import (
    "database/sql"
    _ "github.com/datalake-go/spark-connect-go/spark/sql/driver"
)

db, _ := sql.Open("spark", "sc://spark.internal:15002")
rows, _ := db.QueryContext(ctx, "SELECT id FROM users WHERE tier = $1", "gold")
```

`$N` placeholders are rendered client-side into Spark SQL literals with type-aware quoting (strings, numbers, bools, `[]byte`, `time.Time`). `?` placeholders aren't supported — most `database/sql`-adjacent codegen (sqlc, goose dialects, pgx patterns) emits `$N`, so the narrower grammar keeps the renderer simple.

## Features (deltas over upstream)

- **`database/sql` driver.** `sql.Open("spark", "sc://...")` — any `database/sql` consumer (goose, sqlc-generated code, ad-hoc scripts) plugs in. Registered under name `spark` in `spark/sql/driver`.
- **Typed helpers over `DataFrame`.** `Collect[T]`, `Stream[T]`, `First[T]`, `As[T]`, `DataFrameOf[T]`. Struct tag is `spark:"<column>"`. Decode rows into struct types at the materialization edge.
- **`SparkSessionBuilder.WithDialOptions`.** gRPC dial options exposed on the builder. Wire auth interceptors, TLS, observability without wrapping the builder.
- **`sparkerrors.IsClusterNotReady(err)`.** Classified error for cluster cold-start states. lake-orm uses it upstack for retry decisions.
- **Upstream parity.** Tracks `apache/master`; upstream merges flow through periodically with the fork's commits rebased on top.

## Install

```bash
go get github.com/datalake-go/spark-connect-go
```

Requires a Spark Connect server (Spark 3.4+). See [lake-k8s](https://github.com/datalake-go/lake-k8s) for a pre-baked Spark 4.0 + Iceberg + Delta image and a `docker compose up` laptop stack.

## Building from source

```bash
git clone https://github.com/datalake-go/spark-connect-go.git
cd spark-connect-go
make && make test
```

Regenerating protobuf stubs from the Spark submodule:

```bash
git submodule update --init --recursive
make gen && make test
```

## Related

- [lake-orm](https://github.com/datalake-go/lake-orm) — ORM that uses this fork's typed helpers and `database/sql` driver.
- [lakehouse](https://github.com/datalake-go/lakehouse) — composed runtime that wires this session alongside the ORM, migrations, and dashboard.
- [lake-k8s](https://github.com/datalake-go/lake-k8s) — pre-baked Spark Connect server + laptop-mode compose stack.
- [apache/spark-connect-go](https://github.com/apache/spark-connect-go) — the upstream project this fork tracks.

## Contributing

Feature work that could land upstream should be proposed against `apache/spark-connect-go` first. Fork-only changes (anything that wouldn't be accepted upstream) stay on this tree. See [CONTRIBUTING.md](CONTRIBUTING.md).
