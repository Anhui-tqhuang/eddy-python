<p align="center">
  <img src="https://raw.githubusercontent.com/Anhui-tqhuang/eddy-python/main/docs/assets/logo-mark.svg" alt="eddy" width="160">
</p>

<h1 align="center">eddy for Python</h1>

<p align="center">Reconcile loops for Python, with Postgres as the only moving part.<br>Job-compatible with <a href="https://github.com/Anhui-tqhuang/eddy">eddy for Go</a>.</p>

---

> **Status:** pre-alpha. The API may still change.

Your API owns plain Postgres rows: the desired state. eddy turns those rows into real resources somewhere else, such as cloud resources, DNS records or third-party SaaS state, and keeps them converged. It's the controller pattern without a Kubernetes API server. Postgres stores the state, carries the queue and elects the leader.

This is the Python library. It stores jobs in the same tables and format as the [Go library](https://github.com/Anhui-tqhuang/eddy), so a Python API can feed a Go controller, or the other way round.

## Install

```sh
pip install eddy-pg        # or: uv add eddy-pg
```

It needs Python 3.14+, and uses psycopg 3 (asyncio) and Pydantic 2.

eddy keeps its jobs in [River](https://riverqueue.com)'s tables, which must exist in your database. Create them in one of two ways:

- Start any Go eddy process on the database; it creates them.
- Run River's CLI: `go install github.com/riverqueue/river/cmd/river@latest`, then `river migrate-up --database-url "$DATABASE_URL"`.

`DB.open` raises `MigrationsMissing` when they are absent.

## Quick start

Describe the table as a Pydantic model, then use one `DB` on both sides.

```python
from typing import Annotated
from uuid import UUID

from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel

import eddy_pg


class App(BaseModel):
    id: Annotated[UUID | None, eddy_pg.Default] = None  # filled by the database
    name: str
    image: str
    phase: Annotated[str, eddy_pg.Status] = ""  # written only by the controller


apps = eddy_pg.Table(App, "apps")

pool = AsyncConnectionPool(DATABASE_URL, open=False)
await pool.open()
db = await eddy_pg.DB.open(pool)
```

**API:** write through the table. The reconcile is enqueued for you, in the same transaction as the row.

```python
app = await apps.insert(db, App(name="web", image="web:1"))  # app.id is filled in
app = await apps.update(db, app.model_copy(update={"image": "web:2"}))  # never touches phase
await apps.delete(db, app.id)
```

**Controller:** run it on any number of replicas. One is elected leader and does the work.

```python
ctl = eddy_pg.Controller(db)


@ctl.reconcile(apps)
async def reconcile_app(app: App) -> None:
    await deploy(app)  # raising retries with backoff
    app.phase = "ready"  # saved when the function returns


@ctl.teardown(apps)
async def teardown_app(app: App) -> None:
    await undeploy(app)  # app is the row as it was when deleted


await ctl.run()
```

If a write commits, its reconcile is guaranteed to happen. If it rolls back, nothing happens. There is no enqueue call to remember, no dual write and no outbox table.

## Writing rows

```python
app = await apps.get(db, app_id)  # raises eddy_pg.NotFound
web = await apps.select(db, "WHERE name = %s ORDER BY id", ("web",))
```

**Transactions.** Table calls take the `DB` (each write is its own transaction) or a `Tx` (writes join it).

```python
async with db.transaction() as tx:
    await teams.insert(tx, team)
    await apps.insert(tx, app)  # both rows and both jobs, or none
    await tx.conn.execute("...")  # your own SQL, same transaction

tx = db.adopt(conn)  # join a psycopg connection you already hold
```

**Free-form SQL.** `Table.execute` runs your own INSERT, UPDATE or DELETE and enqueues every row it touched. Use it for bulk changes and anything the typed methods can't express.

```python
n = await apps.execute(db, "UPDATE apps SET image = %s WHERE team_id = %s", (image, team_id))
```

eddy finds the touched rows by adding a `RETURNING` clause, so the statement must be a single statement without its own `RETURNING` and without a table alias.

**What is not captured.** SQL run directly on a connection, psql and other services write rows without enqueueing anything. Those rows are picked up by the periodic resync, or at once if you call `await db.enqueue(apps.of(app_id))`.

## Reconciling

- **The row is always fresh.** eddy loads it before each call, so the function sees current state however stale the job is. It must be idempotent.
- **Status is saved for you.** Whatever the function assigns to status fields is written back when it finishes, whether it succeeded or failed. With a frozen model, return an updated copy and that is saved. `await apps.save_status(db, app)` saves midway through a long reconcile.
- **Exceptions retry.** Any exception retries with backoff. `raise eddy_pg.Permanent("...")` stops retrying, for a row only a new write can fix. `raise eddy_pg.Retry(15)` checks again in 15 seconds without counting as a failure, for polling something that is still converging.
- **Deletes carry the row.** `delete` sends the row's last state with the job, and the teardown function receives it. You don't need a soft-delete column to know what to tear down. Teardowns retry like reconciles.
- **Every row is re-checked.** Each table is resynced every 10 minutes by default, and when a replica becomes leader. `Controller(db, resync=seconds)` changes the interval; zero turns it off.
- **No overlap per object.** One row is never reconciled twice at once.

eddy can also record how each reconcile went, with two more markers:

```python
class App(BaseModel):
    ...
    reconcile_error: Annotated[str | None, eddy_pg.Error] = None  # last error; cleared on success
    reconciled_at: Annotated[datetime | None, eddy_pg.Reconciled] = None  # time of the last success
```

## Field markers

Columns are the model's fields, by name. Markers go in `Annotated[...]`:

| Marker | Meaning |
|---|---|
| `eddy_pg.Default` | Left out of `insert` when the value is `None`, so the database default applies |
| `eddy_pg.Status` | Never written by `insert` or `update`; saved after each reconcile |
| `eddy_pg.Error` | A status `str \| None` that eddy sets to the last reconcile error |
| `eddy_pg.Reconciled` | A status `datetime \| None` that eddy sets on each success |

`Table(App, "apps", key="uid")` names a key column other than `id`, and `kind="app"` names the job kind differently from the table. Field values must survive `model_dump(mode="json")`, which is how a deleted row reaches teardown.

## Scheduling

Declare how a table is scheduled where you declare it, so the API and the controller agree:

```python
reports = eddy_pg.Table(Report, "reports", max_workers=2, priority=3)
```

- **`max_workers`** gives the kind a queue of its own with that many workers, on top of the shared pool sized by `Controller(db, workers=10)`. A slow kind can't crowd out the others.
- **`priority`** orders kinds that share a queue: 1 (the default) is most urgent, 4 least.

## Without a table

Under the table layer is a smaller core for objects that aren't one row of one table:

```python
clusters = eddy_pg.Kind("cluster", UUID)

async with db.transaction() as tx:
    await tx.conn.execute("...")  # your own SQL
    await tx.enqueue(clusters.of(cluster_id))


@ctl.handle(clusters, resync=600, list_ids=list_cluster_ids)
async def reconcile_cluster(cluster_id: UUID) -> None: ...
```

## Middleware

Middleware wraps every reconcile and teardown, for metrics, tracing or logging. The first one registered is the outermost.

```python
@ctl.middleware
async def timing(ref: eddy_pg.Ref, call_next: eddy_pg.Handler) -> None:
    start = time.perf_counter()
    try:
        await call_next(ref)
    finally:
        observe(ref.kind, ref.deleted, eddy_pg.attempt(), time.perf_counter() - start)
```

`ctl.leading` reports leadership for a readiness probe, and `ctl.on_leadership_change(f)` calls `f(True)` or `f(False)` when it changes. `ctl.stop()` drains in-flight reconciles and makes `run()` return.

## Controller options

| Option | Default | |
|---|---|---|
| `workers` | 10 | Concurrent reconciles on the shared queue |
| `resync` | 600 | Seconds between re-checks of every row; 0 turns it off |
| `reconcile_timeout` | 60 | Seconds allowed for one reconcile or teardown |
| `shutdown_timeout` | 30 | Seconds to drain on stop or lost leadership |
| `elect` | `True` | `False` skips leader election, for a single replica |
| `poll_interval` | 1 | Seconds between checks for new work |
| `lock_retry` | 2 | Seconds between leader-lock attempts and health checks |
| `rescue_after` | 3600 | Seconds before a job left running by a dead worker is retried |

`DB.open(pool, queue="eddy")` sets the queue; every process must agree on it.

## Working with the Go library

The two libraries share the job format, the queue names and the leader lock, and the test suite runs them against each other. That allows:

- **A Python API with a Go controller, or the reverse.** Rows written by one are reconciled by the other, including teardowns of deleted rows.
- **Go and Python controller replicas on the same queue.** They compete for one leader lock. Whichever leads must have a reconciler for every kind in use.

For this to work, declare the same table name, kind name, key column and scheduling settings on both sides, and give both models the same columns.

Three differences to know:

- **River's tables are created by Go tooling**, as described under Install.
- **Python works the job table itself.** River has no Python worker, so this library fetches, retries and cleans up jobs with its own SQL, following River's Go client. A change to River's table layout could require an update here; it was written against River 0.51 (migration 8).
- **Not yet in Python:** per-object locks across processes, embedding in an existing River client, and ready-made OpenTelemetry and Prometheus middleware.

## Development

The project uses [uv](https://docs.astral.sh/uv/).

```sh
make sync      # install dependencies
make lint      # ruff and mypy
make test      # unit tests
make test-db   # everything, against Postgres
               # (TEST_DB_URL=postgres://localhost:5432/eddy_test?sslmode=disable)
```

`make test-db` needs Go on the PATH: a small Go program under `tests/support/gohelper` creates River's tables and plays the Go side of the interop tests.

## Releasing

Pushing a tag such as `v0.1.0` that matches the version in `pyproject.toml` publishes to PyPI through `.github/workflows/publish.yml`. This uses PyPI trusted publishing, which needs a one-time setup on pypi.org: add this repository, the workflow `publish.yml` and the environment `pypi` as a trusted publisher for the `eddy-pg` project.

To publish by hand: `uv build && uv publish`.

## License

MIT. River, whose tables this library uses, is licensed under MPL-2.0.
