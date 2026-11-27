<p align="center">
  <img src="https://raw.githubusercontent.com/Anhui-tqhuang/eddy-python/main/docs/assets/logo-mark.svg" alt="eddy" width="160">
</p>

<h1 align="center">eddy for Python</h1>

<p align="center">Reconcile loops for Python, with Postgres as the only moving part.<br>Job-compatible with <a href="https://github.com/Anhui-tqhuang/eddy">eddy for Go</a>.</p>

<p align="center"><a href="https://pypi.org/project/eddy-pg/">pypi.org/project/eddy-pg</a></p>

<p align="center">
  <a href="https://pepy.tech/projects/eddy-pg"><img src="https://static.pepy.tech/personalized-badge/eddy-pg?period=total&units=INTERNATIONAL_SYSTEM&left_color=BLACK&right_color=GREEN&left_text=downloads" alt="PyPI Downloads"></a>
</p>

---

> **Status:** pre-alpha. The API may still change.

Your API owns plain Postgres rows: the desired state. eddy turns those rows into real resources somewhere else, such as cloud resources, DNS records or third-party SaaS state, and keeps them converged. It's the controller pattern without a Kubernetes API server. Postgres stores the state, carries the queue and elects the leader.

This is the Python library. It stores jobs in the same tables and format as the [Go library](https://github.com/Anhui-tqhuang/eddy), so a Python API can feed a Go controller, or the other way round.

## Install

```sh
pip install eddy-pg        # or: uv add eddy-pg
```

The package is published on PyPI as [`eddy-pg`](https://pypi.org/project/eddy-pg/) and imported as `eddy_pg`.

It needs Python 3.14+, and uses psycopg 3 (asyncio) and Pydantic 2. Nothing else has to be installed: eddy keeps its jobs in [River](https://riverqueue.com)'s tables, and `DB.open` creates them.

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
db = await eddy_pg.DB.open(pool)  # creates eddy's job tables if needed
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

**One waiting job per object.** A write to an object whose reconcile already waits to run uses that job, and brings it forward if it waits for later (a `Retry`, or backoff after a failure); the reconcile still runs after the write. A write while the object's reconcile is running does enqueue, since that run may have read the row before the write. A resync skips objects whose job already waits, and a reconcile that raises `Retry` while another job for its object waits completes instead. So an object that keeps asking for a `Retry` has one retry loop, however many resyncs and writes come by. Deletions are never merged. The Go library does the same, so the two can share a queue.

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

## Soft delete

eddy has no soft-delete setting. A soft delete is an ordinary update that the reconciler acts on. Use it when the row should stay visible while its resources are torn down, or when a failed teardown should show in the row's status columns.

```python
class App(BaseModel):
    ...
    deleted_at: datetime | None = None  # set by the API; an ordinary column


# API: mark the row. Its reconcile is enqueued like any other write.
await apps.execute(db, "UPDATE apps SET deleted_at = now() WHERE id = %s", (app_id,))


# Controller: tear down, then remove the row.
@ctl.reconcile(apps)
async def reconcile_app(app: App) -> None:
    if app.deleted_at is not None:
        await undeploy(app)  # raising retries; the row and its error stay visible
        async with db.pool.connection() as conn:
            await conn.execute("DELETE FROM apps WHERE id = %s", (app.id,))
        return
    ...
```

- **Remove the row with plain SQL on a connection.** It enqueues nothing, so the table needs no teardown function. `apps.delete` would enqueue a teardown for work that is already done.
- **The teardown branch must be idempotent.** Until the row is removed, the resync keeps reconciling it.
- **To keep the row for good,** skip the final `DELETE`, record that teardown finished in a status field, and return early when it is set.
- **Filter your own reads.** `get` and `select` return soft-deleted rows; add `deleted_at IS NULL` where the API should hide them.

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

## Metrics and tracing

Ready-made instrumentation ships as optional extras, so the core depends on neither library. Names and labels match the Go library's, so both look the same in your backend.

**OpenTelemetry** (`pip install "eddy-pg[otel]"`): a span and a duration measurement per reconcile.

```python
from eddy_pg.otel import middleware

ctl.middleware(middleware())  # uses the global providers by default
```

Spans are named `eddy.reconcile <kind>` or `eddy.teardown <kind>` and carry `eddy.kind`, `eddy.id`, `eddy.attempt` and `eddy.outcome`. The histogram is `eddy.reconcile.duration`, in seconds. Spans the reconciler starts become children.

**Prometheus** (`pip install "eddy-pg[prometheus]"`):

```python
from eddy_pg.prometheus import Metrics

metrics = Metrics()  # registers with the default registry
ctl.middleware(metrics.middleware)
ctl.on_leadership_change(metrics.set_leading)
```

| Metric | Labels | |
|---|---|---|
| `eddy_reconcile_duration_seconds` | `kind`, `operation`, `outcome` | Histogram of reconcile and teardown durations |
| `eddy_reconciles_in_flight` | `kind` | Reconciles running now |
| `eddy_leader` | — | 1 while this replica leads |

In both, the outcome is `ok`, `retry` (asked to be checked again later), `error` (will be retried) or `permanent`. `eddy_pg.outcome(exc)` gives the same classification to your own middleware.

**Your own middleware** wraps every reconcile and teardown the same way. The first one registered is the outermost.

```python
@ctl.middleware
async def timing(ref: eddy_pg.Ref, call_next: eddy_pg.Handler) -> None:
    start = time.perf_counter()
    try:
        await call_next(ref)
    finally:
        observe(ref.kind, ref.deleted, eddy_pg.attempt(), time.perf_counter() - start)
```

`ctl.leading` reports leadership for a readiness probe, and `ctl.on_leadership_change(f)` calls `f(True)` or `f(False)` when it changes.

## Running inside your app

`ctl.run()` is an ordinary coroutine, so the controller can live in the same process as your API. `ctl.stop()` drains in-flight reconciles and makes `run()` return.

```python
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    task = asyncio.create_task(ctl.run())
    yield
    ctl.stop()
    await task
```

Every replica can run it this way: they elect one leader between them. To have every replica work the queue instead, pass `elect=False` together with `object_locks=True`, which takes a Postgres advisory lock per row so that no two processes reconcile the same row at once.

## Migrations

`DB.open` creates and upgrades River's tables, and is safe to call from several processes at once. To run migrations as a separate step:

```python
await eddy_pg.migrate(pool)  # in your migration step
db = await eddy_pg.DB.open(pool, migrate=False)  # in the app; raises MigrationsMissing if absent
```

The tables are created from River's own migration files, bundled unmodified, so the result is identical to a database migrated by River or by the Go library.

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
| `object_locks` | `False` | Per-row advisory lock across processes; holds one extra pool connection per running reconcile |

`DB.open(pool, queue="eddy")` sets the queue; every process must agree on it.

## Working with the Go library

The two libraries share the job format, the queue names and the leader lock, and the test suite runs them against each other. That allows:

- **A Python API with a Go controller, or the reverse.** Rows written by one are reconciled by the other, including teardowns of deleted rows.
- **Go and Python controller replicas on the same queue.** They compete for one leader lock. Whichever leads must have a reconciler for every kind in use.

For this to work, declare the same table name, kind name, key column and scheduling settings on both sides, and give both models the same columns.

Two differences to know:

- **Python works the job table itself.** River has no Python worker, so this library fetches, retries and cleans up jobs with its own SQL, following River's Go client. A change to River's table layout could require an update here; it was written against River 0.51 (migration 8).
- **There is no `Embed`.** The Go library can hand its workers to a River client your app already runs. River has no Python worker client to hand them to, so the equivalent here is running the controller in your own process, as described under "Running inside your app".

## Development

The project uses [uv](https://docs.astral.sh/uv/).

```sh
make sync      # install dependencies
make lint      # ruff and mypy
make test      # unit tests
make test-db   # everything, against Postgres
               # (TEST_DB_URL=postgres://localhost:5432/eddy_test?sslmode=disable)
```

The interop tests also need Go on the PATH: a small Go program under `tests/support/gohelper` plays the Go side. Without Go they are skipped.

## River version

eddy-python is built against River **v0.51.2**. That is the version of the migration files bundled in `src/eddy_pg/river_migrations`, and the version the job SQL in `src/eddy_pg/_river.py` was written for.

> **Keep this repository and [eddy](https://github.com/Anhui-tqhuang/eddy) (Go) on the same River version.** The two libraries share River's tables, so they must agree on the table layout.

Upgrading River here can need code changes, not just new files. River's Go client is the only official worker, and this library reimplements what it does. When upgrading:

1. Upgrade the Go library first (its `go.mod`), and note the new River version.
2. Run `scripts/sync-river-migrations.sh <version>` to copy that version's migration files. Never edit them by hand.
3. Read River's changelog and the new migration files for changes to `river_job` or to how jobs are fetched, completed, retried, snoozed or rescued. Update `src/eddy_pg/_river.py` to match, and raise `MIN_VERSION` in `src/eddy_pg/_migrate.py` if the code now needs a newer layout.
4. Point `tests/support/gohelper/go.mod` at a Go eddy commit that uses the new River version.
5. Run `make test-db` with Go installed. The interop tests are the check that matters: they compare this library's migration with River's own and run both libraries against one database.
6. Change the version stated in this section and in `src/eddy_pg/river_migrations/NOTICE.md`.

## Releasing

Pushing a tag such as `v0.1.0` that matches the version in `pyproject.toml` publishes to PyPI through `.github/workflows/publish.yml`. This uses PyPI trusted publishing, which needs a one-time setup on pypi.org: add this repository, the workflow `publish.yml` and the environment `pypi` as a trusted publisher for the `eddy-pg` project.

To publish by hand: `uv build && uv publish`.

## License

MIT, except for River's migration files in `src/eddy_pg/river_migrations`, which are bundled unmodified under River's licence, MPL-2.0 (see the `LICENSE` and `NOTICE.md` in that directory).
