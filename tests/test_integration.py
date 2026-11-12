"""Tests against Postgres, with River's tables in place."""

from __future__ import annotations

import asyncio
from typing import Annotated, Any
from uuid import UUID, uuid4

import pytest
from hamcrest import (
    assert_that,
    contains_string,
    equal_to,
    greater_than_or_equal_to,
    has_length,
    is_,
    none,
    not_none,
)
from pydantic import BaseModel, ConfigDict

import eddy_pg

from .conftest import (
    App,
    apps,
    controller,
    eventually,
    job_count,
    open_pool,
    running,
    scalar,
)

pytestmark = pytest.mark.integration


class TestMigrate:
    async def test_open_without_migrating_raises_migrations_missing(
        self, database_url: str, empty_schema: str
    ) -> None:
        pool = await open_pool(database_url, empty_schema)
        try:
            with pytest.raises(eddy_pg.MigrationsMissing, match=r"eddy_pg\.migrate"):
                await eddy_pg.DB.open(pool, migrate=False)
        finally:
            await pool.close()

    async def test_migrate_creates_rivers_tables_once(
        self, database_url: str, empty_schema: str
    ) -> None:
        pool = await open_pool(database_url, empty_schema)
        try:
            first = await eddy_pg.migrate(pool)
            second = await eddy_pg.migrate(pool)
            db = await eddy_pg.DB.open(pool, migrate=False)
            recorded = await scalar(
                db, "SELECT array_agg(version ORDER BY version) FROM river_migration"
            )
            tables = await scalar(
                db,
                "SELECT array_agg(tablename::text ORDER BY tablename) FROM pg_tables "
                "WHERE schemaname = current_schema()",
            )
        finally:
            await pool.close()

        assert_that(first, equal_to([1, 2, 3, 4, 5, 6, 7, 8]))
        assert_that(second, equal_to([]))
        assert_that(recorded, equal_to(first))
        assert_that(
            tables,
            equal_to(
                [
                    "river_job",
                    "river_leader",
                    "river_migration",
                    "river_notification",
                    "river_queue",
                ]
            ),
        )

    async def test_concurrent_opens_migrate_safely(
        self, database_url: str, empty_schema: str
    ) -> None:
        pool = await open_pool(database_url, empty_schema)
        try:
            dbs = await asyncio.gather(*(eddy_pg.DB.open(pool) for _ in range(4)))
            versions = await scalar(dbs[0], "SELECT count(*) FROM river_migration")
        finally:
            await pool.close()

        assert_that(versions, equal_to(8))


class TestTableWrites:
    async def test_insert_fills_defaults_and_ignores_status_input(self, db: eddy_pg.DB) -> None:
        app = await apps.insert(db, App(name="web", image="web:1", phase="ignored"))

        assert_that(app.id, is_(not_none()))
        assert_that(app.created_at, is_(not_none()))
        assert_that(app.phase, equal_to(""))
        assert_that(await job_count(db, "kind = 'eddy.reconcile' AND queue = 'eddy'"), equal_to(1))

    async def test_reconcile_gets_loaded_row_and_saves_status(self, db: eddy_pg.DB) -> None:
        ctl = controller(db)
        seen: list[App] = []
        fail = False

        @ctl.reconcile(apps)
        async def reconcile_app(app: App) -> None:
            seen.append(app.model_copy())
            if fail:
                app.phase = "failed"
                raise eddy_pg.Permanent("image not found")
            app.phase = "ready:" + app.image
            app.image = "must-not-be-saved"  # not a status column

        async def stored() -> App:
            return await apps.get(db, app.id)

        async with running(ctl):
            app = await apps.insert(db, App(name="web", image="web:1"))
            await eventually(lambda: _phase_is(db, app.id, "ready:web:1"))
            got = await stored()
            assert_that(got.image, equal_to("web:1"))
            assert_that(got.reconcile_error, is_(none()))
            assert_that(got.reconciled_at, is_(not_none()))
            assert_that(seen[0].image, equal_to("web:1"))

            # update writes spec and leaves the controller's status alone
            updated = await apps.update(
                db, app.model_copy(update={"image": "web:2", "phase": "api-must-not-write"})
            )
            assert_that(updated.phase, equal_to("ready:web:1"))
            await eventually(lambda: _phase_is(db, app.id, "ready:web:2"))

            # a failing reconcile records its error and the status it set
            fail = True
            await apps.update(db, updated)
            await eventually(lambda: _phase_is(db, app.id, "failed"))
            assert_that((await stored()).reconcile_error, contains_string("image not found"))

            # and the next success clears it
            fail = False
            await db.enqueue(apps.of(app.id))
            await eventually(lambda: _phase_is(db, app.id, "ready:web:2"))
            assert_that((await stored()).reconcile_error, is_(none()))

    async def test_reconcile_returning_a_copy_saves_its_status(self, db: eddy_pg.DB) -> None:
        class Frozen(BaseModel):
            model_config = ConfigDict(frozen=True)
            id: Annotated[UUID | None, eddy_pg.Default] = None
            name: str
            image: str
            phase: Annotated[str, eddy_pg.Status] = ""

        frozen = eddy_pg.Table(Frozen, "apps")
        ctl = controller(db)

        @ctl.reconcile(frozen)
        async def reconcile(row: Frozen) -> Frozen:
            return row.model_copy(update={"phase": "copied"})

        async with running(ctl):
            row = await frozen.insert(db, Frozen(name="web", image="web:1"))
            await eventually(lambda: _phase_is(db, row.id, "copied"))

    async def test_select_get_and_save_status(self, db: eddy_pg.DB) -> None:
        a = await apps.insert(db, App(name="a", image="a:1"))
        await apps.insert(db, App(name="b", image="b:1"))

        named = await apps.select(db, "WHERE name = %s", ("a",))
        everything = await apps.select(db)
        await apps.save_status(db, a.model_copy(update={"phase": "manual"}))

        assert_that([r.id for r in named], equal_to([a.id]))
        assert_that(everything, has_length(2))
        assert_that((await apps.get(db, a.id)).phase, equal_to("manual"))
        assert_that((await apps.get(db, str(a.id))).name, equal_to("a"))  # keys are coerced

    async def test_missing_row_raises_not_found(self, db: eddy_pg.DB) -> None:
        ghost = App(id=uuid4(), name="x", image="x")
        with pytest.raises(eddy_pg.NotFound):
            await apps.get(db, ghost.id)
        with pytest.raises(eddy_pg.NotFound):
            await apps.update(db, ghost)
        with pytest.raises(eddy_pg.NotFound):
            await apps.delete(db, ghost.id)
        assert_that(await job_count(db), equal_to(0))


class TestTeardown:
    async def test_delete_teardown_gets_the_deleted_row(self, db: eddy_pg.DB) -> None:
        ctl = controller(db, resync=0)
        reconciled: list[App] = []
        torn: list[App] = []
        deleted_refs: list[eddy_pg.Ref] = []

        @ctl.reconcile(apps)
        async def reconcile_app(app: App) -> None:
            app.phase = "ready"
            reconciled.append(app)

        @ctl.teardown(apps)
        async def teardown_app(app: App) -> None:
            torn.append(app)
            if len(torn) == 1:
                raise eddy_pg.Retry(0.01)  # teardowns retry like reconciles

        @ctl.middleware
        async def note(ref: eddy_pg.Ref, call_next: eddy_pg.Handler) -> None:
            if ref.deleted:
                deleted_refs.append(ref)
            await call_next(ref)

        async with running(ctl):
            app = await apps.insert(db, App(name="web", image="web:1"))
            await eventually(lambda: _phase_is(db, app.id, "ready"))
            before = len(reconciled)
            await apps.delete(db, app.id)
            await eventually(lambda: len(torn) == 2)

        last = torn[1]
        assert_that(
            (last.id, last.name, last.image, last.phase, last.created_at),
            equal_to((app.id, "web", "web:1", "ready", app.created_at)),
        )
        assert_that(len(reconciled), equal_to(before))
        assert_that(deleted_refs, has_length(2))

    async def test_execute_enqueues_every_touched_row(self, db: eddy_pg.DB) -> None:
        ctl = controller(db, resync=0)
        reconciled: list[str] = []
        torn: list[str] = []

        @ctl.reconcile(apps)
        async def reconcile_app(app: App) -> None:
            reconciled.append(f"{app.name}={app.image}")

        @ctl.teardown(apps)
        async def teardown_app(app: App) -> None:
            torn.append(f"{app.name}={app.image}")

        async with running(ctl):
            inserted = await apps.execute(
                db,
                "INSERT INTO apps (name, image) VALUES ('a', 'a:1'), ('b', 'b:1'), ('c', 'c:1');",
            )
            updated = await apps.execute(
                db, "UPDATE apps SET image = name || %s WHERE name <> %s", (":2", "c")
            )
            await eventually(lambda: {"a=a:2", "b=b:2", "c=c:1"} <= set(reconciled))
            deleted = await apps.execute(db, "DELETE FROM apps WHERE name IN ('a', 'c')")
            untouched = await apps.execute(db, "UPDATE apps SET image = 'x' WHERE false")
            await eventually(lambda: sorted(torn) == ["a=a:2", "c=c:1"])

        assert_that((inserted, updated, deleted, untouched), equal_to((3, 2, 2, 0)))


class TestTransactions:
    async def test_rollback_drops_row_and_job(self, db: eddy_pg.DB) -> None:
        with pytest.raises(RuntimeError, match="boom"):
            async with db.transaction() as tx:
                await apps.insert(tx, App(name="a", image="a:1"))
                raise RuntimeError("boom")

        assert_that(await apps.select(db), has_length(0))
        assert_that(await job_count(db), equal_to(0))

    async def test_writes_join_a_connection_the_caller_holds(self, db: eddy_pg.DB) -> None:
        async with db.pool.connection() as conn, conn.transaction():
            tx = db.adopt(conn)
            a = await apps.insert(tx, App(name="a", image="a:1"))
            await apps.insert(tx, App(name="b", image="b:1"))
            touched = await apps.execute(tx, "UPDATE apps SET image = 'x'")
            await tx.conn.execute("UPDATE apps SET image = 'raw'")  # plain SQL enqueues nothing
            await tx.enqueue(apps.of(a.id))
            assert_that(await job_count(db), equal_to(0))  # nothing visible before commit

        assert_that(touched, equal_to(2))
        assert_that(await job_count(db), equal_to(5))


class TestOutcomes:
    async def test_retry_runs_again_without_using_an_attempt(self, db: eddy_pg.DB) -> None:
        kind = eddy_pg.Kind("app", int)
        ctl = controller(db)
        attempts: list[int] = []

        @ctl.handle(kind)
        async def handle(_: int) -> None:
            attempts.append(eddy_pg.attempt())
            if len(attempts) == 1:
                raise eddy_pg.Retry(0.01)

        async with running(ctl):
            await db.enqueue(kind.of(3))
            await eventually(lambda: _jobs(db, "state = 'completed'", 1))

        assert_that(attempts, equal_to([1, 1]))
        assert_that(await scalar(db, "SELECT metadata->>'snoozes' FROM river_job"), equal_to("1"))

    async def test_permanent_error_cancels_without_retry(self, db: eddy_pg.DB) -> None:
        kind = eddy_pg.Kind("team", int)
        ctl = controller(db)
        calls: list[int] = []

        @ctl.handle(kind)
        async def handle(team: int) -> None:
            calls.append(team)
            raise eddy_pg.Permanent("invalid row")

        async with running(ctl):
            await db.enqueue(kind.of(7))
            await eventually(lambda: _jobs(db, "state = 'cancelled'", 1))

        assert_that(calls, equal_to([7]))
        error = await scalar(db, "SELECT errors[1]->>'error' FROM river_job")
        assert_that(error, equal_to("invalid row"))

    async def test_failure_is_retried_once_its_backoff_passes(self, db: eddy_pg.DB) -> None:
        kind = eddy_pg.Kind("team", int)
        ctl = controller(db)
        calls: list[int] = []

        @ctl.handle(kind)
        async def handle(_: int) -> None:
            calls.append(eddy_pg.attempt())
            if len(calls) == 1:
                raise RuntimeError("quota exceeded")

        async with running(ctl):
            await db.enqueue(kind.of(1))
            await eventually(lambda: _jobs(db, "state = 'retryable'", 1))
            error = await scalar(db, "SELECT errors[1] FROM river_job")
            async with db.pool.connection() as conn:  # skip the backoff
                await conn.execute("UPDATE river_job SET scheduled_at = now()")
            await eventually(lambda: _jobs(db, "state = 'completed'", 1))

        assert_that(calls, equal_to([1, 2]))
        assert_that(error["error"], equal_to("quota exceeded"))
        assert_that(error["attempt"], equal_to(1))
        assert_that(error["trace"], contains_string("RuntimeError"))

    async def test_last_attempt_failure_discards_the_job(self, db: eddy_pg.DB) -> None:
        kind = eddy_pg.Kind("team", int)
        ctl = controller(db)

        @ctl.handle(kind)
        async def handle(_: int) -> None:
            raise RuntimeError("still broken")

        await db.enqueue(kind.of(1))
        async with db.pool.connection() as conn:
            await conn.execute("UPDATE river_job SET max_attempts = 1")
        async with running(ctl):
            await eventually(lambda: _jobs(db, "state = 'discarded'", 1))

    async def test_timeout_fails_the_reconcile_and_records_it(self, db: eddy_pg.DB) -> None:
        ctl = controller(db, reconcile_timeout=0.05)

        @ctl.reconcile(apps)
        async def reconcile_app(app: App) -> None:
            await asyncio.sleep(5)

        async with running(ctl):
            app = await apps.insert(db, App(name="slow", image="s:1"))
            await eventually(lambda: _jobs(db, "state = 'retryable'", 1))

        assert_that((await apps.get(db, app.id)).reconcile_error, is_(not_none()))

    async def test_unknown_kind_is_retried_not_lost(self, db: eddy_pg.DB) -> None:
        known, unknown = eddy_pg.Kind("known", int), eddy_pg.Kind("unknown", int)
        ctl = controller(db)

        @ctl.handle(known)
        async def handle(_: int) -> None: ...

        async with running(ctl):
            await db.enqueue(unknown.of(1))
            await eventually(lambda: _jobs(db, "state = 'retryable'", 1))

    async def test_undecodable_id_is_cancelled(self, db: eddy_pg.DB) -> None:
        ctl = controller(db)

        @ctl.handle(eddy_pg.Kind("team", int))
        async def handle(_: int) -> None: ...

        async with running(ctl):
            await db.enqueue(eddy_pg.Kind("team", str).of("not-a-number"))
            await eventually(lambda: _jobs(db, "state = 'cancelled'", 1))


class TestScheduling:
    async def test_handle_resync_enqueues_listed_ids(self, db: eddy_pg.DB) -> None:
        orgs = eddy_pg.Kind("org")
        ctl = controller(db)
        seen: list[str] = []

        async def list_orgs() -> list[str]:
            return ["acme", "globex"]

        @ctl.handle(orgs, resync=3600, list_ids=list_orgs)
        async def handle(org: str) -> None:
            seen.append(org)

        async with running(ctl):
            await eventually(lambda: sorted(seen) == ["acme", "globex"])

    async def test_table_resync_picks_up_rows_written_outside_eddy(self, db: eddy_pg.DB) -> None:
        async with db.pool.connection() as conn:
            await conn.execute("INSERT INTO apps (name, image) VALUES ('a', 'a:1'), ('b', 'b:1')")
        ctl = controller(db, resync=0.2)
        seen: list[str] = []

        @ctl.reconcile(apps)
        async def reconcile_app(app: App) -> None:
            seen.append(app.name)

        async with running(ctl):
            await eventually(lambda: seen.count("a") >= 2 and seen.count("b") >= 2)

    async def test_max_workers_gives_the_kind_its_own_capped_queue(self, db: eddy_pg.DB) -> None:
        reports = eddy_pg.Table(App, "apps", kind="reports", max_workers=2, priority=3)
        fast = eddy_pg.Kind("fast", int)
        for i in range(6):
            await reports.insert(db, App(name=f"r{i}", image="x"))
        await db.enqueue(fast.of(1))
        assert_that(await job_count(db, "queue = 'eddy-reports' AND priority = 3"), equal_to(6))
        assert_that(await job_count(db, "queue = 'eddy' AND priority = 1"), equal_to(1))

        ctl = controller(db, resync=0)
        in_flight = peak = 0
        done: list[str] = []

        @ctl.reconcile(reports)
        async def reconcile_report(app: App) -> None:
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0.1)
            in_flight -= 1
            done.append(app.name)

        @ctl.handle(fast)
        async def handle_fast(_: int) -> None: ...

        async with running(ctl):
            await eventually(lambda: len(done) == 6)
            await eventually(lambda: _jobs(db, "state = 'completed'", 7))

        assert_that(peak, equal_to(2))

    async def test_same_object_enqueued_twice_never_overlaps(self, db: eddy_pg.DB) -> None:
        kind = eddy_pg.Kind("app", int)
        ctl = controller(db)
        in_flight = peak = done = 0

        @ctl.handle(kind)
        async def handle(_: int) -> None:
            nonlocal in_flight, peak, done
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0.05)
            in_flight -= 1
            done += 1

        await db.enqueue(kind.of(1), kind.of(1), kind.of(1))
        async with running(ctl):
            await eventually(lambda: done == 3)

        assert_that(peak, equal_to(1))


class TestObjectLocks:
    async def test_two_processes_never_reconcile_one_object_at_once(self, db: eddy_pg.DB) -> None:
        kind = eddy_pg.Kind("app", int)
        in_flight = peak = done = 0

        async def handle(_: int) -> None:
            nonlocal in_flight, peak, done
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0.1)
            in_flight -= 1
            done += 1

        # Two controllers that both work the queue, as two processes would.
        replicas = [controller(db, object_locks=True), controller(db, object_locks=True)]
        for ctl in replicas:
            ctl.handle(kind)(handle)

        await db.enqueue(kind.of(9), kind.of(9), kind.of(9), kind.of(9))
        async with running(*replicas):
            await eventually(lambda: done == 4)

        assert_that(peak, equal_to(1))
        # Every object lock was released.
        held = await scalar(
            db,
            "SELECT count(*) FROM pg_locks l JOIN pg_database d ON d.oid = l.database "
            "WHERE l.locktype = 'advisory' AND d.datname = current_database()",
        )
        assert_that(held, equal_to(0))

    async def test_object_lock_is_released_after_a_failed_reconcile(self, db: eddy_pg.DB) -> None:
        kind = eddy_pg.Kind("app", int)
        ctl = controller(db, object_locks=True)
        calls = 0

        @ctl.handle(kind)
        async def handle(_: int) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise eddy_pg.Retry(0.01)

        async with running(ctl):
            await db.enqueue(kind.of(1))
            await eventually(lambda: _jobs(db, "state = 'completed'", 1))

        assert_that(calls, equal_to(2))


class TestMiddlewareAndLeadership:
    async def test_middleware_wraps_in_order(self, db: eddy_pg.DB) -> None:
        kind = eddy_pg.Kind("app", int)
        ctl = controller(db)
        trace: list[str] = []

        def mw(name: str) -> eddy_pg.Middleware:
            async def wrap(ref: eddy_pg.Ref, call_next: eddy_pg.Handler) -> None:
                trace.append(f"{name}>{ref.kind}:{ref.id}")
                await call_next(ref)
                trace.append(f"<{name}")

            return wrap

        ctl.middleware(mw("outer"))
        ctl.middleware(mw("inner"))

        @ctl.handle(kind)
        async def handle(_: int) -> None:
            trace.append("reconcile")

        async with running(ctl):
            await db.enqueue(kind.of(5))
            await eventually(lambda: len(trace) == 5)

        assert_that(
            trace, equal_to(["outer>app:5", "inner>app:5", "reconcile", "<inner", "<outer"])
        )

    async def test_two_replicas_only_one_leads(self, db: eddy_pg.DB) -> None:
        kind = eddy_pg.Kind("app", int)
        changes: list[bool] = []
        replicas = [controller(db, elect=True), controller(db, elect=True)]
        for ctl in replicas:
            ctl.handle(kind)(_noop)
            ctl.on_leadership_change(changes.append)

        async with running(*replicas):
            await eventually(lambda: sum(c.leading for c in replicas) == 1)
            await asyncio.sleep(0.4)
            assert_that(sum(c.leading for c in replicas), equal_to(1))

            # the follower takes over when the leader stops
            leader = next(c for c in replicas if c.leading)
            follower = next(c for c in replicas if not c.leading)
            leader.stop()
            await eventually(lambda: follower.leading)

        assert_that(changes.count(True), greater_than_or_equal_to(2))
        assert_that(any(c.leading for c in replicas), is_(False))

    async def test_cancelling_run_hands_in_flight_jobs_back(self, db: eddy_pg.DB) -> None:
        kind = eddy_pg.Kind("app", int)
        ctl = controller(db, shutdown_timeout=0.05)
        started = asyncio.Event()

        @ctl.handle(kind)
        async def handle(_: int) -> None:
            started.set()
            await asyncio.sleep(30)

        await db.enqueue(kind.of(1))
        task = asyncio.create_task(ctl.run())
        await started.wait()
        ctl.stop()
        await task

        assert_that(await job_count(db, "state = 'scheduled' AND attempt = 0"), equal_to(1))


async def _noop(_: Any) -> None: ...


async def _phase_is(db: eddy_pg.DB, key: UUID | None, phase: str) -> bool:
    return await scalar(db, "SELECT phase FROM apps WHERE id = %s", (key,)) == phase


async def _jobs(db: eddy_pg.DB, where: str, want: int) -> bool:
    return await job_count(db, where) == want
