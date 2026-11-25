"""One waiting reconcile job per object, as in the Go library (merge.go there)."""

from __future__ import annotations

import asyncio
import json

import pytest
from hamcrest import assert_that, equal_to

import eddy_pg

from .conftest import App, apps, controller, eventually, job_count, running

pytestmark = pytest.mark.integration


async def execute(db: eddy_pg.DB, sql: str) -> None:
    async with db.pool.connection() as conn:
        await conn.execute(sql)  # type: ignore[arg-type]


async def waiting(db: eddy_pg.DB, kind: str, key: object) -> int:
    """Reconcile jobs of one object that wait to run: not running, not done."""
    match = json.dumps({"kind": kind, "id": key})
    return await job_count(
        db,
        "kind = 'eddy.reconcile' AND state IN ('available', 'scheduled', 'retryable', 'pending') "
        f"AND args @> '{match}'::jsonb AND NOT args ? 'gone'",
    )


class TestMerge:
    async def test_resync_object_with_a_waiting_job_gets_no_second(self, db: eddy_pg.DB) -> None:
        """An object that keeps asking for a Retry gains no loop per resync."""
        kind = eddy_pg.Kind("app", int)
        ctl = controller(db)
        calls = lists = 0

        async def list_apps() -> list[int]:
            nonlocal lists
            lists += 1
            return [3]

        @ctl.handle(kind, resync=0.2, list_ids=list_apps)
        async def handle(_: int) -> None:
            nonlocal calls
            calls += 1
            raise eddy_pg.Retry(3600)  # never converges

        async with running(ctl):
            await eventually(lambda: calls >= 1 and lists >= 4, what="four resyncs")

            async def one_loop() -> bool:
                return (
                    await waiting(db, "app", 3) == 1
                    and await job_count(
                        db, "kind = 'eddy.reconcile' AND state IN ('running', 'scheduled')"
                    )
                    == 1
                )

            await eventually(one_loop, what="one waiting job, one retry loop")

    async def test_write_while_a_job_waits_for_later_brings_it_forward(
        self, db: eddy_pg.DB
    ) -> None:
        kind = eddy_pg.Kind("app", int)
        await db.enqueue(kind.of(3))
        await execute(
            db, "UPDATE river_job SET state = 'scheduled', scheduled_at = now() + interval '1 hour'"
        )
        await db.enqueue(kind.of(3))
        assert_that(await waiting(db, "app", 3), equal_to(1))
        assert_that(
            await job_count(db, "state = 'available' AND scheduled_at <= now()"), equal_to(1)
        )
        await db.enqueue(kind.of(4))  # another object is not merged with it
        assert_that(await waiting(db, "app", 4), equal_to(1))

    async def test_write_while_running_adds_a_job(self, db: eddy_pg.DB) -> None:
        """The running reconcile may have read the row before the write."""
        kind = eddy_pg.Kind("app", int)
        ctl = controller(db, resync=0)
        started, release = asyncio.Event(), asyncio.Event()
        calls = 0

        @ctl.handle(kind)
        async def handle(_: int) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                started.set()
                await release.wait()

        async with running(ctl):
            await db.enqueue(kind.of(3))
            await started.wait()
            await db.enqueue(kind.of(3))
            assert_that(await waiting(db, "app", 3), equal_to(1))
            release.set()
            await eventually(lambda: calls == 2)

    async def test_retry_with_another_job_waiting_does_not_snooze(self, db: eddy_pg.DB) -> None:
        kind = eddy_pg.Kind("app", int)
        ctl = controller(db, resync=0)
        started, release = asyncio.Event(), asyncio.Event()
        calls = 0

        @ctl.handle(kind)
        async def handle(_: int) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                started.set()
                await release.wait()
                raise eddy_pg.Retry(3600)

        async with running(ctl):
            await db.enqueue(kind.of(3))
            await started.wait()
            await db.enqueue(kind.of(3))  # a write during the run
            release.set()
            await eventually(lambda: calls == 2)

            async def none_waiting() -> bool:
                return await waiting(db, "app", 3) == 0

            await eventually(none_waiting, what="no job left waiting")

    async def test_delete_is_never_merged(self, db: eddy_pg.DB) -> None:
        app = await apps.insert(db, App(name="web", image="web:1"))
        await execute(
            db, "UPDATE river_job SET state = 'scheduled', scheduled_at = now() + interval '1 hour'"
        )
        assert app.id is not None
        await apps.delete(db, app.id)
        assert_that(await job_count(db, "args ? 'gone'"), equal_to(1))
