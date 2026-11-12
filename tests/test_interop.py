"""The Go and Python libraries working the same database.

Each test puts the API on one side and the controller on the other, or has
both compete for leadership, through tests/support/gohelper (the Go library).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
from hamcrest import assert_that, equal_to, is_, not_none

import eddy_pg

from .conftest import App, apps, controller, eventually, open_pool, running, scalar

pytestmark = [pytest.mark.integration, pytest.mark.interop]


async def go(gohelper: Path, database_url: str, schema: str, *args: str) -> str:
    """Run one Go helper command and return its output."""
    proc = await asyncio.create_subprocess_exec(
        str(gohelper),
        database_url,
        schema,
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    assert proc.returncode == 0, err.decode()
    return out.decode().strip()


@contextlib.asynccontextmanager
async def go_controller(gohelper: Path, database_url: str, schema: str) -> AsyncGenerator[None]:
    """A Go eddy controller, leading for the length of the block."""
    proc = await asyncio.create_subprocess_exec(
        str(gohelper), database_url, schema, "run", stdout=asyncio.subprocess.PIPE
    )
    assert proc.stdout is not None
    try:
        line = await asyncio.wait_for(proc.stdout.readline(), 15)
        assert line.strip() == b"leading", f"Go controller did not lead: {line!r}"
        yield
    finally:
        with contextlib.suppress(ProcessLookupError):
            proc.terminate()
        await proc.wait()


SCHEMA_SHAPE = """
SELECT 'column', table_name::text, column_name::text,
       data_type || ' ' || is_nullable || ' ' || replace(coalesce(column_default, ''), %(s)s, '')
FROM information_schema.columns WHERE table_schema = %(s)s
UNION ALL
SELECT 'index', tablename::text, indexname::text, replace(indexdef, %(s)s, '')
FROM pg_indexes WHERE schemaname = %(s)s
UNION ALL
SELECT 'constraint', conrelid::regclass::text, conname::text,
       replace(pg_get_constraintdef(c.oid), %(s)s, '')
FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace WHERE n.nspname = %(s)s
UNION ALL
SELECT 'function', '', p.proname::text, replace(pg_get_functiondef(p.oid), %(s)s, '')
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = %(s)s
ORDER BY 1, 2, 3
"""


async def shape(db: eddy_pg.DB, schema: str) -> list[tuple[str, ...]]:
    """Everything in a schema, with the schema's own name removed."""
    async with db.pool.connection() as conn:
        cur = await conn.execute(SCHEMA_SHAPE, {"s": schema})
        return [
            tuple(str(v).replace(f"{schema}.", "") for v in row) for row in await cur.fetchall()
        ]


async def test_python_migration_matches_rivers_own(
    gohelper: Path, database_url: str, empty_schema: str
) -> None:
    # One schema migrated by Go (River itself), one by this package.
    by_go = empty_schema
    by_python = empty_schema + "_py"
    await go(gohelper, database_url, by_go, "migrate")
    go_pool = await open_pool(database_url, by_go)
    try:
        async with go_pool.connection() as conn:
            await conn.execute(f"CREATE SCHEMA {by_python}")  # type: ignore[arg-type]
        py_pool = await open_pool(database_url, by_python)
        try:
            await eddy_pg.migrate(py_pool)
            go_shape = await shape(await eddy_pg.DB.open(go_pool, migrate=False), by_go)
            py_shape = await shape(await eddy_pg.DB.open(py_pool, migrate=False), by_python)
            # Go accepts the Python-migrated schema as current, and the reverse.
            await go(gohelper, database_url, by_python, "check")
            untouched = await eddy_pg.migrate(go_pool)
        finally:
            await py_pool.close()
            async with go_pool.connection() as conn:
                await conn.execute(f"DROP SCHEMA {by_python} CASCADE")  # type: ignore[arg-type]
    finally:
        await go_pool.close()

    assert_that(len(go_shape) > 40, is_(True))
    assert_that(py_shape, equal_to(go_shape))
    assert_that(untouched, equal_to([]))


async def test_python_api_go_controller(
    db: eddy_pg.DB, gohelper: Path, database_url: str, schema: str
) -> None:
    async with go_controller(gohelper, database_url, schema):
        app = await apps.insert(db, App(name="web", image="web:1"))
        await eventually(lambda: _phase(db, "web", "go:web:1"), what="Go to reconcile")
        stored = await apps.get(db, app.id)
        assert_that(stored.reconciled_at, is_(not_none()))

        # The deleted row travels to Go's teardown intact.
        await apps.delete(db, app.id)
        await eventually(lambda: _torn(db, "go"), what="Go to tear down")

    torn = await _torn_row(db, "go")
    assert_that(torn, equal_to(("web", "web:1", "go:web:1", app.created_at)))


async def test_go_api_python_controller(
    db: eddy_pg.DB, gohelper: Path, database_url: str, schema: str
) -> None:
    ctl = controller(db, resync=0)
    torn: list[App] = []

    @ctl.reconcile(apps)
    async def reconcile_app(app: App) -> None:
        app.phase = "py:" + app.image

    @ctl.teardown(apps)
    async def teardown_app(app: App) -> None:
        torn.append(app)

    async with running(ctl):
        app_id = await go(gohelper, database_url, schema, "insert", "web", "web:1")
        await eventually(lambda: _phase(db, "web", "py:web:1"), what="Python to reconcile")
        stored = await apps.get(db, app_id)

        # The row Go deleted reaches Python's teardown intact.
        await go(gohelper, database_url, schema, "delete", app_id)
        await eventually(lambda: len(torn) == 1, what="Python to tear down")

    assert_that(
        (str(torn[0].id), torn[0].name, torn[0].image, torn[0].phase, torn[0].created_at),
        equal_to((app_id, "web", "web:1", "py:web:1", stored.created_at)),
    )


async def test_go_and_python_controllers_share_one_leader_lock(
    db: eddy_pg.DB, gohelper: Path, database_url: str, schema: str
) -> None:
    ctl = controller(db, elect=True, resync=0)

    @ctl.reconcile(apps)
    async def reconcile_app(app: App) -> None:
        app.phase = "py:" + app.image

    async with go_controller(gohelper, database_url, schema), running(ctl):
        await asyncio.sleep(0.5)
        assert_that(ctl.leading, is_(False))  # Go holds the lock

        await apps.insert(db, App(name="a", image="a:1"))
        await eventually(lambda: _phase(db, "a", "go:a:1"), what="Go, the leader, to reconcile")
    # Go has exited and released the lock: Python can lead now.
    async with running(ctl2 := controller(db, elect=True, resync=0)):
        ctl2.reconcile(apps)(reconcile_app)
        await eventually(lambda: ctl2.leading, what="Python to take over")
        await apps.insert(db, App(name="b", image="b:1"))
        await eventually(lambda: _phase(db, "b", "py:b:1"), what="Python to reconcile")


async def _phase(db: eddy_pg.DB, name: str, phase: str) -> bool:
    return await scalar(db, "SELECT phase FROM apps WHERE name = %s", (name,)) == phase


async def _torn(db: eddy_pg.DB, by: str) -> bool:
    return await scalar(db, "SELECT count(*) FROM torn WHERE by = %s", (by,)) == 1


async def _torn_row(db: eddy_pg.DB, by: str) -> tuple[object, ...] | None:
    async with db.pool.connection() as conn:
        cur = await conn.execute(
            "SELECT name, image, phase, created_at FROM torn WHERE by = %s", (by,)
        )
        return await cur.fetchone()
