"""Shared fixtures.

Integration tests need Postgres (EDDY_TEST_DATABASE_URL). Interop tests also
need Go, to run the Go eddy library through tests/support/gohelper. Without
what they need, tests are skipped.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import shutil
import subprocess
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID

import psycopg
import pytest
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel

import eddy_pg

HELPER_DIR = Path(__file__).parent / "support" / "gohelper"

APPS_DDL = """
CREATE TABLE apps (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL,
    image text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    phase text NOT NULL DEFAULT '',
    reconcile_error text,
    reconciled_at timestamptz);
CREATE TABLE torn (by text, name text, image text, phase text, created_at timestamptz);
"""


class App(BaseModel):
    """Mirrors the App struct in tests/support/gohelper."""

    id: Annotated[UUID | None, eddy_pg.Default] = None
    name: str
    image: str
    created_at: Annotated[datetime | None, eddy_pg.Default] = None
    phase: Annotated[str, eddy_pg.Status] = ""
    reconcile_error: Annotated[str | None, eddy_pg.Error] = None
    reconciled_at: Annotated[datetime | None, eddy_pg.Reconciled] = None


apps = eddy_pg.Table(App, "apps")


@pytest.fixture(scope="session")
def database_url() -> str:
    url = os.environ.get("EDDY_TEST_DATABASE_URL")
    if not url:
        pytest.skip("EDDY_TEST_DATABASE_URL not set")
    return url


@pytest.fixture(scope="session")
def gohelper(database_url: str) -> Path:
    """The Go helper binary, built once per run."""
    if shutil.which("go") is None:
        pytest.skip("Go is needed to run the Go eddy library")
    binary = HELPER_DIR / "gohelper"
    subprocess.run(["go", "build", "-o", str(binary), "."], cwd=HELPER_DIR, check=True)
    return binary


@pytest.fixture
def empty_schema(database_url: str) -> Generator[str]:
    """A fresh schema with nothing in it."""
    name = "eddy_" + uuid.uuid4().hex[:8]
    with psycopg.connect(database_url, autocommit=True) as conn:
        conn.execute(f"CREATE SCHEMA {name}")
        try:
            yield name
        finally:
            conn.execute(f"DROP SCHEMA {name} CASCADE")


@pytest.fixture
def schema(empty_schema: str, database_url: str) -> str:
    """A fresh schema with the test tables; DB.open adds River's."""
    with psycopg.connect(database_url, autocommit=True) as conn:
        conn.execute(f"SET search_path = {empty_schema}")
        conn.execute(APPS_DDL)
    return empty_schema


async def open_pool(database_url: str, schema: str) -> AsyncConnectionPool[Any]:
    pool: AsyncConnectionPool[Any] = AsyncConnectionPool(
        database_url,
        kwargs={"options": f"-c search_path={schema}"},
        min_size=1,
        max_size=12,
        open=False,
    )
    await pool.open()
    return pool


@pytest.fixture
async def pool(database_url: str, schema: str) -> AsyncGenerator[AsyncConnectionPool[Any]]:
    pool = await open_pool(database_url, schema)
    try:
        yield pool
    finally:
        await pool.close()


@pytest.fixture
async def db(pool: AsyncConnectionPool[Any]) -> eddy_pg.DB:
    return await eddy_pg.DB.open(pool)


def controller(db: eddy_pg.DB, **options: Any) -> eddy_pg.Controller:
    """A controller tuned for tests: no election, fast polling."""
    defaults: dict[str, Any] = {"elect": False, "poll_interval": 0.05, "lock_retry": 0.1}
    return eddy_pg.Controller(db, **(defaults | options))


@contextlib.asynccontextmanager
async def running(*controllers: eddy_pg.Controller) -> AsyncGenerator[None]:
    """Run controllers for the length of the block."""
    tasks = [asyncio.create_task(c.run()) for c in controllers]
    try:
        yield
    finally:
        for c in controllers:
            c.stop()
        await asyncio.gather(*tasks)


async def eventually(
    cond: Callable[[], bool | Awaitable[bool]], within: float = 10.0, what: str = "condition"
) -> None:
    """Wait until cond() is true."""
    deadline = asyncio.get_running_loop().time() + within
    while True:
        result = cond()
        if isinstance(result, Awaitable):
            result = await result
        if result:
            return
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(0.02)


async def scalar(db: eddy_pg.DB, query: str, params: tuple[Any, ...] = ()) -> Any:
    async with db.pool.connection() as conn:
        cur = await conn.execute(query, params)  # type: ignore[arg-type]
        row = await cur.fetchone()
        return row[0] if row else None


async def job_count(db: eddy_pg.DB, where: str = "true") -> int:
    return int(await scalar(db, f"SELECT count(*) FROM river_job WHERE {where}"))
