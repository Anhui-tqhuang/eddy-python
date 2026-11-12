"""The database handle shared by the API and the controller."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from . import _river
from ._kind import DEFAULT_QUEUE, Ref

type Conn = AsyncConnection[Any]
type Pool = AsyncConnectionPool[Any]


class Tx:
    """A transaction that knows how to enqueue reconciles. Table writes made
    through it commit or roll back with it, jobs included. ``conn`` is the
    psycopg connection, for your own SQL in the same transaction."""

    def __init__(self, conn: Conn, db: DB) -> None:
        self.conn = conn
        self._db = db

    async def enqueue(self, *refs: Ref) -> None:
        """Schedule a reconcile of each ref when the transaction commits."""
        await _river.insert(self.conn, self._db.queue, [_river.NewJob.for_ref(r) for r in refs])


class DB:
    """eddy's handle on your database, used by the API and the controller
    alike. Writes made through a Table enqueue the row's reconcile themselves;
    a Controller works those reconciles.

        pool = AsyncConnectionPool(conninfo, open=False)
        await pool.open()
        db = await eddy_pg.DB.open(pool)
    """

    def __init__(self, pool: Pool, *, queue: str = DEFAULT_QUEUE) -> None:
        if not queue:
            raise ValueError("queue must not be empty")
        self.pool = pool
        self.queue = queue

    @classmethod
    async def open(cls, pool: Pool, *, queue: str = DEFAULT_QUEUE) -> DB:
        """Return a DB on ``pool`` after checking that River's tables, which
        hold eddy's jobs, exist. Raises MigrationsMissing when they do not."""
        db = cls(pool, queue=queue)
        async with pool.connection() as conn:
            await _river.check_migrated(conn)
        return db

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[Tx]:
        """Run a block in one transaction: it commits when the block finishes
        and rolls back when it raises.

            async with db.transaction() as tx:
                await teams.insert(tx, team)
                await apps.insert(tx, app)   # both rows and both jobs, or none
        """
        async with self.pool.connection() as conn, conn.transaction():
            yield Tx(conn, self)

    def adopt(self, conn: Conn) -> Tx:
        """Join a transaction on a psycopg connection you already hold."""
        return Tx(conn, self)

    async def enqueue(self, *refs: Ref) -> None:
        """Schedule a reconcile of each ref. Table writes do this for you; it
        is for kinds registered with Controller.handle, or to nudge a row."""
        async with self.transaction() as tx:
            await tx.enqueue(*refs)


type Session = DB | Tx


@asynccontextmanager
async def writing(s: Session) -> AsyncGenerator[tuple[Conn, DB]]:
    """A connection inside a transaction: the Tx's own, or a fresh one."""
    if isinstance(s, Tx):
        yield s.conn, s._db
        return
    async with s.transaction() as tx:
        yield tx.conn, s


@asynccontextmanager
async def reading(s: Session) -> AsyncGenerator[Conn]:
    """A connection for statements that need no transaction of their own."""
    if isinstance(s, Tx):
        yield s.conn
        return
    async with s.pool.connection() as conn:
        yield conn
