"""The controller: leader election and the loop that works reconciles."""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import functools
import logging
import os
import socket
import traceback
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ValidationError

from . import _river
from ._db import DB, Conn
from ._errors import NotFound, Permanent, Retry
from ._kind import Kind, Ref, Settings, fnv64a_signed, queue_for
from ._river import MAX_ERROR_LEN, Job, NewJob
from ._table import Table

log = logging.getLogger("eddy_pg")

type Handler = Callable[[Ref], Awaitable[None]]
type Middleware = Callable[[Ref, Handler], Awaitable[None]]

_INSERT_BATCH = 1000
_attempt: contextvars.ContextVar[int] = contextvars.ContextVar("eddy_attempt", default=0)


def attempt() -> int:
    """The attempt number (1 for the first try) of the reconcile running in
    this task, or 0 outside one."""
    return _attempt.get()


class UnknownKind(Exception):
    """A job's kind has no reconciler here. It is retried, so jobs enqueued
    ahead of a rollout are not lost."""


@dataclass(slots=True)
class _TableReg[M: BaseModel]:
    table: Table[M]
    reconcile: Callable[[M], Awaitable[M | None]] | None = None
    teardown: Callable[[M], Awaitable[None]] | None = None


@dataclass(slots=True)
class _KindReg:
    kind: Kind[Any]
    handle: Callable[[Any], Awaitable[None]]
    every: float | None = None
    list_ids: Callable[[], Awaitable[list[Any]]] | None = None


class Controller:
    """Works the reconciles enqueued through a DB. Start one per replica:
    replicas elect a leader through a Postgres advisory lock, and only the
    leader reconciles.

        ctl = eddy_pg.Controller(db)

        @ctl.reconcile(apps)
        async def reconcile_app(app: App) -> None:
            await deploy(app)
            app.phase = "ready"          # status fields are saved for you

        @ctl.teardown(apps)
        async def teardown_app(app: App) -> None:
            await undeploy(app)          # app is the row as it was when deleted

        await ctl.run()

    A Go eddy controller on the same queue competes for the same lock, so the
    two can be mixed; whichever leads must know every kind in use.
    """

    def __init__(
        self,
        db: DB,
        *,
        workers: int = 10,
        resync: float = 600.0,
        reconcile_timeout: float = 60.0,
        shutdown_timeout: float = 30.0,
        elect: bool = True,
        poll_interval: float = 1.0,
        lock_retry: float = 2.0,
        rescue_after: float = 3600.0,
    ) -> None:
        """
        workers: reconciles running at once on the shared queue; kinds with
            max_workers run on top of that, each in its own queue.
        resync: seconds between re-checks of every row of each reconciled
            table; 0 turns it off.
        reconcile_timeout: bound on one reconcile or teardown call.
        shutdown_timeout: how long ``run`` waits for in-flight reconciles when
            it stops or loses leadership, before cancelling them.
        elect: False skips leader election, for exactly one replica or tests.
        poll_interval: how often the queues are checked for work.
        lock_retry: how often a follower retries the lock and the leader
            checks its connection.
        rescue_after: jobs left running this long by a dead worker are retried.
        """
        if workers < 1:
            raise ValueError("workers must be at least 1")
        if resync < 0:
            raise ValueError("resync must not be negative")
        self._db = db
        self._workers = workers
        self._resync = resync
        self._timeout = reconcile_timeout
        self._shutdown_timeout = shutdown_timeout
        self._elect = elect
        self._poll = poll_interval
        self._lock_retry = lock_retry
        self._rescue_after = rescue_after

        self._tables: dict[str, _TableReg[Any]] = {}
        self._kinds: dict[str, _KindReg] = {}
        self._middleware: list[Middleware] = []
        self._on_leadership: list[Callable[[bool], None]] = []

        self._leading = False
        self._stop = asyncio.Event()
        self._wake = asyncio.Event()
        self._running: dict[str, set[asyncio.Task[None]]] = {}
        self._locks: dict[str, tuple[asyncio.Lock, int]] = {}
        self._client = f"eddy-py-{socket.gethostname()}-{os.getpid()}"

    # -- registration -----------------------------------------------------

    def _table(self, table: Table[Any]) -> _TableReg[Any]:
        if table.kind in self._kinds:
            raise ValueError(f"kind {table.kind!r} is already registered with handle")
        return self._tables.setdefault(table.kind, _TableReg(table))

    def reconcile[M: BaseModel, F: Callable[..., Awaitable[Any]]](
        self, table: Table[M]
    ) -> Callable[[F], F]:
        """Register the reconciler for a table's rows:
        ``async def f(row: M) -> M | None``.

        eddy loads the row before each call, so the function always sees
        current state; a row that no longer exists is skipped (see teardown).
        Status fields the function assigns, or sets on a returned copy, are
        saved when it finishes, on success and on failure alike.

        The function must be idempotent. Raising retries with backoff; see
        Retry and Permanent.
        """

        def register(f: F) -> F:
            reg = self._table(table)
            if reg.reconcile is not None:
                raise ValueError(f"reconcile({table.kind}) registered twice")
            reg.reconcile = f
            return f

        return register

    def teardown[M: BaseModel, F: Callable[..., Awaitable[Any]]](
        self, table: Table[M]
    ) -> Callable[[F], F]:
        """Register the function that runs when a row is deleted through the
        Table: ``async def f(row: M) -> None``. ``row`` is the row as it was
        when deleted. It is retried like a reconcile until it succeeds."""

        def register(f: F) -> F:
            reg = self._table(table)
            if reg.teardown is not None:
                raise ValueError(f"teardown({table.kind}) registered twice")
            reg.teardown = f
            return f

        return register

    def handle[ID, F: Callable[..., Awaitable[Any]]](
        self,
        kind: Kind[ID],
        *,
        resync: float | None = None,
        list_ids: Callable[[], Awaitable[list[ID]]] | None = None,
    ) -> Callable[[F], F]:
        """Register the reconciler for a bare kind: ``async def f(id) -> None``.
        It loads what it needs and treats a missing object as "tear everything
        down". With ``resync`` and ``list_ids``, every listed object is
        enqueued again on that interval."""
        if (resync is None) != (list_ids is None):
            raise ValueError("handle: resync and list_ids go together")
        if resync is not None and resync <= 0:
            raise ValueError("handle: resync must be positive")

        def register(f: F) -> F:
            if kind.name in self._kinds or kind.name in self._tables:
                raise ValueError(f"kind {kind.name!r} registered twice")
            self._kinds[kind.name] = _KindReg(kind, f, resync, list_ids)
            return f

        return register

    def middleware(self, mw: Middleware) -> Middleware:
        """Wrap every reconcile and teardown, for metrics, tracing or logging:
        ``async def mw(ref, call_next): ...; await call_next(ref)``. The first
        one registered is the outermost. Usable as a decorator."""
        self._middleware.append(mw)
        return mw

    def on_leadership_change(self, f: Callable[[bool], None]) -> Callable[[bool], None]:
        """Call ``f(True)`` when this replica starts leading and ``f(False)``
        when it stops. Usable as a decorator."""
        self._on_leadership.append(f)
        return f

    # -- lifecycle --------------------------------------------------------

    @property
    def leading(self) -> bool:
        """Whether this replica currently leads; for a readiness probe."""
        return self._leading

    def stop(self) -> None:
        """Ask ``run`` to drain and return."""
        self._stop.set()
        self._wake.set()

    async def run(self) -> None:
        """Campaign for leadership and, while leading, work the queues. When
        leadership is lost, drain and campaign again. Returns after ``stop``
        once in-flight reconciles have finished or timed out; cancelling the
        task does the same."""
        if not self._tables and not self._kinds:
            raise RuntimeError("nothing to reconcile: register with reconcile() or handle()")
        while not self._stop.is_set():
            lock = await self._campaign()
            if lock is None and self._elect:
                return  # stopped while campaigning
            try:
                await self._lead(lock)
            finally:
                if lock is not None:
                    await self._release(lock)
            if not self._stop.is_set():
                log.warning("eddy: lost leadership, campaigning again")

    def _lock_key(self) -> int:
        # The same key the Go library derives, so both compete for one lock.
        return fnv64a_signed(f"eddy:{self._db.queue}".encode())

    async def _campaign(self) -> Conn | None:
        """Block until this replica holds the leader lock; None when stopping
        or when election is off."""
        if not self._elect:
            return None
        while not self._stop.is_set():
            with contextlib.suppress(Exception):
                conn: Conn = await self._db.pool.getconn()
                try:
                    await conn.set_autocommit(True)
                    cur = await conn.execute("SELECT pg_try_advisory_lock(%s)", (self._lock_key(),))
                    row = await cur.fetchone()
                    if row and row[0]:
                        return conn
                except BaseException:
                    await self._db.pool.putconn(conn)
                    raise
                await conn.set_autocommit(False)
                await self._db.pool.putconn(conn)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), self._lock_retry)
        return None

    async def _release(self, conn: Conn) -> None:
        try:
            await conn.execute("SELECT pg_advisory_unlock(%s)", (self._lock_key(),))
            await conn.set_autocommit(False)
        except Exception:
            # The session may still hold the lock: close it rather than reuse it.
            with contextlib.suppress(Exception):
                await conn.close()
        await self._db.pool.putconn(conn)

    async def _lead(self, lock: Conn | None) -> None:
        """Work the queues until stopped or the lock connection fails, then drain."""
        lost = asyncio.Event()
        loops = [
            asyncio.create_task(self._fetch_loop()),
            asyncio.create_task(self._maintain_loop()),
            asyncio.create_task(self._resync_loop()),
        ]
        if lock is not None:
            loops.append(asyncio.create_task(self._watch_lock(lock, lost)))
        self._set_leading(True)
        log.info("eddy: leading on queue %s", self._db.queue)
        waiters = [asyncio.create_task(self._stop.wait()), asyncio.create_task(lost.wait())]
        try:
            await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        finally:
            self._set_leading(False)
            for t in [*waiters, *loops]:
                t.cancel()
            await asyncio.gather(*waiters, *loops, return_exceptions=True)
            await asyncio.shield(self._drain())

    async def _watch_lock(self, lock: Conn, lost: asyncio.Event) -> None:
        while True:
            await asyncio.sleep(self._lock_retry)
            try:
                await lock.execute("SELECT 1")
            except Exception:
                lost.set()
                return

    def _set_leading(self, leading: bool) -> None:
        self._leading = leading
        for f in self._on_leadership:
            f(leading)

    async def _drain(self) -> None:
        tasks = [t for ts in self._running.values() for t in ts]
        if not tasks:
            return
        _, pending = await asyncio.wait(tasks, timeout=self._shutdown_timeout)
        if pending:
            log.warning("eddy: drain timed out, cancelling %d reconciles", len(pending))
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    # -- loops ------------------------------------------------------------

    def _settings(self) -> dict[str, Settings]:
        out = {name: reg.table.settings for name, reg in self._tables.items()}
        out.update({name: reg.kind.settings for name, reg in self._kinds.items()})
        return out

    def _queues(self) -> dict[str, int]:
        """The shared queue plus one per kind declared with max_workers."""
        queues = {self._db.queue: self._workers}
        for kind, settings in self._settings().items():
            if settings.max_workers is not None:
                queues[queue_for(self._db.queue, kind, settings)] = settings.max_workers
        return queues

    async def _fetch_loop(self) -> None:
        queues = self._queues()
        while True:
            self._wake.clear()
            full = False
            try:
                for queue, limit in queues.items():
                    running = self._running.setdefault(queue, set())
                    free = limit - len(running)
                    if free <= 0:
                        continue
                    async with self._db.pool.connection() as conn:
                        jobs = await _river.fetch(conn, queue, free, self._client)
                    for job in jobs:
                        task = asyncio.create_task(self._work(job))
                        running.add(task)
                        task.add_done_callback(functools.partial(self._finished, running))
                    full = full or len(jobs) == free
            except Exception:
                log.exception("eddy: fetching jobs failed")
            if not full:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._wake.wait(), self._poll)

    def _finished(self, running: set[asyncio.Task[None]], task: asyncio.Task[None]) -> None:
        running.discard(task)
        self._wake.set()  # a slot is free: look for more work now

    async def _maintain_loop(self) -> None:
        """What River's Go leader does for its queues, done here for ours."""
        queues = list(self._queues())
        loop = asyncio.get_running_loop()
        next_rescue = next_delete = loop.time()
        while True:
            try:
                async with self._db.pool.connection() as conn:
                    await _river.promote_due(conn, queues)
                    if loop.time() >= next_rescue:
                        await _river.rescue_stuck(conn, queues, self._rescue_after)
                        next_rescue = loop.time() + 60
                    if loop.time() >= next_delete:
                        await _river.delete_finished(conn, queues)
                        next_delete = loop.time() + 3600
            except Exception:
                log.exception("eddy: queue maintenance failed")
            await asyncio.sleep(self._poll)

    async def _resync_loop(self) -> None:
        """Enqueue every object again: at once, then on each kind's interval."""
        loop = asyncio.get_running_loop()
        due: dict[str, float] = {}
        for name, treg in self._tables.items():
            if treg.reconcile is not None and self._resync > 0:
                due[name] = loop.time()
        for name, kreg in self._kinds.items():
            if kreg.every is not None:
                due[name] = loop.time()
        while due:
            name = min(due, key=lambda n: due[n])
            await asyncio.sleep(max(due[name] - loop.time(), 0))
            try:
                await self._resync_one(name)
            except Exception:
                log.exception("eddy: resync of %s failed", name)
            every = self._resync if name in self._tables else self._kinds[name].every
            due[name] = loop.time() + (every or self._resync)

    async def _resync_one(self, name: str) -> None:
        async with self._db.pool.connection() as conn:
            if name in self._tables:
                table = self._tables[name].table
                jobs = [NewJob(name, k, settings=table.settings) for k in await table.keys(conn)]
            else:
                kreg = self._kinds[name]
                assert kreg.list_ids is not None
                jobs = [NewJob.for_ref(kreg.kind.of(i)) for i in await kreg.list_ids()]
            for start in range(0, len(jobs), _INSERT_BATCH):
                await _river.insert(conn, self._db.queue, jobs[start : start + _INSERT_BATCH])
        log.debug("eddy: resync enqueued %d %s", len(jobs), name)

    # -- one job ----------------------------------------------------------

    @contextlib.asynccontextmanager
    async def _object_lock(self, key: str) -> AsyncGenerator[None]:
        """Serialise reconciles of one object, even when it was enqueued twice."""
        lock, users = self._locks.get(key, (asyncio.Lock(), 0))
        self._locks[key] = (lock, users + 1)
        try:
            async with lock:
                yield
        finally:
            lock, users = self._locks[key]
            if users <= 1:
                del self._locks[key]
            else:
                self._locks[key] = (lock, users - 1)

    async def _work(self, job: Job) -> None:
        ref = job.ref
        outcome: Callable[[Conn], Awaitable[Any]]
        try:
            async with self._object_lock(str(ref)):
                token = _attempt.set(job.attempt)
                try:
                    await self._chain(ref, job)
                finally:
                    _attempt.reset(token)
            outcome = lambda c: _river.complete(c, job)  # noqa: E731
        except Retry as r:
            after = r.after
            outcome = lambda c: _river.snooze(c, job, after)  # noqa: E731
        except Permanent as p:
            log.warning("eddy: reconcile of %s failed for good: %s", ref, p)
            message = str(p)
            outcome = lambda c: _river.cancel(c, job, message)  # noqa: E731
        except asyncio.CancelledError:
            # Shut down mid-reconcile: hand the job back without using an attempt.
            with contextlib.suppress(Exception):
                await asyncio.shield(self._settle(lambda c: _river.snooze(c, job, 0)))
            raise
        except Exception as e:
            log.warning("eddy: reconcile of %s failed (attempt %d): %s", ref, job.attempt, e)
            message, trace = str(e) or type(e).__name__, traceback.format_exc()
            outcome = lambda c: _river.fail(c, job, message, trace)  # noqa: E731
        try:
            await self._settle(outcome)
        except Exception:
            log.exception("eddy: recording the outcome of %s failed", ref)

    async def _settle(self, outcome: Callable[[Conn], Awaitable[Any]]) -> None:
        async with self._db.pool.connection() as conn:
            await outcome(conn)

    async def _chain(self, ref: Ref, job: Job) -> None:
        async def innermost(_: Ref) -> None:
            await self._dispatch(job)

        call: Handler = innermost
        for mw in reversed(self._middleware):
            call = _bind(mw, call)
        await call(ref)

    async def _call[T](self, f: Callable[[Any], Awaitable[T]], arg: Any) -> T:
        async with asyncio.timeout(self._timeout):
            return await f(arg)

    async def _dispatch(self, job: Job) -> None:
        if job.kind in self._tables:
            await self._run_table(self._tables[job.kind], job)
            return
        kreg = self._kinds.get(job.kind)
        if kreg is None:
            raise UnknownKind(f"no reconciler registered for kind {job.kind!r}")
        try:
            ident = kreg.kind.decode(job.key)
        except ValidationError as e:
            raise Permanent(f"decode {job.kind} id {job.key!r}: {e}") from e
        await self._call(kreg.handle, ident)

    async def _run_table(self, reg: _TableReg[Any], job: Job) -> None:
        table = reg.table
        if job.gone is not None and reg.teardown is not None:
            try:
                gone = table.model.model_validate(job.gone)
            except ValidationError as e:
                raise Permanent(f"decode deleted {job.kind} row: {e}") from e
            await self._call(reg.teardown, gone)
        if reg.reconcile is None:
            return
        # Reconcile whatever row has the key now: usually none after a delete,
        # but the key may have been reused since.
        try:
            row = await table.get(self._db, table.decode_key(job.key))
        except ValidationError as e:
            raise Permanent(f"decode {job.kind} key {job.key!r}: {e}") from e
        except NotFound:
            return
        before = table.status_of(row)
        failure: Exception | None = None
        try:
            returned = await self._call(reg.reconcile, row)
            if returned is not None:
                row = returned
        except Exception as e:
            failure = e
        error = None
        if failure is not None and not isinstance(failure, Retry):
            error = (str(failure) or type(failure).__name__)[:MAX_ERROR_LEN]
        row = table.stamped(row, error, reconciled=failure is None)
        if table.status_of(row) != before:
            try:
                await table.save_status(self._db, row)
            except Exception as save_error:
                if failure is None or isinstance(failure, Retry):
                    raise
                raise failure from save_error
        if failure is not None:
            raise failure


def _bind(mw: Middleware, call_next: Handler) -> Handler:
    async def call(ref: Ref) -> None:
        await mw(ref, call_next)

    return call
