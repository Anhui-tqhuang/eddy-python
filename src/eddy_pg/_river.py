"""River's job table, spoken from Python.

River (https://riverqueue.com) stores jobs in ``river_job``, and its Go client
both inserts and works them. Its Python client only inserts, so this module
does the working too, following the Go client's state machine closely enough
that a Go eddy process and a Python one can share a queue:

    available -> running -> completed
                         -> retryable (failed, will retry)   -> available
                         -> scheduled (asked for a Retry)    -> available
                         -> cancelled (Permanent) / discarded (out of attempts)

This is the one place that depends on River's table layout; _migrate.py
creates that layout from River's own migration files.

One object, one waiting job (as in the Go library's merge.go). A write uses the
object's waiting job, bringing it forward when it waits for later (a Retry, or
backoff after an error), and inserts only without one; a resync skips objects
whose job waits and cancels extra waiting jobs; a reconcile that asks for a
Retry while another job for its object waits completes instead. A write while
the object's reconcile runs still inserts (that run may have read the row
before the write), and a deleted row's job is never merged.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from ._kind import Ref, Settings, dumps, fnv64a_signed, queue_for

JOB_KIND = "eddy.reconcile"  # the Go library's job kind
MAX_ATTEMPTS = 25  # River's default
MAX_ATTEMPTED_BY = 100
MAX_ERROR_LEN = 1024


@dataclass(frozen=True, slots=True)
class NewJob:
    """A reconcile to enqueue."""

    kind: str
    id: str  # JSON text
    gone: dict[str, Any] | None = None  # a deleted row's last state
    settings: Settings = field(default_factory=Settings)

    @classmethod
    def for_ref(cls, ref: Ref) -> NewJob:
        return cls(ref.kind, ref.id, settings=ref.settings)

    def args(self) -> str:
        args: dict[str, Any] = {"kind": self.kind, "id": json.loads(self.id)}
        if self.gone is not None:
            args["gone"] = self.gone
        return dumps(args)


@dataclass(frozen=True, slots=True)
class Job:
    """A job fetched for working."""

    id: int
    kind: str
    key: Any  # the object's identifier, decoded from JSON
    gone: dict[str, Any] | None
    attempt: int
    max_attempts: int
    errors: int  # how many failures are recorded so far

    @property
    def ref(self) -> Ref:
        return Ref(self.kind, dumps(self.key), deleted=self.gone is not None)


WAITING_STATES = "('available', 'scheduled', 'retryable', 'pending')"


def _key(job: NewJob) -> str:
    return dumps({"kind": job.kind, "id": json.loads(job.id)})


async def waiting_jobs(
    conn: AsyncConnection[Any], jobs: list[NewJob], except_id: int = 0
) -> dict[int, tuple[int, bool]]:
    """By index into ``jobs``: the id of the waiting job of each job's object,
    other than ``except_id``, and whether it waits for later than now. Jobs of
    deleted rows are never looked up."""
    index = [i for i, j in enumerate(jobs) if j.gone is None]
    if not index:
        return {}
    cur = await conn.execute(
        f"""
        SELECT w.n, j.id, j.later
        FROM unnest(%s::text[]::jsonb[]) WITH ORDINALITY AS w(key, n)
        CROSS JOIN LATERAL (
            SELECT id, scheduled_at > now() AS later FROM river_job
            WHERE kind = '{JOB_KIND}' AND state IN {WAITING_STATES}
              AND args @> w.key AND NOT args ? 'gone' AND id <> %s
            ORDER BY scheduled_at LIMIT 1
        ) j
        """,
        ([_key(jobs[i]) for i in index], except_id),
    )
    return {index[n - 1]: (job_id, later) for n, job_id, later in await cur.fetchall()}


def _without_duplicates(jobs: list[NewJob]) -> list[NewJob]:
    seen: set[str] = set()
    out = []
    for j in jobs:
        if j.gone is None:
            if (key := _key(j)) in seen:
                continue
            seen.add(key)
        out.append(j)
    return out


async def insert(
    conn: AsyncConnection[Any], base_queue: str, jobs: list[NewJob], *, resync: bool = False
) -> None:
    """Enqueue jobs on ``conn``, inside whatever transaction it is in. An object
    with a waiting job gets no second one: a write brings that job forward when
    it waits for later; a resync (``resync=True``) leaves it be."""
    jobs = _without_duplicates(jobs)
    if not jobs:
        return
    waiting = await waiting_jobs(conn, jobs)
    if not resync:
        later = [job_id for job_id, is_later in waiting.values() if is_later]
        if later:
            await conn.execute(
                "UPDATE river_job SET state = 'available', scheduled_at = now() "
                "WHERE id = ANY(%s) AND state IN ('scheduled', 'retryable')",
                (later,),
            )
    jobs = [j for i, j in enumerate(jobs) if i not in waiting]
    if not jobs:
        return
    rows = [
        (j.args(), j.settings.priority or 1, queue_for(base_queue, j.kind, j.settings))
        for j in jobs
    ]
    async with conn.cursor() as cur:
        await cur.executemany(
            "INSERT INTO river_job (args, kind, max_attempts, priority, queue) "
            f"VALUES (%s::jsonb, '{JOB_KIND}', {MAX_ATTEMPTS}, %s, %s)",
            rows,
        )
        # Wake River clients listening for work, as River's own inserts do.
        # The notification is delivered only if the transaction commits.
        for queue in {r[2] for r in rows}:
            await cur.execute(
                "SELECT pg_notify(current_schema() || '.river_insert', %s)",
                (dumps({"queue": queue}),),
            )


async def fetch(conn: AsyncConnection[Any], queue: str, limit: int, client: str) -> list[Job]:
    """Claim up to ``limit`` available jobs from ``queue``."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            WITH locked AS (
                SELECT id FROM river_job
                WHERE state = 'available' AND queue = %(queue)s AND kind = '{JOB_KIND}'
                  AND scheduled_at <= now()
                ORDER BY priority, scheduled_at, id
                LIMIT %(limit)s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE river_job j SET
                state = 'running',
                attempt = j.attempt + 1,
                attempted_at = now(),
                attempted_by = array_append(
                    CASE WHEN array_length(j.attempted_by, 1) >= {MAX_ATTEMPTED_BY}
                         THEN j.attempted_by[
                              array_length(j.attempted_by, 1) + 2 - {MAX_ATTEMPTED_BY}:]
                         ELSE j.attempted_by END,
                    %(client)s::text)
            FROM locked WHERE j.id = locked.id
            RETURNING j.id, j.args, j.attempt, j.max_attempts,
                      coalesce(array_length(j.errors, 1), 0) AS errors
            """,
            {"queue": queue, "limit": limit, "client": client},
        )
        return [
            Job(
                id=r["id"],
                kind=r["args"].get("kind", ""),
                key=r["args"].get("id"),
                gone=r["args"].get("gone"),
                attempt=r["attempt"],
                max_attempts=r["max_attempts"],
                errors=r["errors"],
            )
            for r in await cur.fetchall()
        ]


async def complete(conn: AsyncConnection[Any], job: Job) -> None:
    await conn.execute(
        "UPDATE river_job SET state = 'completed', finalized_at = now() "
        "WHERE id = %s AND state = 'running'",
        (job.id,),
    )


def _attempt_error(job: Job, error: str, trace: str = "") -> Jsonb:
    return Jsonb(
        {
            "at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "attempt": job.attempt,
            "error": error[:MAX_ERROR_LEN],
            "trace": trace,
        }
    )


def retry_delay(errors_so_far: int) -> float:
    """River's default backoff: (failures)^4 seconds, give or take 10%."""
    base = float((errors_so_far + 1) ** 4)
    return base + base * random.uniform(-0.1, 0.1)


async def fail(conn: AsyncConnection[Any], job: Job, error: str, trace: str = "") -> bool:
    """Record a failed attempt. Returns True when no retry will follow."""
    final = job.attempt >= job.max_attempts
    if final:
        await conn.execute(
            "UPDATE river_job SET state = 'discarded', finalized_at = now(), "
            "errors = array_append(errors, %s::jsonb) WHERE id = %s AND state = 'running'",
            (_attempt_error(job, error, trace), job.id),
        )
    else:
        await conn.execute(
            "UPDATE river_job SET state = 'retryable', "
            "scheduled_at = now() + make_interval(secs => %s), "
            "errors = array_append(errors, %s::jsonb) WHERE id = %s AND state = 'running'",
            (retry_delay(job.errors), _attempt_error(job, error, trace), job.id),
        )
    return final


async def cancel(conn: AsyncConnection[Any], job: Job, error: str) -> None:
    await conn.execute(
        "UPDATE river_job SET state = 'cancelled', finalized_at = now(), "
        "errors = array_append(errors, %s::jsonb) WHERE id = %s AND state = 'running'",
        (_attempt_error(job, error), job.id),
    )


async def snooze_or_give_way(conn: AsyncConnection[Any], job: Job, after: float) -> None:
    """A reconcile asked for a Retry: snooze, unless another job for the object
    already waits (a write during this run). That one carries on, and this one
    completes, so no second retry loop forms."""
    others = await waiting_jobs(conn, [NewJob(job.kind, dumps(job.key))], except_id=job.id)
    if others:
        await complete(conn, job)
    else:
        await snooze(conn, job, after)


async def cancel_extra_waiting(conn: AsyncConnection[Any], kind: str) -> None:
    """Cancel all but the earliest waiting job of each object of ``kind``. Two
    jobs in flight at once can each miss the other and both snooze; this takes
    the loops back to one."""
    await conn.execute(
        f"""
        UPDATE river_job SET state = 'cancelled', finalized_at = now()
        WHERE state IN {WAITING_STATES} AND id IN (
            SELECT id FROM (
                SELECT id,
                       row_number() OVER (PARTITION BY args->'id' ORDER BY scheduled_at, id) AS n
                FROM river_job
                WHERE kind = '{JOB_KIND}' AND state IN {WAITING_STATES}
                  AND args->>'kind' = %s AND NOT args ? 'gone'
            ) w WHERE n > 1
        )
        """,
        (kind,),
    )


async def snooze(conn: AsyncConnection[Any], job: Job, after: float) -> None:
    """Run the job again after ``after`` seconds without using up an attempt."""
    await conn.execute(
        """
        UPDATE river_job SET
            state = 'scheduled',
            scheduled_at = now() + make_interval(secs => %s),
            attempt = greatest(attempt - 1, 0),
            metadata = jsonb_set(metadata, '{snoozes}',
                to_jsonb(coalesce((metadata->>'snoozes')::int, 0) + 1))
        WHERE id = %s AND state = 'running'
        """,
        (after, job.id),
    )


async def promote_due(conn: AsyncConnection[Any], queues: list[str]) -> None:
    """Make retryable and scheduled jobs available once their time has come.
    River's Go client does this from its leader; with only Python working the
    queues, it falls to us."""
    await conn.execute(
        "UPDATE river_job SET state = 'available' "
        "WHERE state IN ('retryable', 'scheduled') AND queue = ANY(%s) AND scheduled_at <= now()",
        (queues,),
    )


async def rescue_stuck(conn: AsyncConnection[Any], queues: list[str], after: float) -> None:
    """Retry jobs left running by a worker that died."""
    stuck = Jsonb(
        {
            "at": datetime.now(UTC).isoformat(),
            "attempt": 0,
            "error": "Stuck job rescued by eddy",
            "trace": "",
        }
    )
    await conn.execute(
        """
        UPDATE river_job SET
            state = CASE WHEN attempt >= max_attempts THEN 'discarded'::river_job_state
                         ELSE 'retryable'::river_job_state END,
            finalized_at = CASE WHEN attempt >= max_attempts THEN now() END,
            scheduled_at = now(),
            errors = array_append(errors, %s::jsonb)
        WHERE state = 'running' AND queue = ANY(%s)
          AND attempted_at < now() - make_interval(secs => %s)
        """,
        (stuck, queues, after),
    )


async def delete_finished(conn: AsyncConnection[Any], queues: list[str]) -> None:
    """Drop finished jobs on River's default schedule: completed after a day,
    cancelled and discarded after a week."""
    await conn.execute(
        """
        DELETE FROM river_job WHERE queue = ANY(%s) AND (
            (state = 'completed' AND finalized_at < now() - interval '24 hours')
            OR (state IN ('cancelled', 'discarded') AND finalized_at < now() - interval '7 days'))
        """,
        (queues,),
    )


def object_lock_key(ref: Ref) -> int:
    """The advisory-lock key for one object; the Go library derives the same."""
    return fnv64a_signed(f"eddy/object:{ref}".encode())
