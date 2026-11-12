"""Creating River's tables.

eddy keeps its jobs in River's tables. River's Go tooling normally creates
them; so that a Python project needs no Go, this package bundles River's own
migration files (see ``river_migrations/NOTICE.md``) and applies them the way
River does, recording each version in ``river_migration``. A database migrated
here is indistinguishable from one migrated by River.
"""

from __future__ import annotations

import re
from importlib import resources
from typing import Any

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from ._errors import MigrationsMissing

LINE = "main"  # River's name for its core migration line
MIN_VERSION = 6  # the oldest layout this package can work with
SCHEMA_TEMPLATE = "/* TEMPLATE: schema */"  # River's placeholder for a schema prefix
# The same key the Go library locks while migrating, so the two never race.
MIGRATE_LOCK = 0x656464796D696772  # "eddymigr"

_FILE = re.compile(r"^(\d+)_.*\.up\.sql$")


def bundled() -> dict[int, str]:
    """The bundled migrations, by version, ready to run in the current schema."""
    out: dict[int, str] = {}
    for entry in resources.files("eddy_pg").joinpath("river_migrations").iterdir():
        match = _FILE.match(entry.name)
        if match:
            out[int(match.group(1))] = entry.read_text().replace(SCHEMA_TEMPLATE, "")
    return dict(sorted(out.items()))


async def applied(conn: AsyncConnection[Any]) -> set[int]:
    """The versions of River's main line already applied in this schema."""
    cur = await conn.execute("SELECT to_regclass('river_migration') IS NOT NULL")
    row = await cur.fetchone()
    if not row or not row[0]:
        return set()
    if await _has_line_column(conn):
        cur = await conn.execute("SELECT version FROM river_migration WHERE line = %s", (LINE,))
    else:
        cur = await conn.execute("SELECT version FROM river_migration")
    return {int(r[0]) for r in await cur.fetchall()}


async def _has_line_column(conn: AsyncConnection[Any]) -> bool:
    # River added the "line" column in version 5.
    cur = await conn.execute(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = 'river_migration' "
        "AND column_name = 'line')"
    )
    row = await cur.fetchone()
    return bool(row and row[0])


async def check(conn: AsyncConnection[Any]) -> None:
    """Raise MigrationsMissing when River's tables are absent or too old."""
    version = max(await applied(conn), default=0)
    if version < MIN_VERSION:
        raise MigrationsMissing(
            "River's tables are missing or too old in this schema "
            f"(found migration {version}, need {MIN_VERSION}+). "
            "Call eddy_pg.migrate(pool), or open the DB without migrate=False."
        )


async def migrate(pool: AsyncConnectionPool[Any]) -> list[int]:
    """Create or upgrade River's tables in the pool's current schema, and
    return the versions applied. It is a no-op when they are current, and safe
    to call from several processes at once.

    ``DB.open`` does this for you; call it yourself to run migrations as a
    separate step, then open with ``migrate=False``.
    """
    migrations = bundled()
    async with pool.connection() as conn:
        if set(migrations) <= await applied(conn):
            return []
        await conn.commit()
        await conn.execute("SELECT pg_advisory_lock(%s)", (MIGRATE_LOCK,))
        await conn.commit()
        try:
            done = await applied(conn)  # again, now that we hold the lock
            await conn.commit()
            ran: list[int] = []
            for version, statements in migrations.items():
                if version in done:
                    continue
                async with conn.transaction():
                    await conn.execute(statements)
                    if await _has_line_column(conn):
                        await conn.execute(
                            "INSERT INTO river_migration (line, version) VALUES (%s, %s)",
                            (LINE, version),
                        )
                    else:
                        await conn.execute(
                            "INSERT INTO river_migration (version) VALUES (%s)", (version,)
                        )
                ran.append(version)
            return ran
        finally:
            await conn.rollback()
            await conn.execute("SELECT pg_advisory_unlock(%s)", (MIGRATE_LOCK,))
            await conn.commit()
