"""Tables: a Pydantic model mapped to a database table."""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import BaseModel, TypeAdapter

from . import _river
from ._db import Conn, Session, reading, writing
from ._errors import NotFound
from ._kind import Ref, Settings


@dataclass(frozen=True, slots=True)
class _Marker:
    name: str

    def __repr__(self) -> str:
        return f"eddy_pg.{self.name}"


Default = _Marker("Default")
"""Left out of ``insert`` when the value is None, so the database default applies."""
Status = _Marker("Status")
"""Never written by ``insert`` or ``update``; saved after each reconcile."""
Error = _Marker("Error")
"""A status ``str | None`` that eddy sets to the last reconcile error."""
Reconciled = _Marker("Reconciled")
"""A status ``datetime | None`` that eddy sets on each successful reconcile."""

_RETURNING = re.compile(r"\breturning\b", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class _Column:
    name: str
    default: bool = False
    status: bool = False
    error: bool = False
    reconciled: bool = False


def _param(value: Any) -> Any:
    """Adapt a model field for psycopg."""
    if isinstance(value, BaseModel):
        return Jsonb(value.model_dump(mode="json"))
    if isinstance(value, dict):
        return Jsonb(value)
    if isinstance(value, Enum):
        return value.value
    return value


class Table[M: BaseModel]:
    """Maps Pydantic model ``M`` to a database table. Writes through it
    enqueue the row's reconcile in the same transaction, so there is no
    enqueue call to remember.

        class App(BaseModel):
            id: Annotated[UUID | None, eddy_pg.Default] = None  # filled by the database
            image: str
            phase: Annotated[str, eddy_pg.Status] = ""          # written by the controller

        apps = eddy_pg.Table(App, "apps")

    Columns are the model's fields, by name. Field values must survive
    ``model_dump(mode="json")``, which is how a deleted row reaches teardown.
    """

    def __init__(
        self,
        model: type[M],
        name: str,
        *,
        key: str = "id",
        kind: str | None = None,
        priority: int | None = None,
        max_workers: int | None = None,
    ) -> None:
        if not name:
            raise ValueError("Table needs a table name")
        self.model = model
        self.name = name
        self.kind = kind or name
        self.key = key
        self.settings = Settings(priority=priority, max_workers=max_workers)

        self._columns: list[_Column] = []
        for field_name, info in model.model_fields.items():
            marks = {m.name for m in info.metadata if isinstance(m, _Marker)}
            self._columns.append(
                _Column(
                    field_name,
                    default="Default" in marks,
                    status=bool(marks & {"Status", "Error", "Reconciled"}),
                    error="Error" in marks,
                    reconciled="Reconciled" in marks,
                )
            )
        by_name = {c.name: c for c in self._columns}
        if key not in by_name:
            raise TypeError(f"{model.__name__} has no field for key column {key!r}")
        if by_name[key].status:
            raise TypeError(f"key column {key!r} cannot be a status column")
        annotation = model.model_fields[key].annotation
        self._key_adapter: TypeAdapter[Any] = TypeAdapter(Any if annotation is None else annotation)

        self._table = sql.Identifier(*name.split("."))
        self._key = sql.Identifier(key)
        self._select = sql.SQL(", ").join(sql.Identifier(c.name) for c in self._columns)
        self._returning = sql.SQL(" RETURNING ") + sql.SQL(", ").join(
            sql.Identifier(*name.split("."), c.name) for c in self._columns
        )

    # -- references -------------------------------------------------------

    def of(self, key: Any) -> Ref:
        """A reference to the row with this key, for ``DB.enqueue``."""
        return Ref(self.kind, self._key_json(key), settings=self.settings)

    def _key_json(self, key: Any) -> str:
        return self._key_adapter.dump_json(self._key_adapter.validate_python(key)).decode()

    def decode_key(self, raw: Any) -> Any:
        return self._key_adapter.validate_python(raw)

    def _job(self, row: M, *, gone: bool) -> _river.NewJob:
        return _river.NewJob(
            self.kind,
            self._key_json(getattr(row, self.key)),
            gone=row.model_dump(mode="json") if gone else None,
            settings=self.settings,
        )

    # -- writes -----------------------------------------------------------

    async def insert(self, s: Session, row: M) -> M:
        """Add ``row`` and enqueue its reconcile. Fields marked Default are
        left to the database when None. Returns the row as stored, with
        generated keys and defaults filled in."""
        cols = [
            c
            for c in self._columns
            if not c.status and not (c.default and getattr(row, c.name) is None)
        ]
        if cols:
            stmt = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                self._table,
                sql.SQL(", ").join(sql.Identifier(c.name) for c in cols),
                sql.SQL(", ").join(sql.Placeholder() * len(cols)),
            )
        else:
            stmt = sql.SQL("INSERT INTO {} DEFAULT VALUES").format(self._table)
        params = [_param(getattr(row, c.name)) for c in cols]
        return await self._write(s, stmt + self._returning, params, gone=False)

    async def update(self, s: Session, row: M) -> M:
        """Write ``row``'s non-status columns to the row with the same key and
        enqueue its reconcile. Returns the row as stored. Raises NotFound when
        no row has that key."""
        cols = [c for c in self._columns if not c.status and c.name != self.key]
        if cols:
            sets = sql.SQL(", ").join(
                sql.SQL("{} = {}").format(sql.Identifier(c.name), sql.Placeholder()) for c in cols
            )
        else:  # nothing to write, but the caller still asked for a reconcile
            sets = sql.SQL("{} = {}").format(self._key, self._key)
        stmt = sql.SQL("UPDATE {} SET {} WHERE {} = {}").format(
            self._table, sets, self._key, sql.Placeholder()
        )
        params = [_param(getattr(row, c.name)) for c in cols] + [getattr(row, self.key)]
        return await self._write(s, stmt + self._returning, params, gone=False)

    async def delete(self, s: Session, key: Any) -> None:
        """Remove the row with this key and enqueue its teardown: the row's
        last state travels with the job, and the teardown function receives
        it. Raises NotFound when no row has that key."""
        stmt = sql.SQL("DELETE FROM {} WHERE {} = {}").format(
            self._table, self._key, sql.Placeholder()
        )
        await self._write(s, stmt + self._returning, [self.decode_key(key)], gone=True)

    async def _write(self, s: Session, stmt: sql.Composed, params: list[Any], *, gone: bool) -> M:
        async with writing(s) as (conn, db):
            rows = await self._query(conn, stmt, params)
            if not rows:
                raise NotFound(f"{self.name}: no row matches")
            await _river.insert(conn, db.queue, [self._job(rows[0], gone=gone)])
            return rows[0]

    async def execute(self, s: Session, query: str, params: Sequence[Any] = ()) -> int:
        """Run a free-form INSERT, UPDATE or DELETE on this table and enqueue
        every row it touched, in the same transaction. Returns how many rows
        that was.

            await apps.execute(db, "UPDATE apps SET image = %s WHERE team_id = %s", (image, team))

        eddy learns which rows were touched by adding a RETURNING clause, so
        the query must not have one, must be a single statement, and must not
        alias the table.
        """
        if _RETURNING.search(query):
            raise ValueError(f"{self.name}.execute: the query must not contain RETURNING")
        async with writing(s) as (conn, db):
            stmt = sql.SQL(query.strip().rstrip(";")) + self._returning
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(stmt, params)
                gone = (cur.statusmessage or "").startswith("DELETE")
                rows = [self.model.model_validate(r) for r in await cur.fetchall()]
            await _river.insert(conn, db.queue, [self._job(r, gone=gone) for r in rows])
            return len(rows)

    # -- reads ------------------------------------------------------------

    async def get(self, s: Session, key: Any) -> M:
        """Return the row with this key. Raises NotFound when there is none."""
        stmt = sql.SQL("SELECT {} FROM {} WHERE {} = {}").format(
            self._select, self._table, self._key, sql.Placeholder()
        )
        async with reading(s) as conn:
            rows = await self._query(conn, stmt, [self.decode_key(key)])
        if not rows:
            raise NotFound(f"{self.name}: no row with {self.key} = {key!r}")
        return rows[0]

    async def select(self, s: Session, clause: str = "", params: Sequence[Any] = ()) -> list[M]:
        """Return the rows matching ``clause``, which follows the FROM (for
        example ``"WHERE team_id = %s ORDER BY name"``); with no clause, every
        row."""
        stmt = sql.SQL("SELECT {} FROM {} ").format(self._select, self._table) + sql.SQL(clause)
        async with reading(s) as conn:
            return await self._query(conn, stmt, params)

    async def keys(self, conn: Conn) -> list[str]:
        """Every row's key, as JSON text."""
        cur = await conn.execute(sql.SQL("SELECT {} FROM {}").format(self._key, self._table))
        return [self._key_json(r[0]) for r in await cur.fetchall()]

    async def _query(self, conn: Conn, stmt: sql.Composed, params: Sequence[Any]) -> list[M]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(stmt, params)
            return [self.model.model_validate(r) for r in await cur.fetchall()]

    # -- status -----------------------------------------------------------

    async def save_status(self, s: Session, row: M) -> None:
        """Write ``row``'s status columns, and nothing else, to the row with
        the same key. It enqueues nothing. The controller already does this
        when a reconciler returns; call it only to save progress midway."""
        cols = [c for c in self._columns if c.status]
        if not cols:
            return
        stmt = sql.SQL("UPDATE {} SET {} WHERE {} = {}").format(
            self._table,
            sql.SQL(", ").join(
                sql.SQL("{} = {}").format(sql.Identifier(c.name), sql.Placeholder()) for c in cols
            ),
            self._key,
            sql.Placeholder(),
        )
        params = [_param(getattr(row, c.name)) for c in cols] + [getattr(row, self.key)]
        async with reading(s) as conn:
            await conn.execute(stmt, params)

    def status_of(self, row: M) -> dict[str, Any]:
        """The row's status fields, in a form that can be compared later."""
        return row.model_dump(mode="json", include={c.name for c in self._columns if c.status})

    def stamped(self, row: M, error: str | None, *, reconciled: bool) -> M:
        """A copy of ``row`` with its Error and Reconciled fields set from a
        reconcile's result."""
        update: dict[str, Any] = {}
        for c in self._columns:
            if c.error:
                update[c.name] = error
            elif c.reconciled and reconciled:
                update[c.name] = datetime.now().astimezone()
        return row.model_copy(update=update) if update else row
