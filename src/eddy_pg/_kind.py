"""Kinds, references and per-kind scheduling.

Everything here must match the Go library byte for byte where it reaches the
database: job arguments, queue names and lock keys are shared between the two.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from pydantic import TypeAdapter

DEFAULT_QUEUE = "eddy"
MAX_QUEUE_NAME = 64  # River's limit on queue names

_FNV32_OFFSET, _FNV32_PRIME = 0x811C9DC5, 0x01000193
_FNV64_OFFSET, _FNV64_PRIME = 0xCBF29CE484222325, 0x100000001B3


def fnv32a(data: bytes) -> int:
    h = _FNV32_OFFSET
    for b in data:
        h = ((h ^ b) * _FNV32_PRIME) & 0xFFFFFFFF
    return h


def fnv64a_signed(data: bytes) -> int:
    """FNV-1a 64 as a signed 64-bit integer, the form Postgres advisory locks take."""
    h = _FNV64_OFFSET
    for b in data:
        h = ((h ^ b) * _FNV64_PRIME) & 0xFFFFFFFFFFFFFFFF
    return h - (1 << 64) if h >= (1 << 63) else h


@dataclass(frozen=True, slots=True)
class Settings:
    """Scheduling choices made where a Kind or Table is declared, so the side
    that inserts jobs and the side that works them agree."""

    priority: int | None = None  # River priority 1-4; None is the default (1)
    max_workers: int | None = None  # size of the kind's own queue; None shares the main one

    def __post_init__(self) -> None:
        if self.priority is not None and not 1 <= self.priority <= 4:
            raise ValueError(f"priority must be between 1 and 4, got {self.priority}")
        if self.max_workers is not None and self.max_workers < 1:
            raise ValueError(f"max_workers must be at least 1, got {self.max_workers}")


def queue_for(base: str, kind: str, settings: Settings) -> str:
    """The queue a kind's jobs go to: the base queue, or the kind's own when it
    has max_workers. Mirrors the Go library's naming exactly."""
    if settings.max_workers is None:
        return base
    # River allows lower-case letters and digits, joined by single "_" or "-".
    out: list[str] = []
    sep = True  # no leading separator
    for c in kind.lower():
        if ("a" <= c <= "z") or ("0" <= c <= "9"):
            out.append(c)
            sep = False
        elif not sep:
            out.append("_")
            sep = True
    name = base + "-" + "".join(out).rstrip("_")
    if name.endswith("-") or len(name) > MAX_QUEUE_NAME:
        # Nothing usable in the kind name, or too long: fall back to a hash.
        suffix = f"-{fnv32a(kind.encode()):08x}"
        name = name[: MAX_QUEUE_NAME - len(suffix)].rstrip("-_") + suffix
    return name


def dumps(value: Any) -> str:
    """Compact JSON, as Go's encoding/json writes it."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


@dataclass(frozen=True, slots=True)
class Ref:
    """Points at one object: a kind name and its identifier as JSON text.
    Build one with ``Kind.of`` or ``Table.of``."""

    kind: str
    id: str
    deleted: bool = False
    settings: Settings = field(default_factory=Settings, compare=False, repr=False)

    def __str__(self) -> str:
        return f"{self.kind}/{self.id}"


class Kind[ID]:
    """A type of object that eddy reconciles, with the Python type of its
    identifier. It is the layer under Table, for objects that are not one row
    of one table.

        clusters = eddy_pg.Kind("cluster", UUID)
    """

    def __init__(
        self,
        name: str,
        id_type: type[ID] | Any = str,
        *,
        priority: int | None = None,
        max_workers: int | None = None,
    ) -> None:
        if not name:
            raise ValueError("Kind needs a name")
        self.name = name
        self.settings = Settings(priority=priority, max_workers=max_workers)
        self._adapter: TypeAdapter[ID] = TypeAdapter(id_type)

    def of(self, id: ID) -> Ref:
        """A reference to the object of this kind identified by ``id``."""
        return Ref(self.name, self._adapter.dump_json(id).decode(), settings=self.settings)

    def decode(self, raw: Any) -> ID:
        return self._adapter.validate_python(raw)
