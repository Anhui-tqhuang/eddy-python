"""eddy: reconcile loops for Python, with Postgres as the only moving part.

A Table maps a Pydantic model to a database table. Writes through it enqueue
the row's reconcile in the same transaction; a Controller, run on any number
of replicas, elects a leader through a Postgres advisory lock and works those
reconciles. Jobs are stored in River's tables in the same format as the Go
eddy library, so the two can share a database.
"""

from ._controller import Controller, Handler, Middleware, UnknownKind, attempt
from ._db import DB, Tx
from ._errors import MigrationsMissing, NotFound, Permanent, Retry
from ._kind import DEFAULT_QUEUE, Kind, Ref
from ._table import Default, Error, Reconciled, Status, Table

__all__ = [
    "DB",
    "DEFAULT_QUEUE",
    "Controller",
    "Default",
    "Error",
    "Handler",
    "Kind",
    "Middleware",
    "MigrationsMissing",
    "NotFound",
    "Permanent",
    "Reconciled",
    "Ref",
    "Retry",
    "Status",
    "Table",
    "Tx",
    "UnknownKind",
    "attempt",
]
