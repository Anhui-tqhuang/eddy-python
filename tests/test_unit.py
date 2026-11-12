"""Tests that need no database."""

from __future__ import annotations

from datetime import timedelta
from typing import Annotated
from uuid import UUID

import pytest
from hamcrest import (
    assert_that,
    calling,
    close_to,
    equal_to,
    has_length,
    is_,
    less_than_or_equal_to,
    matches_regexp,
    raises,
)
from pydantic import BaseModel

import eddy_pg
from eddy_pg import _kind, _river
from eddy_pg._kind import Settings, fnv64a_signed, queue_for

pytestmark = pytest.mark.unit

RIVER_QUEUE_NAME = r"^(?:[a-z0-9])+(?:[_|\-]?[a-z0-9]+)*$"


# The expected names are the ones the Go library produces for the same input.
@pytest.mark.parametrize(
    ("kind", "settings", "expected"),
    [
        ("apps", Settings(), "eddy"),
        ("apps", Settings(priority=3), "eddy"),
        ("apps", Settings(max_workers=2), "eddy-apps"),
        ("Billing.Invoices", Settings(max_workers=1), "eddy-billing_invoices"),
        ("__a--b..c__", Settings(max_workers=1), "eddy-a_b_c"),
        ("***", Settings(max_workers=1), "eddy-e22c13cd"),
    ],
)
def test_queue_for_various_kinds_matches_go_naming(
    kind: str, settings: Settings, expected: str
) -> None:
    name = queue_for("eddy", kind, settings)
    assert_that(name, equal_to(expected))
    assert_that(name, matches_regexp(RIVER_QUEUE_NAME))


def test_queue_for_long_kind_stays_within_limit_and_distinct() -> None:
    long = queue_for("eddy", "a_very_long_kind_name" * 5, Settings(max_workers=1))
    other = queue_for("eddy", "a_very_long_kind_name" * 5 + "x", Settings(max_workers=1))
    assert_that(len(long), less_than_or_equal_to(_kind.MAX_QUEUE_NAME))
    assert_that(long, matches_regexp(RIVER_QUEUE_NAME))
    assert_that(long == other, is_(False))


def test_fnv64a_signed_known_vectors_match_reference() -> None:
    assert_that(fnv64a_signed(b""), equal_to(0xCBF29CE484222325 - (1 << 64)))
    assert_that(fnv64a_signed(b"a"), equal_to(0xAF63DC4C8601EC8C - (1 << 64)))


@pytest.mark.parametrize("kwargs", [{"priority": 0}, {"priority": 5}, {"max_workers": 0}])
def test_settings_out_of_range_raises_value_error(kwargs: dict[str, int]) -> None:
    assert_that(calling(Settings).with_args(**kwargs), raises(ValueError))


def test_kind_of_encodes_id_as_json() -> None:
    uid = UUID("6f1c1e2a-0000-4000-8000-000000000001")
    assert_that(str(eddy_pg.Kind("team", int).of(42)), equal_to("team/42"))
    assert_that(str(eddy_pg.Kind("org").of("acme")), equal_to('org/"acme"'))
    assert_that(str(eddy_pg.Kind("app", UUID).of(uid)), equal_to(f'app/"{uid}"'))
    assert_that(calling(eddy_pg.Kind).with_args(""), raises(ValueError))


def test_retry_accepts_seconds_and_timedelta() -> None:
    assert_that(eddy_pg.Retry(15).after, equal_to(15.0))
    assert_that(eddy_pg.Retry(timedelta(minutes=1)).after, equal_to(60.0))
    assert_that(eddy_pg.Retry(-5).after, equal_to(0.0))


def test_new_job_args_match_go_job_format() -> None:
    plain = _river.NewJob("apps", "42")
    gone = _river.NewJob("apps", '"x"', gone={"id": "x", "name": "web"})
    assert_that(plain.args(), equal_to('{"kind":"apps","id":42}'))
    assert_that(gone.args(), equal_to('{"kind":"apps","id":"x","gone":{"id":"x","name":"web"}}'))


@pytest.mark.parametrize(("errors", "base"), [(0, 1.0), (1, 16.0), (2, 81.0)])
def test_retry_delay_follows_rivers_backoff(errors: int, base: float) -> None:
    assert_that(_river.retry_delay(errors), close_to(base, base * 0.1))


class _Row(BaseModel):
    id: int
    phase: Annotated[str, eddy_pg.Status] = ""


def test_table_bad_mapping_raises() -> None:
    class StatusKey(BaseModel):
        id: Annotated[int, eddy_pg.Status] = 0

    assert_that(calling(eddy_pg.Table).with_args(_Row, ""), raises(ValueError))
    assert_that(calling(eddy_pg.Table).with_args(_Row, "t", key="uid"), raises(TypeError))
    assert_that(calling(eddy_pg.Table).with_args(StatusKey, "t"), raises(TypeError))
    assert_that(calling(eddy_pg.Table).with_args(_Row, "t", priority=9), raises(ValueError))


def test_table_of_names_kind_and_key() -> None:
    table = eddy_pg.Table(_Row, "billing.rows", kind="row", max_workers=2)
    ref = table.of(7)
    assert_that(str(ref), equal_to("row/7"))
    assert_that(ref.deleted, is_(False))
    assert_that(queue_for("eddy", table.kind, table.settings), equal_to("eddy-row"))


async def test_table_execute_with_returning_raises_value_error() -> None:
    table = eddy_pg.Table(_Row, "rows")
    with pytest.raises(ValueError, match="RETURNING"):
        await table.execute(None, "UPDATE rows SET phase = '' RETURNING id")  # type: ignore[arg-type]


def test_controller_registration_misuse_raises() -> None:
    db = eddy_pg.DB(None)  # type: ignore[arg-type]
    table = eddy_pg.Table(_Row, "rows")
    kind = eddy_pg.Kind("rows", int)

    async def f(_: object) -> None: ...

    ctl = eddy_pg.Controller(db)
    ctl.reconcile(table)(f)
    ctl.teardown(table)(f)
    assert_that(calling(ctl.reconcile(table)).with_args(f), raises(ValueError))
    assert_that(calling(ctl.teardown(table)).with_args(f), raises(ValueError))
    assert_that(calling(ctl.handle(kind)).with_args(f), raises(ValueError))

    other = eddy_pg.Controller(db)
    other.handle(kind)(f)
    assert_that(calling(other.handle(kind)).with_args(f), raises(ValueError))
    assert_that(calling(other.reconcile(table)).with_args(f), raises(ValueError))
    assert_that(calling(other.handle).with_args(kind, resync=5), raises(ValueError))

    assert_that(calling(eddy_pg.Controller).with_args(db, workers=0), raises(ValueError))
    assert_that(calling(eddy_pg.Controller).with_args(db, resync=-1), raises(ValueError))
    assert_that(calling(eddy_pg.DB).with_args(None, queue=""), raises(ValueError))


async def test_controller_run_with_nothing_registered_raises() -> None:
    with pytest.raises(RuntimeError, match="nothing to reconcile"):
        await eddy_pg.Controller(eddy_pg.DB(None)).run()  # type: ignore[arg-type]


def test_attempt_outside_a_reconcile_is_zero() -> None:
    assert_that(eddy_pg.attempt(), equal_to(0))
    assert_that(eddy_pg.__all__, has_length(19))
