"""The OpenTelemetry and Prometheus middleware. No database needed."""

from __future__ import annotations

import pytest
from hamcrest import assert_that, equal_to, has_length, is_
from opentelemetry import trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode
from prometheus_client import CollectorRegistry

import eddy_pg
from eddy_pg import otel
from eddy_pg.prometheus import Metrics

pytestmark = pytest.mark.unit

REF = eddy_pg.Ref("app", "7")
GONE = eddy_pg.Ref("app", "7", deleted=True)

# (what the reconciler raises, outcome label, whether the span is an error)
CASES = [
    (None, "ok", False),
    (eddy_pg.Retry(1), "retry", False),
    (RuntimeError("boom"), "error", True),
    (eddy_pg.Permanent("bad row"), "permanent", True),
]


def handler_raising(error: BaseException | None) -> eddy_pg.Handler:
    async def handle(_: eddy_pg.Ref) -> None:
        if error is not None:
            raise error

    return handle


async def call(mw: eddy_pg.Middleware, ref: eddy_pg.Ref, error: BaseException | None) -> None:
    """Run the middleware; it must pass the reconciler's exception through."""
    if error is None:
        await mw(ref, handler_raising(None))
        return
    with pytest.raises(type(error)):
        await mw(ref, handler_raising(error))


@pytest.mark.parametrize(("error", "expected"), [(e, o) for e, o, _ in CASES])
def test_outcome_classifies_how_a_reconcile_ended(
    error: BaseException | None, expected: str
) -> None:
    assert_that(eddy_pg.outcome(error), equal_to(expected))


class TestOpenTelemetry:
    @pytest.mark.parametrize(("error", "outcome", "is_error"), CASES)
    async def test_middleware_records_span_and_duration(
        self, error: BaseException | None, outcome: str, is_error: bool
    ) -> None:
        spans = InMemorySpanExporter()
        tracers = TracerProvider()
        tracers.add_span_processor(SimpleSpanProcessor(spans))
        reader = InMemoryMetricReader()
        mw = otel.middleware(
            tracer_provider=tracers, meter_provider=MeterProvider(metric_readers=[reader])
        )

        await call(mw, REF, error)

        (span,) = spans.get_finished_spans()
        assert_that(span.name, equal_to("eddy.reconcile app"))
        assert_that(
            dict(span.attributes or {}),
            equal_to(
                {"eddy.kind": "app", "eddy.id": "7", "eddy.attempt": 0, "eddy.outcome": outcome}
            ),
        )
        assert_that(span.status.status_code is StatusCode.ERROR, is_(is_error))
        assert_that(span.events, has_length(1 if is_error else 0))

        data = reader.get_metrics_data()
        assert data is not None
        (metric,) = data.resource_metrics[0].scope_metrics[0].metrics
        (point,) = metric.data.data_points
        assert_that(metric.name, equal_to("eddy.reconcile.duration"))
        assert_that(
            dict(point.attributes),
            equal_to({"eddy.kind": "app", "eddy.operation": "reconcile", "eddy.outcome": outcome}),
        )
        assert_that(point.count, equal_to(1))

    async def test_teardown_is_named_and_reconciler_runs_inside_the_span(self) -> None:
        spans = InMemorySpanExporter()
        tracers = TracerProvider()
        tracers.add_span_processor(SimpleSpanProcessor(spans))
        inner: list[int] = []

        async def handle(_: eddy_pg.Ref) -> None:
            inner.append(trace.get_current_span().get_span_context().span_id)

        await otel.middleware(tracer_provider=tracers)(GONE, handle)

        (span,) = spans.get_finished_spans()
        assert_that(span.name, equal_to("eddy.teardown app"))
        assert_that(inner, equal_to([span.context.span_id]))

    async def test_defaults_use_the_global_providers(self) -> None:
        await otel.middleware()(REF, handler_raising(None))


class TestPrometheus:
    async def test_middleware_feeds_duration_and_in_flight(self) -> None:
        registry = CollectorRegistry()
        metrics = Metrics(registry, buckets=(0.1, 1, 10))
        during: list[float | None] = []

        async def handle(_: eddy_pg.Ref) -> None:
            during.append(registry.get_sample_value("eddy_reconciles_in_flight", {"kind": "app"}))

        await metrics.middleware(REF, handle)
        for error, _, _ in CASES[1:]:
            await call(metrics.middleware, REF, error)
        await metrics.middleware(GONE, handler_raising(None))

        def count(operation: str, outcome: str) -> float | None:
            return registry.get_sample_value(
                "eddy_reconcile_duration_seconds_count",
                {"kind": "app", "operation": operation, "outcome": outcome},
            )

        assert_that(during, equal_to([1.0]))
        assert_that(
            registry.get_sample_value("eddy_reconciles_in_flight", {"kind": "app"}), equal_to(0.0)
        )
        assert_that(
            [count("reconcile", o) for o in ("ok", "retry", "error", "permanent")],
            equal_to([1.0, 1.0, 1.0, 1.0]),
        )
        assert_that(count("teardown", "ok"), equal_to(1.0))

    def test_set_leading_feeds_the_leader_gauge(self) -> None:
        registry = CollectorRegistry()
        metrics = Metrics(registry)

        metrics.set_leading(True)
        leading = registry.get_sample_value("eddy_leader")
        metrics.set_leading(False)

        assert_that((leading, registry.get_sample_value("eddy_leader")), equal_to((1.0, 0.0)))

    def test_registering_twice_raises_value_error(self) -> None:
        registry = CollectorRegistry()
        Metrics(registry)
        with pytest.raises(ValueError, match="Duplicated"):
            Metrics(registry)
