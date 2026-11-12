"""OpenTelemetry instrumentation for eddy reconciles.

    from eddy_pg.otel import middleware

    ctl.middleware(middleware())

Install the extra to use it: ``pip install "eddy-pg[otel]"``. The span names,
attributes and metric match the Go library's eddyotel module, so both show up
the same way in a backend.
"""

from __future__ import annotations

import time

from opentelemetry import metrics, trace
from opentelemetry.metrics import MeterProvider
from opentelemetry.trace import SpanKind, Status, StatusCode, TracerProvider

from ._controller import Handler, Middleware, attempt
from ._errors import OUTCOME_ERROR, OUTCOME_PERMANENT, outcome
from ._kind import Ref

SCOPE = "eddy_pg"


def middleware(
    *,
    tracer_provider: TracerProvider | None = None,
    meter_provider: MeterProvider | None = None,
) -> Middleware:
    """Return a middleware that, for every reconcile and teardown:

    - starts a span named ``eddy.reconcile <kind>`` or ``eddy.teardown <kind>``
      with the attributes ``eddy.kind``, ``eddy.id``, ``eddy.attempt`` and
      ``eddy.outcome``, marked as an error unless the outcome is ok or retry;
    - records its duration in the histogram ``eddy.reconcile.duration``
      (seconds), with ``eddy.kind``, ``eddy.operation`` and ``eddy.outcome``.

    The providers default to the global ones. Spans the reconciler starts
    become children of the reconcile span.
    """
    tracer = trace.get_tracer(SCOPE, tracer_provider=tracer_provider)
    duration = metrics.get_meter(SCOPE, meter_provider=meter_provider).create_histogram(
        "eddy.reconcile.duration",
        unit="s",
        description="Duration of eddy reconciles and teardowns.",
    )

    async def instrument(ref: Ref, call_next: Handler) -> None:
        operation = "teardown" if ref.deleted else "reconcile"
        with tracer.start_as_current_span(
            f"eddy.{operation} {ref.kind}",
            kind=SpanKind.CONSUMER,
            attributes={"eddy.kind": ref.kind, "eddy.id": ref.id, "eddy.attempt": attempt()},
            # Retry is raised as an exception but is not an error: decide below.
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            start = time.perf_counter()
            error: BaseException | None = None
            try:
                await call_next(ref)
            except BaseException as e:
                error = e
                raise
            finally:
                result = outcome(error)
                span.set_attribute("eddy.outcome", result)
                if error is not None and result in (OUTCOME_ERROR, OUTCOME_PERMANENT):
                    span.record_exception(error)
                    span.set_status(Status(StatusCode.ERROR, str(error)))
                duration.record(
                    time.perf_counter() - start,
                    {"eddy.kind": ref.kind, "eddy.operation": operation, "eddy.outcome": result},
                )

    return instrument
