"""Prometheus metrics for eddy reconciles.

    from eddy_pg.prometheus import Metrics

    m = Metrics()
    ctl.middleware(m.middleware)
    ctl.on_leadership_change(m.set_leading)

Install the extra to use it: ``pip install "eddy-pg[prometheus]"``. The metric
names and labels match the Go library's eddyprom module.
"""

from __future__ import annotations

import time
from collections.abc import Sequence

from prometheus_client import REGISTRY, CollectorRegistry, Gauge, Histogram

from ._controller import Handler
from ._errors import outcome
from ._kind import Ref


class Metrics:
    """eddy's Prometheus collectors:

    - ``eddy_reconcile_duration_seconds{kind,operation,outcome}``: histogram of
      how long reconciles and teardowns took
    - ``eddy_reconciles_in_flight{kind}``: reconciles running now
    - ``eddy_leader``: 1 while this replica leads

    ``operation`` is "reconcile" or "teardown"; ``outcome`` is "ok", "retry",
    "error" or "permanent".
    """

    def __init__(
        self,
        registry: CollectorRegistry = REGISTRY,
        *,
        buckets: Sequence[float] = Histogram.DEFAULT_BUCKETS,
    ) -> None:
        """Create the metrics and register them with ``registry``. Raises
        ValueError when it already has metrics of the same names."""
        self._duration = Histogram(
            "eddy_reconcile_duration_seconds",
            "Duration of eddy reconciles and teardowns.",
            ["kind", "operation", "outcome"],
            buckets=buckets,
            registry=registry,
        )
        self._in_flight = Gauge(
            "eddy_reconciles_in_flight",
            "Reconciles and teardowns running now.",
            ["kind"],
            registry=registry,
        )
        self._leader = Gauge(
            "eddy_leader", "1 while this replica is the eddy leader, else 0.", registry=registry
        )

    async def middleware(self, ref: Ref, call_next: Handler) -> None:
        """The eddy middleware that feeds the histogram and the in-flight
        gauge; pass it to ``Controller.middleware``."""
        operation = "teardown" if ref.deleted else "reconcile"
        in_flight = self._in_flight.labels(ref.kind)
        in_flight.inc()
        start = time.perf_counter()
        error: BaseException | None = None
        try:
            await call_next(ref)
        except BaseException as e:
            error = e
            raise
        finally:
            in_flight.dec()
            self._duration.labels(ref.kind, operation, outcome(error)).observe(
                time.perf_counter() - start
            )

    def set_leading(self, leading: bool) -> None:
        """Feed the leader gauge; pass it to ``Controller.on_leadership_change``."""
        self._leader.set(1 if leading else 0)
