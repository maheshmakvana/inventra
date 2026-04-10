"""Advanced features for inventra — caching, pipeline, async, observability, diff, security."""
from __future__ import annotations

import asyncio
import functools
import hashlib
import json
import logging
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, TypeVar

from inventra.models import InventoryItem, SyncEvent, SyncReport, SyncStatus

logger = logging.getLogger(__name__)
T = TypeVar("T")


# ─────────────────────────────────────────────────────────────────────────────
# CACHING
# ─────────────────────────────────────────────────────────────────────────────

class InventoryCache:
    """LRU + TTL cache for inventory queries, keyed by SHA-256."""

    def __init__(self, max_size: int = 256, ttl_seconds: float = 300.0) -> None:
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._store: OrderedDict[str, Tuple[Any, float]] = OrderedDict()
        self._hits = 0
        self._misses = 0
        self._lock = threading.Lock()

    def _key(self, *args: Any, **kwargs: Any) -> str:
        raw = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True, default=str)
        return hashlib.sha256(raw.encode()).hexdigest()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key not in self._store:
                self._misses += 1
                return None
            value, expires_at = self._store[key]
            if time.monotonic() > expires_at:
                del self._store[key]
                self._misses += 1
                return None
            self._store.move_to_end(key)
            self._hits += 1
            return value

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            if key in self._store:
                self._store.move_to_end(key)
            self._store[key] = (value, time.monotonic() + self.ttl_seconds)
            while len(self._store) > self.max_size:
                self._store.popitem(last=False)

    def memoize(self, fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            key = self._key(fn.__name__, *args, **kwargs)
            cached = self.get(key)
            if cached is not None:
                return cached  # type: ignore[return-value]
            result = fn(*args, **kwargs)
            self.set(key, result)
            return result
        return wrapper

    def stats(self) -> Dict[str, Any]:
        total = self._hits + self._misses
        return {
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": round(self._hits / total, 3) if total else 0.0,
            "size": len(self._store),
            "max_size": self.max_size,
            "ttl_seconds": self.ttl_seconds,
        }

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class _PipelineStep:
    name: str
    fn: Callable
    args: Tuple = field(default_factory=tuple)
    kwargs: Dict = field(default_factory=dict)


class SyncPipeline:
    """Fluent pipeline for chaining inventory transforms with retry and audit log."""

    def __init__(self) -> None:
        self._steps: List[_PipelineStep] = []
        self._audit: List[Dict[str, Any]] = []
        self._retry_count = 0
        self._retry_delay = 0.5

    def map(self, fn: Callable[[List[InventoryItem]], List[InventoryItem]], name: str = "") -> "SyncPipeline":
        self._steps.append(_PipelineStep(name=name or fn.__name__, fn=fn))
        return self

    def filter(self, predicate: Callable[[InventoryItem], bool], name: str = "") -> "SyncPipeline":
        def _filter(items: List[InventoryItem]) -> List[InventoryItem]:
            return [i for i in items if predicate(i)]
        self._steps.append(_PipelineStep(name=name or "filter", fn=_filter))
        return self

    def with_retry(self, count: int = 3, delay: float = 0.5) -> "SyncPipeline":
        self._retry_count = count
        self._retry_delay = delay
        return self

    def run(self, items: List[InventoryItem]) -> List[InventoryItem]:
        result = items
        for step in self._steps:
            attempts = 0
            while True:
                try:
                    t0 = time.monotonic()
                    result = step.fn(result)
                    elapsed = time.monotonic() - t0
                    self._audit.append({"step": step.name, "in": len(items), "out": len(result), "elapsed_ms": round(elapsed * 1000, 2), "ok": True})
                    break
                except Exception as exc:
                    attempts += 1
                    if attempts > self._retry_count:
                        self._audit.append({"step": step.name, "error": str(exc), "ok": False})
                        raise
                    time.sleep(self._retry_delay)
        return result

    async def arun(self, items: List[InventoryItem]) -> List[InventoryItem]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: self.run(items))

    def audit_log(self) -> List[Dict[str, Any]]:
        return list(self._audit)


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class InventoryRule:
    field: str
    rule_type: str  # "min_qty", "max_qty", "required_channel", "sku_pattern"
    value: Any
    message: str = ""


class InventoryValidator:
    """Declarative rule-based validator for inventory items."""

    def __init__(self) -> None:
        self._rules: List[InventoryRule] = []

    def add_rule(self, rule: InventoryRule) -> "InventoryValidator":
        self._rules.append(rule)
        return self

    def validate(self, item: InventoryItem) -> Tuple[bool, List[str]]:
        errors: List[str] = []
        for rule in self._rules:
            if rule.rule_type == "min_qty" and item.quantity < rule.value:
                errors.append(rule.message or f"SKU {item.sku}: quantity {item.quantity} below min {rule.value}")
            elif rule.rule_type == "max_qty" and item.quantity > rule.value:
                errors.append(rule.message or f"SKU {item.sku}: quantity {item.quantity} above max {rule.value}")
            elif rule.rule_type == "required_channel" and item.channel != rule.value:
                errors.append(rule.message or f"SKU {item.sku}: expected channel {rule.value}, got {item.channel}")
            elif rule.rule_type == "sku_pattern":
                import re
                if not re.fullmatch(rule.value, item.sku):
                    errors.append(rule.message or f"SKU {item.sku} does not match pattern {rule.value}")
        return len(errors) == 0, errors

    def validate_batch(self, items: List[InventoryItem]) -> Dict[str, List[str]]:
        return {item.sku: self.validate(item)[1] for item in items if not self.validate(item)[0]}


# ─────────────────────────────────────────────────────────────────────────────
# ASYNC & CONCURRENCY
# ─────────────────────────────────────────────────────────────────────────────

class RateLimiter:
    """Token-bucket rate limiter (sync + async)."""

    def __init__(self, rate: float, capacity: float) -> None:
        self.rate = rate
        self.capacity = capacity
        self._tokens = capacity
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.rate)
        self._last = now

    def acquire(self, tokens: float = 1.0) -> bool:
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    async def async_acquire(self, tokens: float = 1.0) -> bool:
        while not self.acquire(tokens):
            await asyncio.sleep(0.05)
        return True


class CancellationToken:
    """Cooperative cancellation for async batch operations."""

    def __init__(self) -> None:
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled


def batch_sync_items(
    items: List[InventoryItem],
    sync_fn: Callable[[InventoryItem], SyncEvent],
    max_workers: int = 4,
    token: Optional[CancellationToken] = None,
) -> List[SyncEvent]:
    """Sync multiple items concurrently with a thread pool."""
    results: List[SyncEvent] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(sync_fn, item): item for item in items}
        for future in as_completed(futures):
            if token and token.is_cancelled:
                break
            try:
                results.append(future.result())
            except Exception as exc:
                item = futures[future]
                results.append(SyncEvent(sku=item.sku, channel=item.channel, status=SyncStatus.FAILED, error=str(exc)))
    return results


async def abatch_sync_items(
    items: List[InventoryItem],
    sync_fn: Callable[[InventoryItem], SyncEvent],
    max_concurrency: int = 4,
    token: Optional[CancellationToken] = None,
) -> List[SyncEvent]:
    """Async concurrent sync with semaphore."""
    sem = asyncio.Semaphore(max_concurrency)
    loop = asyncio.get_event_loop()

    async def run_one(item: InventoryItem) -> SyncEvent:
        async with sem:
            if token and token.is_cancelled:
                return SyncEvent(sku=item.sku, channel=item.channel, status=SyncStatus.FAILED, error="cancelled")
            return await loop.run_in_executor(None, lambda: sync_fn(item))

    return list(await asyncio.gather(*[run_one(i) for i in items], return_exceptions=False))


# ─────────────────────────────────────────────────────────────────────────────
# OBSERVABILITY
# ─────────────────────────────────────────────────────────────────────────────

class SyncProfiler:
    """Time and memory profiler for sync operations."""

    def __init__(self) -> None:
        self._records: List[Dict[str, Any]] = []

    def profile(self, fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            import tracemalloc
            tracemalloc.start()
            t0 = time.monotonic()
            try:
                result = fn(*args, **kwargs)
                elapsed = time.monotonic() - t0
                _, peak = tracemalloc.get_traced_memory()
                tracemalloc.stop()
                self._records.append({"fn": fn.__name__, "elapsed_ms": round(elapsed * 1000, 2), "peak_bytes": peak, "ok": True})
                return result
            except Exception as exc:
                elapsed = time.monotonic() - t0
                tracemalloc.stop()
                self._records.append({"fn": fn.__name__, "elapsed_ms": round(elapsed * 1000, 2), "error": str(exc), "ok": False})
                raise
        return wrapper

    def report(self) -> List[Dict[str, Any]]:
        return list(self._records)


# ─────────────────────────────────────────────────────────────────────────────
# STREAMING
# ─────────────────────────────────────────────────────────────────────────────

def stream_inventory(items: List[InventoryItem]) -> Generator[InventoryItem, None, None]:
    """Generator yielding inventory items one at a time."""
    for item in items:
        yield item


def inventory_to_ndjson(items: List[InventoryItem]) -> Generator[str, None, None]:
    """Stream inventory items as NDJSON lines."""
    for item in items:
        yield item.model_dump_json() + "\n"


def inventory_chunks(items: List[InventoryItem], size: int = 100) -> Generator[List[InventoryItem], None, None]:
    """Yield inventory items in chunks."""
    for i in range(0, len(items), size):
        yield items[i: i + size]


# ─────────────────────────────────────────────────────────────────────────────
# DIFF & REGRESSION
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class InventoryDiff:
    """Diff between two inventory snapshots."""

    added: List[str] = field(default_factory=list)        # SKUs new in snapshot_b
    removed: List[str] = field(default_factory=list)      # SKUs only in snapshot_a
    modified: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # {sku: {field: (old, new)}}

    def summary(self) -> Dict[str, Any]:
        return {"added": len(self.added), "removed": len(self.removed), "modified": len(self.modified)}

    def to_json(self) -> str:
        return json.dumps({"added": self.added, "removed": self.removed, "modified": self.modified})


def diff_inventory(
    snapshot_a: List[InventoryItem],
    snapshot_b: List[InventoryItem],
) -> InventoryDiff:
    """Compare two inventory snapshots."""
    map_a = {i.sku: i for i in snapshot_a}
    map_b = {i.sku: i for i in snapshot_b}

    diff = InventoryDiff(
        added=[sku for sku in map_b if sku not in map_a],
        removed=[sku for sku in map_a if sku not in map_b],
    )
    for sku in set(map_a) & set(map_b):
        changes: Dict[str, Any] = {}
        for f in ("quantity", "reserved", "price", "channel"):
            va, vb = getattr(map_a[sku], f), getattr(map_b[sku], f)
            if va != vb:
                changes[f] = {"old": va, "new": vb}
        if changes:
            diff.modified[sku] = changes
    return diff


# ─────────────────────────────────────────────────────────────────────────────
# SECURITY
# ─────────────────────────────────────────────────────────────────────────────

class AuditLog:
    """Append-only audit log for inventory operations."""

    def __init__(self) -> None:
        self._entries: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def record(self, action: str, sku: str, channel: str, detail: Optional[str] = None) -> None:
        with self._lock:
            self._entries.append({"ts": datetime.utcnow().isoformat(), "action": action, "sku": sku, "channel": channel, "detail": detail})

    def export(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._entries)


class PIIScrubber:
    """Scrub potential PII from inventory metadata."""

    import re as _re
    _EMAIL = _re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
    _PHONE = _re.compile(r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b")

    @classmethod
    def scrub(cls, text: str) -> str:
        text = cls._EMAIL.sub("[EMAIL]", text)
        text = cls._PHONE.sub("[PHONE]", text)
        return text

    @classmethod
    def scrub_metadata(cls, metadata: Dict[str, Any]) -> Dict[str, Any]:
        return {k: cls.scrub(str(v)) if isinstance(v, str) else v for k, v in metadata.items()}


# ─────────────────────────────────────────────────────────────────────────────
# EXPERT: DEMAND FORECAST ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class DemandForecastEngine:
    """
    Lightweight demand forecasting using exponential smoothing (Holt's method).

    Maintains a per-SKU series of historical quantities and produces a next-period
    forecast plus a confidence interval. No external ML dependencies required —
    all math is pure Python. Designed for SMB inventory planners who need
    actionable reorder signals without a data science stack.

    Usage::

        engine = DemandForecastEngine(alpha=0.3, beta=0.1)
        engine.record("SKU-001", 120)
        engine.record("SKU-001", 135)
        engine.record("SKU-001", 128)
        print(engine.forecast("SKU-001"))
        # {'sku': 'SKU-001', 'forecast': 131.4, 'lower': 118.3, 'upper': 144.5, 'trend': 3.2}
    """

    def __init__(self, alpha: float = 0.3, beta: float = 0.1, confidence_z: float = 1.645) -> None:
        if not (0 < alpha <= 1) or not (0 < beta <= 1):
            raise ValueError("alpha and beta must be in (0, 1]")
        self.alpha = alpha
        self.beta = beta
        self.confidence_z = confidence_z   # 1.645 ≈ 90%, 1.96 ≈ 95%
        self._series: Dict[str, List[float]] = {}
        self._lock = threading.Lock()

    def record(self, sku: str, quantity: float) -> None:
        """Append a new observed quantity for a SKU."""
        with self._lock:
            self._series.setdefault(sku, []).append(float(quantity))
        logger.debug("DemandForecastEngine: recorded sku=%s qty=%s", sku, quantity)

    def _holt(self, series: List[float]) -> Tuple[float, float]:
        """Run Holt double-exponential smoothing; return (level, trend)."""
        if len(series) == 1:
            return series[0], 0.0
        level = series[0]
        trend = series[1] - series[0]
        for obs in series[1:]:
            prev_level = level
            level = self.alpha * obs + (1 - self.alpha) * (level + trend)
            trend = self.beta * (level - prev_level) + (1 - self.beta) * trend
        return level, trend

    def forecast(self, sku: str, periods_ahead: int = 1) -> Dict[str, Any]:
        """
        Return a forecast dict for a given SKU.

        Keys: sku, forecast, lower, upper, trend, observations.
        Raises KeyError if no data recorded for the SKU.
        """
        with self._lock:
            series = list(self._series.get(sku, []))
        if not series:
            raise KeyError(f"No demand data recorded for SKU '{sku}'")
        level, trend = self._holt(series)
        forecast_val = level + trend * periods_ahead

        # Residuals-based standard error for CI
        import statistics
        if len(series) >= 2:
            residuals = [abs(series[i] - (series[i - 1] if i > 0 else series[0])) for i in range(1, len(series))]
            std_err = statistics.stdev(residuals) if len(residuals) > 1 else residuals[0]
        else:
            std_err = forecast_val * 0.1  # 10% fallback

        margin = self.confidence_z * std_err * (periods_ahead ** 0.5)
        return {
            "sku": sku,
            "forecast": round(forecast_val, 2),
            "lower": round(max(0.0, forecast_val - margin), 2),
            "upper": round(forecast_val + margin, 2),
            "trend": round(trend, 4),
            "observations": len(series),
            "periods_ahead": periods_ahead,
        }

    def reorder_signals(
        self,
        items: List[InventoryItem],
        safety_stock_days: int = 7,
        lead_time_days: int = 3,
    ) -> List[Dict[str, Any]]:
        """
        Cross-reference live inventory with demand forecasts to produce reorder signals.

        Returns a list of SKUs where available stock falls below
        (daily_forecast * (safety_stock_days + lead_time_days)).
        """
        signals = []
        for item in items:
            try:
                fc = self.forecast(item.sku)
            except KeyError:
                continue
            daily_rate = fc["forecast"] / 30  # assume monthly data points
            reorder_point = daily_rate * (safety_stock_days + lead_time_days)
            if item.available < reorder_point:
                signals.append({
                    "sku": item.sku,
                    "channel": item.channel,
                    "available": item.available,
                    "reorder_point": round(reorder_point, 2),
                    "days_of_stock": round(item.available / daily_rate, 1) if daily_rate > 0 else None,
                    "urgency": "critical" if item.available == 0 else "high" if item.available < reorder_point * 0.5 else "medium",
                    "forecasted_monthly_demand": fc["forecast"],
                })
        return sorted(signals, key=lambda x: {"critical": 0, "high": 1, "medium": 2}[x["urgency"]])

    def sku_summary(self) -> Dict[str, int]:
        """Return observation counts per SKU."""
        with self._lock:
            return {sku: len(series) for sku, series in self._series.items()}


# ─────────────────────────────────────────────────────────────────────────────
# EXPERT: CHANNEL HEALTH MONITOR
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ChannelHealthSnapshot:
    """Point-in-time health metrics for a single channel."""
    channel: str
    total_syncs: int = 0
    failed_syncs: int = 0
    conflict_count: int = 0
    avg_sync_ms: float = 0.0
    last_sync_ts: Optional[str] = None
    error_rate: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "channel": self.channel,
            "total_syncs": self.total_syncs,
            "failed_syncs": self.failed_syncs,
            "conflict_count": self.conflict_count,
            "avg_sync_ms": round(self.avg_sync_ms, 2),
            "error_rate": round(self.error_rate, 4),
            "last_sync_ts": self.last_sync_ts,
            "status": self._status(),
        }

    def _status(self) -> str:
        if self.error_rate > 0.3:
            return "degraded"
        if self.error_rate > 0.1:
            return "warning"
        return "healthy"


class ChannelHealthMonitor:
    """
    Track per-channel sync health metrics in real time.

    Records every SyncEvent outcome and maintains rolling success/failure
    counters plus a rolling average of sync durations. Generates structured
    health reports and Markdown dashboards for ops teams.

    Usage::

        monitor = ChannelHealthMonitor()
        monitor.record_event(event, elapsed_ms=45.2)
        print(monitor.report("shopify"))
    """

    def __init__(self) -> None:
        self._channels: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def _ensure(self, channel: str) -> None:
        if channel not in self._channels:
            self._channels[channel] = {
                "total": 0, "failed": 0, "conflicts": 0,
                "elapsed_ms_sum": 0.0, "last_ts": None,
            }

    def record_event(self, event: SyncEvent, elapsed_ms: float = 0.0) -> None:
        """Record a SyncEvent outcome; call after every sync operation."""
        with self._lock:
            ch = event.channel
            self._ensure(ch)
            self._channels[ch]["total"] += 1
            if event.status == SyncStatus.FAILED:
                self._channels[ch]["failed"] += 1
            if event.status == SyncStatus.CONFLICT:
                self._channels[ch]["conflicts"] += 1
            self._channels[ch]["elapsed_ms_sum"] += elapsed_ms
            self._channels[ch]["last_ts"] = datetime.utcnow().isoformat()
        logger.debug("ChannelHealthMonitor: recorded %s for channel=%s", event.status.value, event.channel)

    def snapshot(self, channel: str) -> ChannelHealthSnapshot:
        """Return a health snapshot for one channel."""
        with self._lock:
            if channel not in self._channels:
                return ChannelHealthSnapshot(channel=channel)
            d = self._channels[channel]
        total = d["total"]
        return ChannelHealthSnapshot(
            channel=channel,
            total_syncs=total,
            failed_syncs=d["failed"],
            conflict_count=d["conflicts"],
            avg_sync_ms=d["elapsed_ms_sum"] / total if total else 0.0,
            last_sync_ts=d["last_ts"],
            error_rate=d["failed"] / total if total else 0.0,
        )

    def report(self, channel: str) -> Dict[str, Any]:
        """Return a structured health dict for a channel."""
        return self.snapshot(channel).to_dict()

    def all_channels(self) -> List[Dict[str, Any]]:
        """Return health snapshots for all tracked channels, sorted by error_rate desc."""
        with self._lock:
            channels = list(self._channels.keys())
        snapshots = [self.snapshot(ch).to_dict() for ch in channels]
        return sorted(snapshots, key=lambda x: x["error_rate"], reverse=True)

    def to_markdown(self) -> str:
        """Render a Markdown dashboard table of all channel health."""
        rows = self.all_channels()
        lines = ["# Channel Health Dashboard", "",
                 "| Channel | Total | Failed | Conflicts | Avg ms | Error Rate | Status |",
                 "|---------|-------|--------|-----------|--------|------------|--------|"]
        for r in rows:
            lines.append(
                f"| {r['channel']} | {r['total_syncs']} | {r['failed_syncs']} | "
                f"{r['conflict_count']} | {r['avg_sync_ms']} | "
                f"{r['error_rate']:.1%} | {r['status'].upper()} |"
            )
        return "\n".join(lines)

    def degraded_channels(self) -> List[str]:
        """Return channel names currently in degraded or warning state."""
        return [r["channel"] for r in self.all_channels() if r["status"] in ("degraded", "warning")]


# ─────────────────────────────────────────────────────────────────────────────
# EXPERT: OVERSELL RISK ANALYZER
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class OversellRisk:
    """Oversell risk assessment for a single SKU."""
    sku: str
    available: int
    channels: List[str]
    per_channel_quantities: Dict[str, int]
    total_reserved: int
    net_available: int
    risk_level: str   # "none", "low", "medium", "high", "critical"
    recommendation: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sku": self.sku,
            "available": self.available,
            "channels": self.channels,
            "per_channel_quantities": self.per_channel_quantities,
            "total_reserved": self.total_reserved,
            "net_available": self.net_available,
            "risk_level": self.risk_level,
            "recommendation": self.recommendation,
        }


class OversellRiskAnalyzer:
    """
    Identify SKUs at risk of overselling across multiple sales channels.

    Aggregates inventory across all channel instances of a SKU, computes net
    available stock (quantity − reserved), and classifies oversell risk. Also
    detects channel-level stock divergence that may signal sync lag or conflicts.

    Usage::

        analyzer = OversellRiskAnalyzer()
        risks = analyzer.analyze(items)
        print(analyzer.to_markdown(risks))
    """

    _THRESHOLDS: Dict[str, int] = {
        "critical": 0,   # net_available <= 0
        "high": 5,       # net_available <= 5
        "medium": 15,    # net_available <= 15
        "low": 30,       # net_available <= 30
    }

    def analyze(self, items: List[InventoryItem]) -> List[OversellRisk]:
        """Compute oversell risk for all SKUs in the item list."""
        # Group by SKU across all channels
        sku_map: Dict[str, List[InventoryItem]] = {}
        for item in items:
            sku_map.setdefault(item.sku, []).append(item)

        risks = []
        for sku, records in sku_map.items():
            total_qty = sum(r.quantity for r in records)
            total_reserved = sum(r.reserved for r in records)
            net = total_qty - total_reserved
            channels = [r.channel for r in records]
            per_ch = {r.channel: r.quantity for r in records}

            if net <= self._THRESHOLDS["critical"]:
                level = "critical"
                rec = f"STOP SELLING {sku} immediately — net available is {net}. Pause all channel listings."
            elif net <= self._THRESHOLDS["high"]:
                level = "high"
                rec = f"Restrict {sku} to highest-priority channel only; only {net} units available."
            elif net <= self._THRESHOLDS["medium"]:
                level = "medium"
                rec = f"Monitor {sku} closely; {net} units available across {len(channels)} channels."
            elif net <= self._THRESHOLDS["low"]:
                level = "low"
                rec = f"Trigger reorder for {sku}; {net} units remain."
            else:
                level = "none"
                rec = f"{sku} is well-stocked with {net} units available."

            risks.append(OversellRisk(
                sku=sku,
                available=total_qty,
                channels=channels,
                per_channel_quantities=per_ch,
                total_reserved=total_reserved,
                net_available=net,
                risk_level=level,
                recommendation=rec,
            ))

        order = {"critical": 0, "high": 1, "medium": 2, "low": 3, "none": 4}
        return sorted(risks, key=lambda x: order[x.risk_level])

    def critical_skus(self, items: List[InventoryItem]) -> List[OversellRisk]:
        """Return only SKUs at critical or high risk."""
        return [r for r in self.analyze(items) if r.risk_level in ("critical", "high")]

    def divergence_report(self, items: List[InventoryItem]) -> List[Dict[str, Any]]:
        """
        Detect SKUs where stock levels differ significantly across channels
        (potential sync lag or missed updates).
        """
        sku_map: Dict[str, List[InventoryItem]] = {}
        for item in items:
            sku_map.setdefault(item.sku, []).append(item)

        diverged = []
        for sku, records in sku_map.items():
            if len(records) < 2:
                continue
            qtys = [r.quantity for r in records]
            max_q, min_q = max(qtys), min(qtys)
            spread = max_q - min_q
            if spread > 0:
                diverged.append({
                    "sku": sku,
                    "max_qty": max_q,
                    "min_qty": min_q,
                    "spread": spread,
                    "spread_pct": round(spread / max_q * 100, 1) if max_q else 0.0,
                    "channels": {r.channel: r.quantity for r in records},
                })
        return sorted(diverged, key=lambda x: x["spread"], reverse=True)

    def to_markdown(self, risks: List[OversellRisk]) -> str:
        """Render a Markdown risk report table."""
        lines = ["# Oversell Risk Report", "",
                 "| SKU | Net Available | Channels | Risk Level | Recommendation |",
                 "|-----|--------------|----------|------------|----------------|"]
        for r in risks:
            lines.append(
                f"| {r.sku} | {r.net_available} | {', '.join(r.channels)} | "
                f"{r.risk_level.upper()} | {r.recommendation} |"
            )
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# EXPERT: SYNC SPAN EMITTER (OpenTelemetry with stdlib fallback)
# ─────────────────────────────────────────────────────────────────────────────

class SyncSpanEmitter:
    """
    Emit OpenTelemetry spans for inventory sync operations.
    Falls back to structured logging when opentelemetry-sdk is not installed.
    """

    def __init__(self, service_name: str = "inventra") -> None:
        self._service = service_name
        self._otel_available = False
        self._tracer: Any = None
        try:
            from opentelemetry import trace
            from opentelemetry.sdk.trace import TracerProvider
            provider = TracerProvider()
            trace.set_tracer_provider(provider)
            self._tracer = trace.get_tracer(service_name)
            self._otel_available = True
            logger.debug("SyncSpanEmitter: OpenTelemetry tracer initialised")
        except ImportError:
            logger.debug("SyncSpanEmitter: opentelemetry not installed — using log fallback")

    def span(self, operation: str, attributes: Optional[Dict[str, Any]] = None) -> Any:
        """Context manager: emit an OTEL span or log span start/end."""
        if self._otel_available and self._tracer is not None:
            span = self._tracer.start_span(operation)
            if attributes:
                for k, v in attributes.items():
                    span.set_attribute(k, str(v))
            return span
        return _LogSpan(operation, attributes or {}, self._service)

    def emit_sync(self, event: SyncEvent, elapsed_ms: float = 0.0) -> None:
        """Emit a span for a completed sync event."""
        attrs = {
            "sku": event.sku,
            "channel": event.channel,
            "status": event.status.value,
            "elapsed_ms": elapsed_ms,
        }
        with self.span("inventory.sync", attrs):
            pass

    def emit_report(self, report: SyncReport) -> None:
        """Emit a span summarising a full sync report."""
        attrs = {
            "total": report.total,
            "success": report.success,
            "failed": report.failed,
            "conflicts": report.conflicts,
        }
        with self.span("inventory.sync_report", attrs):
            pass


class _LogSpan:
    """Stdlib-logging fallback span used when OTEL is unavailable."""

    def __init__(self, name: str, attrs: Dict[str, Any], service: str) -> None:
        self._name = name
        self._attrs = attrs
        self._service = service
        self._t0 = time.monotonic()

    def __enter__(self) -> "_LogSpan":
        logger.debug("[span:start] service=%s operation=%s attrs=%s", self._service, self._name, self._attrs)
        return self

    def __exit__(self, *args: Any) -> None:
        elapsed = round((time.monotonic() - self._t0) * 1000, 2)
        logger.debug("[span:end] service=%s operation=%s elapsed_ms=%s", self._service, self._name, elapsed)
