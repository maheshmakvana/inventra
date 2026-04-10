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
