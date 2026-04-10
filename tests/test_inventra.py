"""Tests for inventra — multi-channel inventory sync."""
import asyncio
import pytest
from datetime import datetime

from inventra import (
    InventorySync,
    InventoryItem,
    SyncStatus,
    ConflictResolution,
    InventoryCache,
    InventoryValidator,
    InventoryRule,
    SyncPipeline,
    RateLimiter,
    CancellationToken,
    batch_sync_items,
    diff_inventory,
    inventory_to_ndjson,
    inventory_chunks,
    AuditLog,
    PIIScrubber,
    stream_inventory,
)
from inventra.channels.mock import MockChannelAdapter


def make_adapter(name: str, inventory: dict) -> MockChannelAdapter:
    adapter = MockChannelAdapter(config={}, initial_inventory=inventory)
    adapter.channel_name = name
    adapter.connect()
    return adapter


def make_item(sku: str, qty: int, channel: str = "shopify") -> InventoryItem:
    return InventoryItem(sku=sku, channel=channel, quantity=qty)


# ─── Models ───────────────────────────────────────────────────────────────────

def test_inventory_item_available():
    item = InventoryItem(sku="ABC", channel="shopify", quantity=10, reserved=3)
    assert item.available == 7


def test_inventory_item_sku_normalized():
    item = InventoryItem(sku="  abc-001  ", channel="shopify", quantity=5)
    assert item.sku == "ABC-001"


def test_inventory_item_negative_reserved_clamped():
    item = InventoryItem(sku="X", channel="shopify", quantity=5, reserved=0)
    assert item.available == 5


# ─── Mock Channel ─────────────────────────────────────────────────────────────

def test_mock_adapter_get_inventory():
    adapter = make_adapter("shopify", {"SKU-A": 10, "SKU-B": 5})
    items = adapter.get_inventory()
    skus = {i.sku for i in items}
    assert "SKU-A" in skus
    assert "SKU-B" in skus


def test_mock_adapter_update_inventory():
    adapter = make_adapter("shopify", {"SKU-A": 10})
    new_item = make_item("SKU-A", 7)
    event = adapter.update_inventory(new_item)
    assert event.status == SyncStatus.SUCCESS
    assert event.new_quantity == 7
    assert event.old_quantity == 10


def test_mock_adapter_context_manager():
    adapter = MockChannelAdapter(config={})
    adapter.channel_name = "test"
    with adapter:
        assert adapter.is_connected
    assert not adapter.is_connected


# ─── Sync Engine ──────────────────────────────────────────────────────────────

def test_sync_resolves_conflict_lowest_stock():
    ch_a = make_adapter("shopify", {"SKU-X": 50})
    ch_b = make_adapter("amazon", {"SKU-X": 30})
    sync = InventorySync(channels=[ch_a, ch_b], conflict_resolution=ConflictResolution.LOWEST_STOCK)
    report = sync.sync()
    assert report.conflicts == 1
    assert report.conflict_records[0].resolved_quantity == 30


def test_sync_resolves_conflict_highest_stock():
    ch_a = make_adapter("shopify", {"SKU-X": 50})
    ch_b = make_adapter("amazon", {"SKU-X": 30})
    sync = InventorySync(channels=[ch_a, ch_b], conflict_resolution=ConflictResolution.HIGHEST_STOCK)
    report = sync.sync()
    assert report.conflict_records[0].resolved_quantity == 50


def test_sync_no_conflict_when_equal():
    ch_a = make_adapter("shopify", {"SKU-Y": 20})
    ch_b = make_adapter("amazon", {"SKU-Y": 20})
    sync = InventorySync(channels=[ch_a, ch_b])
    report = sync.sync()
    assert report.conflicts == 0


def test_sync_dry_run_does_not_update():
    ch_a = make_adapter("shopify", {"SKU-Z": 50})
    ch_b = make_adapter("amazon", {"SKU-Z": 10})
    sync = InventorySync(channels=[ch_a, ch_b])
    report = sync.sync(dry_run=True)
    assert report.success == 0
    shopify_items = ch_a.get_inventory(["SKU-Z"])
    assert shopify_items[0].quantity == 50  # unchanged


def test_get_stock():
    ch_a = make_adapter("shopify", {"SKU-1": 15})
    ch_b = make_adapter("amazon", {"SKU-1": 12})
    sync = InventorySync(channels=[ch_a, ch_b])
    stock = sync.get_stock("SKU-1")
    assert stock["shopify"] == 15
    assert stock["amazon"] == 12


def test_async_sync():
    ch_a = make_adapter("shopify", {"SKU-A": 40})
    ch_b = make_adapter("amazon", {"SKU-A": 30})
    sync = InventorySync(channels=[ch_a, ch_b])
    report = asyncio.run(sync.async_sync())
    assert report.conflicts == 1


# ─── Cache ────────────────────────────────────────────────────────────────────

def test_cache_hit_miss():
    cache = InventoryCache(max_size=10, ttl_seconds=60)
    cache.set("k1", "v1")
    assert cache.get("k1") == "v1"
    assert cache.get("k2") is None
    stats = cache.stats()
    assert stats["hits"] == 1
    assert stats["misses"] == 1


def test_cache_memoize():
    cache = InventoryCache(max_size=10, ttl_seconds=60)
    call_count = [0]

    @cache.memoize
    def expensive(x):
        call_count[0] += 1
        return x * 2

    assert expensive(5) == 10
    assert expensive(5) == 10
    assert call_count[0] == 1


def test_cache_eviction():
    cache = InventoryCache(max_size=3, ttl_seconds=60)
    for i in range(5):
        cache.set(f"k{i}", i)
    assert len(cache._store) == 3


# ─── Pipeline ─────────────────────────────────────────────────────────────────

def test_pipeline_filter():
    items = [make_item(f"SKU-{i}", i * 10) for i in range(5)]
    pipeline = SyncPipeline().filter(lambda it: it.quantity > 20, name="qty_filter")
    result = pipeline.run(items)
    assert all(i.quantity > 20 for i in result)


def test_pipeline_map():
    items = [make_item("SKU-1", 10)]
    pipeline = SyncPipeline().map(lambda its: [i.model_copy(update={"reserved": 2}) for i in its])
    result = pipeline.run(items)
    assert result[0].reserved == 2


def test_pipeline_audit_log():
    items = [make_item("SKU-1", 10)]
    pipeline = SyncPipeline().filter(lambda i: True, name="no_op")
    pipeline.run(items)
    log = pipeline.audit_log()
    assert len(log) == 1
    assert log[0]["step"] == "no_op"
    assert log[0]["ok"] is True


def test_pipeline_async():
    items = [make_item("SKU-1", 5)]
    pipeline = SyncPipeline().filter(lambda i: True)
    result = asyncio.run(pipeline.arun(items))
    assert len(result) == 1


# ─── Validator ────────────────────────────────────────────────────────────────

def test_validator_passes():
    validator = InventoryValidator()
    validator.add_rule(InventoryRule(field="quantity", rule_type="min_qty", value=0))
    item = make_item("SKU-OK", 10)
    ok, errors = validator.validate(item)
    assert ok
    assert errors == []


def test_validator_fails_min_qty():
    validator = InventoryValidator()
    validator.add_rule(InventoryRule(field="quantity", rule_type="min_qty", value=5, message="Too low"))
    item = make_item("SKU-LOW", 2)
    ok, errors = validator.validate(item)
    assert not ok
    assert any("Too low" in e for e in errors)


# ─── Rate Limiter ─────────────────────────────────────────────────────────────

def test_rate_limiter_allows_within_capacity():
    limiter = RateLimiter(rate=100, capacity=5)
    for _ in range(5):
        assert limiter.acquire()


def test_rate_limiter_blocks_when_exhausted():
    limiter = RateLimiter(rate=0.01, capacity=1)
    assert limiter.acquire()
    assert not limiter.acquire()


# ─── Concurrency ──────────────────────────────────────────────────────────────

def test_batch_sync_items():
    adapter = make_adapter("shopify", {})

    def sync_fn(item: InventoryItem):
        return adapter.update_inventory(item)

    items = [make_item(f"SKU-{i}", i * 5) for i in range(10)]
    events = batch_sync_items(items, sync_fn, max_workers=4)
    assert len(events) == 10
    assert all(e.status == SyncStatus.SUCCESS for e in events)


def test_cancellation_token():
    token = CancellationToken()
    assert not token.is_cancelled
    token.cancel()
    assert token.is_cancelled


# ─── Diff ─────────────────────────────────────────────────────────────────────

def test_diff_detects_changes():
    a = [make_item("SKU-A", 10), make_item("SKU-B", 5)]
    b = [make_item("SKU-A", 8), make_item("SKU-C", 3)]
    diff = diff_inventory(a, b)
    assert "SKU-C" in diff.added
    assert "SKU-B" in diff.removed
    assert "SKU-A" in diff.modified
    assert diff.modified["SKU-A"]["quantity"]["old"] == 10
    assert diff.modified["SKU-A"]["quantity"]["new"] == 8


def test_diff_summary():
    a = [make_item("SKU-A", 10)]
    b = [make_item("SKU-A", 20), make_item("SKU-B", 5)]
    diff = diff_inventory(a, b)
    s = diff.summary()
    assert s["added"] == 1
    assert s["modified"] == 1


def test_diff_to_json():
    a = [make_item("SKU-1", 5)]
    b = [make_item("SKU-1", 10)]
    diff = diff_inventory(a, b)
    j = diff.to_json()
    import json
    data = json.loads(j)
    assert "modified" in data


# ─── Streaming ────────────────────────────────────────────────────────────────

def test_stream_inventory():
    items = [make_item(f"SKU-{i}", i) for i in range(5)]
    result = list(stream_inventory(items))
    assert len(result) == 5


def test_inventory_to_ndjson():
    items = [make_item("SKU-1", 10), make_item("SKU-2", 5)]
    lines = list(inventory_to_ndjson(items))
    assert len(lines) == 2
    assert lines[0].endswith("\n")


def test_inventory_chunks():
    items = [make_item(f"SKU-{i}", i) for i in range(25)]
    chunks = list(inventory_chunks(items, size=10))
    assert len(chunks) == 3
    assert len(chunks[0]) == 10
    assert len(chunks[2]) == 5


# ─── Audit & PII ──────────────────────────────────────────────────────────────

def test_audit_log():
    log = AuditLog()
    log.record("update", "SKU-001", "shopify", "qty changed")
    entries = log.export()
    assert len(entries) == 1
    assert entries[0]["sku"] == "SKU-001"


def test_pii_scrubber():
    cleaned = PIIScrubber.scrub("Contact: user@example.com or 555-123-4567")
    assert "[EMAIL]" in cleaned
    assert "[PHONE]" in cleaned
    assert "user@example.com" not in cleaned
