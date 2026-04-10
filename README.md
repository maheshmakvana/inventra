# inventra

**Multi-channel inventory sync for small and mid-market eCommerce** — real-time conflict resolution, async sync engine, drift detection, and audit log.

Sync inventory across Shopify, Amazon, eBay, WooCommerce, and custom channels with automatic conflict resolution. No $500/mo SaaS required.

[![PyPI version](https://badge.fury.io/py/inventra.svg)](https://pypi.org/project/inventra/)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)

## The Problem

$1 trillion/year is lost to stockouts. 34% of small merchants report real-time sync as their top operational headache. Enterprise tools start at $500/mo — nothing affordable exists for independent sellers.

## Installation

```bash
pip install inventra
```

## Quick Start

```python
from inventra import InventorySync, InventoryItem, ConflictResolution
from inventra.channels.mock import MockChannelAdapter

# Set up two channels with different stock levels
shopify = MockChannelAdapter(config={}, initial_inventory={"SKU-001": 50, "SKU-002": 10})
shopify.channel_name = "shopify"
shopify.connect()

amazon = MockChannelAdapter(config={}, initial_inventory={"SKU-001": 45, "SKU-002": 10})
amazon.channel_name = "amazon"
amazon.connect()

# Sync with lowest-stock wins (prevents overselling)
sync = InventorySync(
    channels=[shopify, amazon],
    conflict_resolution=ConflictResolution.LOWEST_STOCK,
)

report = sync.sync()
print(report.summary())
# {'total': 2, 'success': 2, 'failed': 0, 'conflicts': 1, ...}
```

## Async Sync

```python
import asyncio
from inventra import InventorySync

sync = InventorySync(channels=[shopify, amazon])

async def main():
    report = await sync.async_sync()
    print(report.summary())

asyncio.run(main())
```

## Conflict Resolution Strategies

| Strategy | Behaviour |
|---|---|
| `LOWEST_STOCK` | Sync to the lowest quantity — prevents overselling (default) |
| `HIGHEST_STOCK` | Sync to the highest quantity |
| `LATEST_UPDATE` | Use the most recently updated channel |
| `CHANNEL_PRIORITY` | Respect a user-defined channel priority order |
| `MANUAL` | Raise `ConflictError` — let your code decide |

```python
sync = InventorySync(conflict_resolution=ConflictResolution.CHANNEL_PRIORITY)
sync.set_channel_priority(["shopify", "amazon", "ebay"])
```

## Building a Custom Channel Adapter

```python
from inventra.channels.base import BaseChannelAdapter
from inventra.models import InventoryItem, SyncEvent, SyncStatus

class MyShopifyAdapter(BaseChannelAdapter):
    channel_name = "shopify"

    def connect(self):
        # authenticate with Shopify API
        self._connected = True

    def disconnect(self):
        self._connected = False

    def get_inventory(self, skus=None):
        # fetch from Shopify Inventory API
        ...

    def update_inventory(self, item):
        # push update to Shopify
        return SyncEvent(sku=item.sku, channel=self.channel_name, status=SyncStatus.SUCCESS, new_quantity=item.quantity)
```

## Advanced Features

### Pipeline

```python
from inventra import SyncPipeline

pipeline = (
    SyncPipeline()
    .filter(lambda item: item.quantity > 0, name="exclude_oos")
    .map(lambda items: [i.model_copy(update={"reserved": 2}) for i in items], name="reserve_safety_stock")
    .with_retry(count=2)
)

result = pipeline.run(items)
print(pipeline.audit_log())
```

### Caching

```python
from inventra import InventoryCache

cache = InventoryCache(max_size=500, ttl_seconds=120)

@cache.memoize
def expensive_fetch(channel_name):
    ...

print(cache.stats())
# {'hits': 10, 'misses': 2, 'hit_rate': 0.833, 'size': 1}
```

### Diff & Change Detection

```python
from inventra import diff_inventory

diff = diff_inventory(snapshot_before, snapshot_after)
print(diff.summary())  # {'added': 2, 'removed': 0, 'modified': 5}
print(diff.to_json())
```

### Async Batch

```python
from inventra import abatch_sync_items, CancellationToken

token = CancellationToken()
events = await abatch_sync_items(items, adapter.update_inventory, max_concurrency=8, token=token)
```

### Rate Limiter

```python
from inventra import RateLimiter

limiter = RateLimiter(rate=10, capacity=10)  # 10 ops/sec
if limiter.acquire():
    adapter.update_inventory(item)
```

### Streaming

```python
from inventra import inventory_to_ndjson

for line in inventory_to_ndjson(items):
    socket.send(line)
```

### Audit Log + PII Scrubbing

```python
from inventra import AuditLog, PIIScrubber

log = AuditLog()
log.record("update", sku="SKU-001", channel="shopify", detail="qty 50→45")

metadata = {"note": "Customer email: user@example.com"}
clean = PIIScrubber.scrub_metadata(metadata)
# {'note': 'Customer email: [EMAIL]'}
```

## Supported Channels

`inventra` ships with a `MockChannelAdapter` for testing. Community and official adapters (Shopify, Amazon, eBay, WooCommerce, Etsy) can be built by subclassing `BaseChannelAdapter`.

## Changelog

### v1.2.2 (2026-04-10)
- Added Contributing and Author sections to README

### v1.2.1 (2026-04-10)
- Added Changelog section to README for release traceability

### v1.2.0
- Added `ReorderPointCalculator` — demand-driven reorder point and safety stock calculations
- Added `InventoryABCAnalyzer` — ABC/XYZ segmentation for inventory prioritisation
- Expanded SEO keywords for PyPI discoverability

### v1.0.1
- Advanced features: pipeline, caching, validation, diff/trend, streaming, audit log

### v1.0.0
- Initial release: multi-channel inventory sync, conflict resolution, drift detection, audit log

## License

MIT

## Contributing

Contributions are welcome! Here's how to get started:

1. Fork the repository on [GitHub](https://github.com/maheshmakvana/inventra)
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Make your changes and add tests
4. Run the test suite: `pytest tests/ -v`
5. Submit a pull request

Please open an issue first for major changes to discuss the approach.

## Author

**Mahesh Makvana** — [GitHub](https://github.com/maheshmakvana) · [PyPI](https://pypi.org/user/maheshmakvana/)

MIT License
