"""inventra — Multi-channel inventory sync for small and mid-market eCommerce."""
from inventra.models import (
    ChannelType,
    ConflictRecord,
    ConflictResolution,
    InventoryItem,
    SyncEvent,
    SyncReport,
    SyncStatus,
)
from inventra.sync import ConflictResolver, InventorySync
from inventra.channels.base import BaseChannelAdapter
from inventra.channels.mock import MockChannelAdapter
from inventra.exceptions import (
    ChannelError,
    ConflictError,
    InventraError,
    SyncError,
    ValidationError,
)
from inventra.advanced import (
    AuditLog,
    CancellationToken,
    InventoryCache,
    InventoryDiff,
    InventoryValidator,
    InventoryRule,
    PIIScrubber,
    RateLimiter,
    SyncPipeline,
    SyncProfiler,
    abatch_sync_items,
    batch_sync_items,
    diff_inventory,
    inventory_chunks,
    inventory_to_ndjson,
    stream_inventory,
)

__version__ = "1.0.0"
__all__ = [
    # Core
    "InventorySync",
    "ConflictResolver",
    "InventoryItem",
    "SyncEvent",
    "SyncReport",
    "SyncStatus",
    "ChannelType",
    "ConflictRecord",
    "ConflictResolution",
    # Channels
    "BaseChannelAdapter",
    "MockChannelAdapter",
    # Exceptions
    "InventraError",
    "SyncError",
    "ConflictError",
    "ChannelError",
    "ValidationError",
    # Advanced
    "InventoryCache",
    "SyncPipeline",
    "InventoryValidator",
    "InventoryRule",
    "RateLimiter",
    "CancellationToken",
    "batch_sync_items",
    "abatch_sync_items",
    "SyncProfiler",
    "stream_inventory",
    "inventory_to_ndjson",
    "inventory_chunks",
    "InventoryDiff",
    "diff_inventory",
    "AuditLog",
    "PIIScrubber",
]
