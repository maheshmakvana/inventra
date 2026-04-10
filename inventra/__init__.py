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
    ABCXYZResult,
    AuditLog,
    CancellationToken,
    ChannelHealthMonitor,
    ChannelHealthSnapshot,
    DemandForecastEngine,
    InventoryABCAnalyzer,
    InventoryCache,
    InventoryDiff,
    InventoryValidator,
    InventoryRule,
    OversellRisk,
    OversellRiskAnalyzer,
    PIIScrubber,
    RateLimiter,
    ReorderPointCalculator,
    ReorderRecommendation,
    SyncPipeline,
    SyncProfiler,
    SyncSpanEmitter,
    abatch_sync_items,
    batch_sync_items,
    diff_inventory,
    inventory_chunks,
    inventory_to_ndjson,
    stream_inventory,
)

__version__ = "1.2.0"
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
    # Advanced — base
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
    # Advanced — expert v1.1.0
    "DemandForecastEngine",
    "ChannelHealthMonitor",
    "ChannelHealthSnapshot",
    "OversellRiskAnalyzer",
    "OversellRisk",
    "SyncSpanEmitter",
    # Advanced — expert v1.2.0
    "ReorderPointCalculator",
    "ReorderRecommendation",
    "InventoryABCAnalyzer",
    "ABCXYZResult",
]
