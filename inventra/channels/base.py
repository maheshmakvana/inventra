"""Base channel adapter interface for inventra."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from inventra.models import InventoryItem, SyncEvent, SyncStatus

logger = logging.getLogger(__name__)


class BaseChannelAdapter(ABC):
    """Abstract base class for all channel adapters."""

    channel_name: str = "base"

    def __init__(self, config: Dict) -> None:
        self.config = config
        self._connected: bool = False

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the channel."""

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the channel."""

    @abstractmethod
    def get_inventory(self, skus: Optional[List[str]] = None) -> List[InventoryItem]:
        """Fetch inventory items from the channel."""

    @abstractmethod
    def update_inventory(self, item: InventoryItem) -> SyncEvent:
        """Push inventory update to the channel."""

    def batch_update(self, items: List[InventoryItem]) -> List[SyncEvent]:
        """Update multiple items; override for channel-native batch API."""
        events: List[SyncEvent] = []
        for item in items:
            try:
                event = self.update_inventory(item)
            except Exception as exc:
                logger.error("Channel %s batch update failed for SKU %s: %s", self.channel_name, item.sku, exc)
                event = SyncEvent(
                    sku=item.sku,
                    channel=self.channel_name,
                    status=SyncStatus.FAILED,
                    error=str(exc),
                )
            events.append(event)
        return events

    @property
    def is_connected(self) -> bool:
        return self._connected

    def __enter__(self) -> "BaseChannelAdapter":
        self.connect()
        return self

    def __exit__(self, *args: object) -> None:
        self.disconnect()
