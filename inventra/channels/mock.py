"""Mock channel adapter for testing and demos."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional

from inventra.channels.base import BaseChannelAdapter
from inventra.models import InventoryItem, SyncEvent, SyncStatus

logger = logging.getLogger(__name__)


class MockChannelAdapter(BaseChannelAdapter):
    """In-memory mock channel for testing."""

    channel_name: str = "mock"

    def __init__(self, config: Dict, initial_inventory: Optional[Dict[str, int]] = None) -> None:
        super().__init__(config)
        self._inventory: Dict[str, InventoryItem] = {}
        if initial_inventory:
            for sku, qty in initial_inventory.items():
                self._inventory[sku] = InventoryItem(
                    sku=sku,
                    channel=self.channel_name,
                    quantity=qty,
                )

    def connect(self) -> None:
        self._connected = True
        logger.debug("MockChannelAdapter connected")

    def disconnect(self) -> None:
        self._connected = False
        logger.debug("MockChannelAdapter disconnected")

    def get_inventory(self, skus: Optional[List[str]] = None) -> List[InventoryItem]:
        if skus:
            return [self._inventory[s] for s in skus if s in self._inventory]
        return list(self._inventory.values())

    def update_inventory(self, item: InventoryItem) -> SyncEvent:
        old = self._inventory.get(item.sku)
        old_qty = old.quantity if old else None
        self._inventory[item.sku] = item.model_copy(
            update={"channel": self.channel_name, "updated_at": datetime.utcnow()}
        )
        return SyncEvent(
            sku=item.sku,
            channel=self.channel_name,
            status=SyncStatus.SUCCESS,
            old_quantity=old_qty,
            new_quantity=item.quantity,
        )
