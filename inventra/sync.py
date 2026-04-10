"""Core inventory sync engine for inventra."""
from __future__ import annotations

import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional

from inventra.channels.base import BaseChannelAdapter
from inventra.exceptions import InventraError, ConflictError, SyncError
from inventra.models import (
    ConflictRecord,
    ConflictResolution,
    InventoryItem,
    SyncEvent,
    SyncReport,
    SyncStatus,
)

logger = logging.getLogger(__name__)


class ConflictResolver:
    """Resolve inventory conflicts across channels."""

    def __init__(self, strategy: ConflictResolution = ConflictResolution.LOWEST_STOCK) -> None:
        self.strategy = strategy
        self._channel_priority: List[str] = []

    def set_channel_priority(self, channels: List[str]) -> None:
        self._channel_priority = channels

    def resolve(self, sku: str, quantities: Dict[str, int]) -> ConflictRecord:
        """Resolve conflict and return the winning quantity."""
        resolved: int

        if self.strategy == ConflictResolution.HIGHEST_STOCK:
            resolved = max(quantities.values())
        elif self.strategy == ConflictResolution.LOWEST_STOCK:
            resolved = min(quantities.values())
        elif self.strategy == ConflictResolution.CHANNEL_PRIORITY:
            for ch in self._channel_priority:
                if ch in quantities:
                    resolved = quantities[ch]
                    break
            else:
                resolved = min(quantities.values())
        elif self.strategy == ConflictResolution.MANUAL:
            raise ConflictError(f"Manual resolution required for SKU {sku}: {quantities}")
        else:
            resolved = min(quantities.values())

        record = ConflictRecord(
            sku=sku,
            channels=list(quantities.keys()),
            quantities=quantities,
            resolved_quantity=resolved,
            resolution=self.strategy,
        )
        logger.info("Conflict resolved for SKU %s → %d (strategy=%s)", sku, resolved, self.strategy.value)
        return record


class InventorySync:
    """Central sync engine for multi-channel inventory management."""

    def __init__(
        self,
        channels: Optional[List[BaseChannelAdapter]] = None,
        conflict_resolution: ConflictResolution = ConflictResolution.LOWEST_STOCK,
        max_workers: int = 4,
    ) -> None:
        self._channels: Dict[str, BaseChannelAdapter] = {}
        self._lock = threading.Lock()
        self._resolver = ConflictResolver(conflict_resolution)
        self._max_workers = max_workers

        if channels:
            for ch in channels:
                self.add_channel(ch)

    def add_channel(self, adapter: BaseChannelAdapter) -> None:
        """Register a channel adapter."""
        with self._lock:
            self._channels[adapter.channel_name] = adapter
        logger.info("Channel registered: %s", adapter.channel_name)

    def remove_channel(self, channel_name: str) -> None:
        with self._lock:
            self._channels.pop(channel_name, None)

    def set_channel_priority(self, order: List[str]) -> None:
        self._resolver.set_channel_priority(order)

    def _fetch_all_inventory(
        self, skus: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, InventoryItem]]:
        """Fetch inventory from all channels. Returns {sku: {channel: item}}."""
        result: Dict[str, Dict[str, InventoryItem]] = {}

        def fetch_from(adapter: BaseChannelAdapter) -> List[InventoryItem]:
            if not adapter.is_connected:
                adapter.connect()
            return adapter.get_inventory(skus)

        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futures = {pool.submit(fetch_from, adapter): name for name, adapter in self._channels.items()}
            for future in as_completed(futures):
                ch_name = futures[future]
                try:
                    items = future.result()
                    for item in items:
                        if item.sku not in result:
                            result[item.sku] = {}
                        result[item.sku][ch_name] = item
                except Exception as exc:
                    logger.error("Failed to fetch inventory from channel %s: %s", ch_name, exc)

        return result

    def _detect_conflicts(
        self, inventory: Dict[str, Dict[str, InventoryItem]]
    ) -> Dict[str, Dict[str, int]]:
        """Identify SKUs with differing quantities across channels."""
        conflicts: Dict[str, Dict[str, int]] = {}
        for sku, channel_map in inventory.items():
            qtys = {ch: item.quantity for ch, item in channel_map.items()}
            if len(set(qtys.values())) > 1:
                conflicts[sku] = qtys
        return conflicts

    def sync(
        self,
        skus: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> SyncReport:
        """Perform a full sync across all channels."""
        report = SyncReport(started_at=datetime.utcnow())

        inventory = self._fetch_all_inventory(skus)
        conflicts = self._detect_conflicts(inventory)

        # Resolve conflicts
        resolved_quantities: Dict[str, int] = {}
        for sku, qtys in conflicts.items():
            try:
                record = self._resolver.resolve(sku, qtys)
                report.conflict_records.append(record)
                resolved_quantities[sku] = record.resolved_quantity  # type: ignore[assignment]
                report.conflicts += 1
            except ConflictError as exc:
                logger.warning("Unresolved conflict for SKU %s: %s", sku, exc)
                report.events.append(SyncEvent(sku=sku, channel="all", status=SyncStatus.CONFLICT, error=str(exc)))

        if dry_run:
            logger.info("Dry run — no updates pushed")
            report.finished_at = datetime.utcnow()
            report.duration_seconds = (report.finished_at - report.started_at).total_seconds()
            return report

        # Push resolved quantities to all channels
        for sku, target_qty in resolved_quantities.items():
            for ch_name, adapter in self._channels.items():
                item = inventory.get(sku, {}).get(ch_name)
                if item is None:
                    continue
                updated = item.model_copy(update={"quantity": target_qty, "updated_at": datetime.utcnow()})
                try:
                    event = adapter.update_inventory(updated)
                    report.events.append(event)
                    report.success += 1
                except Exception as exc:
                    logger.error("Update failed on channel %s for SKU %s: %s", ch_name, sku, exc)
                    report.events.append(SyncEvent(sku=sku, channel=ch_name, status=SyncStatus.FAILED, error=str(exc)))
                    report.failed += 1

        report.total = len(inventory)
        report.finished_at = datetime.utcnow()
        report.duration_seconds = (report.finished_at - report.started_at).total_seconds()
        logger.info("Sync complete: %s", report.summary())
        return report

    async def async_sync(
        self,
        skus: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> SyncReport:
        """Async variant of sync — runs blocking sync in a thread pool."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: self.sync(skus=skus, dry_run=dry_run))

    def get_stock(self, sku: str) -> Dict[str, int]:
        """Get current stock for a SKU across all channels."""
        result: Dict[str, int] = {}
        for ch_name, adapter in self._channels.items():
            if not adapter.is_connected:
                adapter.connect()
            items = adapter.get_inventory([sku])
            if items:
                result[ch_name] = items[0].quantity
        return result
