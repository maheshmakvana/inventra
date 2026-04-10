"""Core inventory sync engine for inventra."""
from __future__ import annotations

import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Set

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
    """
    Resolve inventory quantity conflicts across channels.

    Strategies:
    - LOWEST_STOCK: Use the minimum quantity (safest — prevents oversell).
    - HIGHEST_STOCK: Use the maximum quantity (maximises availability).
    - LATEST_UPDATE: Use the quantity from the most-recently updated channel.
    - CHANNEL_PRIORITY: Use quantity from the highest-priority channel.
    - MANUAL: Raise ConflictError so the caller can handle it.
    """

    def __init__(self, strategy: ConflictResolution = ConflictResolution.LOWEST_STOCK) -> None:
        self.strategy = strategy
        self._channel_priority: List[str] = []

    def set_channel_priority(self, channels: List[str]) -> None:
        """Define the ordered list of channels for CHANNEL_PRIORITY strategy."""
        self._channel_priority = channels

    def resolve(
        self,
        sku: str,
        quantities: Dict[str, int],
        updated_ats: Optional[Dict[str, datetime]] = None,
    ) -> ConflictRecord:
        """Resolve a quantity conflict and return the winning ConflictRecord."""
        resolved: int

        if self.strategy == ConflictResolution.HIGHEST_STOCK:
            resolved = max(quantities.values())

        elif self.strategy == ConflictResolution.LOWEST_STOCK:
            resolved = min(quantities.values())

        elif self.strategy == ConflictResolution.LATEST_UPDATE:
            if updated_ats:
                latest_channel = max(updated_ats, key=lambda ch: updated_ats[ch])
                resolved = quantities.get(latest_channel, min(quantities.values()))
            else:
                resolved = min(quantities.values())

        elif self.strategy == ConflictResolution.CHANNEL_PRIORITY:
            resolved = min(quantities.values())  # fallback
            for ch in self._channel_priority:
                if ch in quantities:
                    resolved = quantities[ch]
                    break

        elif self.strategy == ConflictResolution.MANUAL:
            raise ConflictError(
                f"Manual resolution required for SKU '{sku}': {quantities}"
            )

        else:
            resolved = min(quantities.values())

        record = ConflictRecord(
            sku=sku,
            channels=list(quantities.keys()),
            quantities=quantities,
            resolved_quantity=resolved,
            resolution=self.strategy,
        )
        logger.info(
            "Conflict resolved for SKU '%s' → %d (strategy=%s)", sku, resolved, self.strategy.value
        )
        return record


class InventorySync:
    """
    Central sync engine for multi-channel inventory management.

    Key improvements over naive sync:
    - Delta sync: only pushes items whose quantity actually changed since the
      last resolved state, reducing unnecessary API calls to each channel.
    - Per-update retry: failed channel updates are retried up to `update_retries`
      times before being recorded as failures.
    - Updated_at awareness: the LATEST_UPDATE conflict strategy uses the
      `updated_at` field from each channel's item to pick the freshest source.
    - Skipped tracking: items with no conflicts and no changes are counted as
      skipped rather than success to give accurate reporting.
    """

    def __init__(
        self,
        channels: Optional[List[BaseChannelAdapter]] = None,
        conflict_resolution: ConflictResolution = ConflictResolution.LOWEST_STOCK,
        max_workers: int = 4,
        update_retries: int = 2,
    ) -> None:
        self._channels: Dict[str, BaseChannelAdapter] = {}
        self._lock = threading.Lock()
        self._resolver = ConflictResolver(conflict_resolution)
        self._max_workers = max_workers
        self._update_retries = update_retries
        # Last synced quantities per SKU — used for delta detection
        self._last_resolved: Dict[str, int] = {}

        if channels:
            for ch in channels:
                self.add_channel(ch)

    def add_channel(self, adapter: BaseChannelAdapter) -> None:
        """Register a channel adapter."""
        with self._lock:
            self._channels[adapter.channel_name] = adapter
        logger.info("Channel registered: %s", adapter.channel_name)

    def remove_channel(self, channel_name: str) -> None:
        """Unregister a channel adapter."""
        with self._lock:
            self._channels.pop(channel_name, None)

    def set_channel_priority(self, order: List[str]) -> None:
        """Set priority order for CHANNEL_PRIORITY conflict resolution."""
        self._resolver.set_channel_priority(order)

    def _fetch_all_inventory(
        self, skus: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, InventoryItem]]:
        """
        Fetch inventory from all channels concurrently.
        Returns {sku: {channel_name: InventoryItem}}.
        """
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
                    logger.error("Failed to fetch inventory from channel '%s': %s", ch_name, exc)

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

    def _is_delta(self, sku: str, target_qty: int) -> bool:
        """Return True if the target quantity differs from the last resolved value."""
        last = self._last_resolved.get(sku)
        return last is None or last != target_qty

    def _push_update(
        self,
        adapter: BaseChannelAdapter,
        updated_item: InventoryItem,
        report: SyncReport,
    ) -> None:
        """Push a single item update to a channel with retry logic."""
        last_exc: Optional[Exception] = None
        for attempt in range(self._update_retries + 1):
            try:
                event = adapter.update_inventory(updated_item)
                report.events.append(event)
                report.success += 1
                return
            except Exception as exc:
                last_exc = exc
                if attempt < self._update_retries:
                    logger.warning(
                        "Update attempt %d/%d failed for SKU '%s' on channel '%s': %s",
                        attempt + 1, self._update_retries + 1,
                        updated_item.sku, adapter.channel_name, exc,
                    )

        logger.error(
            "All %d update attempts failed for SKU '%s' on channel '%s': %s",
            self._update_retries + 1, updated_item.sku, adapter.channel_name, last_exc,
        )
        report.events.append(SyncEvent(
            sku=updated_item.sku,
            channel=adapter.channel_name,
            status=SyncStatus.FAILED,
            error=str(last_exc),
        ))
        report.failed += 1

    def sync(
        self,
        skus: Optional[List[str]] = None,
        dry_run: bool = False,
        delta_only: bool = True,
    ) -> SyncReport:
        """
        Perform a full sync across all registered channels.

        Args:
            skus: Optional list of SKUs to sync. None = all SKUs.
            dry_run: If True, resolves conflicts but does not push any updates.
            delta_only: If True (default), only pushes updates where the
                        resolved quantity differs from the previously synced value.
                        Set to False to force-push all resolved quantities.
        """
        report = SyncReport(started_at=datetime.utcnow())

        inventory = self._fetch_all_inventory(skus)
        if not inventory:
            logger.warning("No inventory fetched — no channels connected or no matching SKUs.")
            report.finished_at = datetime.utcnow()
            report.duration_seconds = 0.0
            return report

        conflicts = self._detect_conflicts(inventory)

        # Resolve conflicts
        resolved_quantities: Dict[str, int] = {}
        for sku, qtys in conflicts.items():
            try:
                # Gather updated_at for LATEST_UPDATE strategy
                updated_ats = {
                    ch: item.updated_at
                    for ch, item in inventory[sku].items()
                    if item.updated_at is not None
                }
                record = self._resolver.resolve(sku, qtys, updated_ats=updated_ats)
                report.conflict_records.append(record)
                resolved_quantities[sku] = record.resolved_quantity  # type: ignore[assignment]
                report.conflicts += 1
            except ConflictError as exc:
                logger.warning("Unresolved conflict for SKU '%s': %s", sku, exc)
                report.events.append(SyncEvent(
                    sku=sku, channel="all", status=SyncStatus.CONFLICT, error=str(exc)
                ))

        # For SKUs with no conflict, use the single-channel value
        for sku, channel_map in inventory.items():
            if sku not in resolved_quantities and len(channel_map) == 1:
                resolved_quantities[sku] = next(iter(channel_map.values())).quantity

        report.total = len(inventory)

        if dry_run:
            logger.info("Dry run — conflicts=%d, would update %d SKUs", report.conflicts, len(resolved_quantities))
            report.finished_at = datetime.utcnow()
            report.duration_seconds = (report.finished_at - report.started_at).total_seconds()
            return report

        # Push resolved quantities to all channels
        pushed_skus: Set[str] = set()
        for sku, target_qty in resolved_quantities.items():
            # Delta check: skip if quantity is unchanged
            if delta_only and not self._is_delta(sku, target_qty):
                report.skipped += 1
                logger.debug("Delta sync: SKU '%s' unchanged at %d — skipped", sku, target_qty)
                continue

            pushed_skus.add(sku)
            for ch_name, adapter in self._channels.items():
                item = inventory.get(sku, {}).get(ch_name)
                if item is None:
                    continue
                old_qty = item.quantity
                updated = item.model_copy(update={"quantity": target_qty, "updated_at": datetime.utcnow()})
                updated.metadata["old_quantity"] = old_qty
                self._push_update(adapter, updated, report)

            # Update delta tracking
            self._last_resolved[sku] = target_qty

        report.finished_at = datetime.utcnow()
        report.duration_seconds = (report.finished_at - report.started_at).total_seconds()
        logger.info(
            "Sync complete: total=%d conflicts=%d success=%d failed=%d skipped=%d pushed_skus=%d duration=%.2fs",
            report.total, report.conflicts, report.success, report.failed,
            report.skipped, len(pushed_skus), report.duration_seconds,
        )
        return report

    async def async_sync(
        self,
        skus: Optional[List[str]] = None,
        dry_run: bool = False,
        delta_only: bool = True,
    ) -> SyncReport:
        """Async variant of sync — runs the blocking sync in a thread executor."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, lambda: self.sync(skus=skus, dry_run=dry_run, delta_only=delta_only)
        )

    def get_stock(self, sku: str) -> Dict[str, int]:
        """Get current stock for a SKU across all channels."""
        result: Dict[str, int] = {}
        with self._lock:
            channels = dict(self._channels)
        for ch_name, adapter in channels.items():
            if not adapter.is_connected:
                adapter.connect()
            items = adapter.get_inventory([sku])
            if items:
                result[ch_name] = items[0].quantity
        return result

    def reset_delta_tracking(self) -> None:
        """Clear the delta tracking cache (forces a full push on next sync)."""
        self._last_resolved.clear()
        logger.info("Delta tracking cache cleared")

    def channel_status(self) -> Dict[str, bool]:
        """Return connection status for all registered channels."""
        with self._lock:
            return {name: adapter.is_connected for name, adapter in self._channels.items()}
