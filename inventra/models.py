"""Data models for inventra — multi-channel inventory sync."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


class ChannelType(str, Enum):
    SHOPIFY = "shopify"
    AMAZON = "amazon"
    EBAY = "ebay"
    WOOCOMMERCE = "woocommerce"
    ETSY = "etsy"
    CUSTOM = "custom"


class SyncStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    CONFLICT = "conflict"


class ConflictResolution(str, Enum):
    HIGHEST_STOCK = "highest_stock"
    LOWEST_STOCK = "lowest_stock"
    LATEST_UPDATE = "latest_update"
    CHANNEL_PRIORITY = "channel_priority"
    MANUAL = "manual"


class InventoryItem(BaseModel):
    """Represents a single product/SKU inventory record."""

    sku: str
    channel: str
    quantity: int = Field(ge=0)
    reserved: int = Field(default=0, ge=0)
    price: Optional[float] = None
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("sku")
    @classmethod
    def sku_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("sku must not be empty")
        return v.strip().upper()

    @property
    def available(self) -> int:
        return max(0, self.quantity - self.reserved)


class SyncEvent(BaseModel):
    """Records a single sync operation outcome."""

    sku: str
    channel: str
    status: SyncStatus
    old_quantity: Optional[int] = None
    new_quantity: Optional[int] = None
    error: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ConflictRecord(BaseModel):
    """Conflict detected during sync between channels."""

    sku: str
    channels: List[str]
    quantities: Dict[str, int]
    resolved_quantity: Optional[int] = None
    resolution: Optional[ConflictResolution] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class SyncReport(BaseModel):
    """Summary of a full sync run."""

    total: int = 0
    success: int = 0
    failed: int = 0
    conflicts: int = 0
    skipped: int = 0
    events: List[SyncEvent] = Field(default_factory=list)
    conflict_records: List[ConflictRecord] = Field(default_factory=list)
    started_at: datetime = Field(default_factory=datetime.utcnow)
    finished_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None

    def summary(self) -> Dict[str, Any]:
        return {
            "total": self.total,
            "success": self.success,
            "failed": self.failed,
            "conflicts": self.conflicts,
            "skipped": self.skipped,
            "duration_seconds": self.duration_seconds,
        }
