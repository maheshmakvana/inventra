"""Exceptions for inventra."""


class InventraError(Exception):
    """Base exception for inventra."""


class SyncError(InventraError):
    """Raised when a sync operation fails."""


class ConflictError(InventraError):
    """Raised when a conflict cannot be auto-resolved."""


class ChannelError(InventraError):
    """Raised when a channel adapter encounters an error."""


class ValidationError(InventraError):
    """Raised on invalid inventory data."""
