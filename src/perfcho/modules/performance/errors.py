"""Define expected performance calculation failures."""


class PerformanceCalculationError(Exception):
    """Describe a calculator failure with explicit retry semantics."""

    def __init__(self, message: str, *, retryable: bool) -> None:
        """Store a bounded operational message and whether retry can recover."""
        super().__init__(message)
        self.retryable = retryable
