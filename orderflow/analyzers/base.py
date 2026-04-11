"""Base analyzer contract for orderflow engine."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:
    from orderflow.core.trade import Trade


class BaseAnalyzer(ABC):
    """Analyzer interface consumed by OrderFlowEngine."""

    @abstractmethod
    def on_trade(self, trade: Trade) -> None:
        """Process a single normalized trade."""

    def on_batch(self, trades: Iterable[Trade]) -> None:
        """Process a batch of trades.

        Default behavior reuses single-trade path to preserve correctness.
        """
        for trade in trades:
            self.on_trade(trade)

    @abstractmethod
    def snapshot(self) -> dict[str, Any]:
        """Return current analyzer state for downstream consumption."""

    @abstractmethod
    def reset(self) -> None:
        """Reset analyzer state."""
