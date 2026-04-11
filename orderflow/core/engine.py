"""Order flow event bus engine."""

from __future__ import annotations

from collections import OrderedDict
from typing import Iterable

from orderflow.analyzers.base import BaseAnalyzer
from orderflow.core.trade import Trade


class OrderFlowEngine:
    """Route normalized trade events to registered analyzers."""

    def __init__(self) -> None:
        self._analyzers: "OrderedDict[str, BaseAnalyzer]" = OrderedDict()

    def add_analyzer(self, name: str, analyzer: BaseAnalyzer) -> None:
        if not name:
            raise ValueError("Analyzer name must be non-empty")
        if name in self._analyzers:
            raise ValueError(f"Analyzer '{name}' already registered")
        self._analyzers[name] = analyzer

    @property
    def analyzers(self) -> tuple[tuple[str, BaseAnalyzer], ...]:
        return tuple(self._analyzers.items())

    def on_trade(self, trade: Trade) -> None:
        for analyzer in self._analyzers.values():
            analyzer.on_trade(trade)

    def on_batch(self, trades: Iterable[Trade]) -> None:
        cached = tuple(trades)
        for analyzer in self._analyzers.values():
            analyzer.on_batch(cached)

    def snapshot(self) -> dict[str, dict]:
        return {name: analyzer.snapshot() for name, analyzer in self._analyzers.items()}

    def reset(self) -> None:
        for analyzer in self._analyzers.values():
            analyzer.reset()
