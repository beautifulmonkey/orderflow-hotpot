"""Trade data model and normalization helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Union

SideLike = Union[int, str]

BUY = "buy"
SELL = "sell"

def normalize_side(side: SideLike) -> str:
    """Normalize side input to canonical string ("buy" / "sell")."""
    if isinstance(side, int):
        if side == 1:
            return BUY
        if side == -1:
            return SELL
        raise ValueError(f"Unsupported integer side: {side}")

    normalized = side.strip().lower()
    if normalized in {"buy", "buyer", "b", "ask", "1", "+"}:
        return BUY
    if normalized in {"sell", "seller", "s", "bid", "-1", "-"}:
        return SELL
    raise ValueError(f"Unsupported side value: {side}")


@dataclass(frozen=True)
class Trade:
    """Lightweight, normalized trade event consumed by analyzers."""

    ts_ns: int
    side: str
    price: float
    size: float

    def __post_init__(self) -> None:
        if self.side not in (BUY, SELL):
            raise ValueError("side must be 'buy' or 'sell'")
        if self.ts_ns < 0:
            raise ValueError("ts_ns must be >= 0")
        if self.size < 0:
            raise ValueError("size must be >= 0")

    @classmethod
    def from_raw(
        cls,
        *,
        ts_ns: int,
        side: SideLike,
        price: float,
        size: float,
    ) -> "Trade":
        """Build a normalized trade from raw values."""
        return cls(
            ts_ns=int(ts_ns),
            side=normalize_side(side),
            price=float(price),
            size=float(size),
        )
