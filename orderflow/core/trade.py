"""Trade data model and normalization helpers."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Union

SideLike = Union[int, str]

BUY = 1
SELL = -1


def normalize_side(side: SideLike) -> int:
    """Normalize side input to encoded integer (buy=1, sell=-1)."""
    if isinstance(side, int):
        if side in (BUY, SELL):
            return side
        raise ValueError(f"Unsupported integer side: {side}")

    normalized = side.strip().lower()
    if normalized in {"buy", "b", "ask", "1", "+"}:
        return BUY
    if normalized in {"sell", "s", "bid", "-1", "-"}:
        return SELL
    raise ValueError(f"Unsupported side value: {side}")


def _quantize_price_to_tick(price: float, tick_size: float) -> int:
    if tick_size <= 0:
        raise ValueError("tick_size must be > 0")
    ratio = Decimal(str(price)) / Decimal(str(tick_size))
    # Use decimal rounding to avoid binary floating-point surprises.
    return int(ratio.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


@dataclass(frozen=True)
class Trade:
    """Lightweight, normalized trade event consumed by analyzers."""

    ts_ns: int
    side: int
    price_tick: int
    size: float

    def __post_init__(self) -> None:
        if self.side not in (BUY, SELL):
            raise ValueError("side must be 1 (buy) or -1 (sell)")
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
        tick_size: float,
    ) -> "Trade":
        """Build a normalized trade from raw values."""
        return cls(
            ts_ns=int(ts_ns),
            side=normalize_side(side),
            price_tick=_quantize_price_to_tick(price=price, tick_size=tick_size),
            size=float(size),
        )
