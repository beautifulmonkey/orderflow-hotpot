"""DuckDB footprint analyzer (cold-data only)."""

from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
from typing import Callable

import duckdb

from orderflow.analyzers.base import BaseAnalyzer
from orderflow.core.trade import BUY, Trade

MINUTE_NS = 60_000_000_000

try:
    from zoneinfo import ZoneInfo as _ZoneInfo
except ImportError:  # pragma: no cover - Python < 3.9
    _ZoneInfo = None

_TimeZoneFactory = Callable[[str], object]

if _ZoneInfo is not None:
    _timezone_factory: _TimeZoneFactory = _ZoneInfo
else:  # pragma: no cover
    import pytz

    _timezone_factory = pytz.timezone


class FootprintAnalyzer(BaseAnalyzer):
    """Persist only closed 1m buckets; query only persisted data."""

    def __init__(
        self,
        *,
        tick_size: float,
        symbol: str = "",
        align_timezone: str = "America/New_York",
        max_minutes: int | None = None,
    ) -> None:
        if tick_size <= 0:
            raise ValueError("tick_size must be > 0")
        if max_minutes is not None and max_minutes <= 0:
            raise ValueError("max_minutes must be > 0")

        self._tick_size = float(tick_size)
        self._symbol = symbol
        self._timezone_name = align_timezone
        self._align_tz = _timezone_factory(align_timezone)
        self._max_minutes = max_minutes
        self._trades_processed = 0

        self._open_minute_ns: int | None = None
        self._open_minute_end_ns: int | None = None
        # tick -> [bid_volume, ask_volume, trade_count, bid_count, ask_count]
        self._open_cells: dict[int, list[float]] = {}

        self._minute_align_cache: dict[int, int] = {}
        self._cold_minutes_queue: deque[int] = deque()
        self._cold_minutes_set: set[int] = set()

        self._con = duckdb.connect(database=":memory:")
        self._con.execute(
            """
            CREATE TABLE footprint_1m (
                minute_start_ns BIGINT,
                price_tick BIGINT,
                bid_volume DOUBLE,
                ask_volume DOUBLE,
                trade_count BIGINT,
                bid_count BIGINT,
                ask_count BIGINT
            )
            """
        )

    @staticmethod
    def _window_minutes(level: str | int) -> int:
        mapping = {"1m": 1, "5m": 5, "15m": 15, 1: 1, 5: 5, 15: 15}
        if level not in mapping:
            raise ValueError("level must be one of: 1m, 5m, 15m, 1, 5, 15")
        return mapping[level]

    def _minute_start_ns(self, ts_ns: int) -> int:
        utc_minute_ns = (ts_ns // MINUTE_NS) * MINUTE_NS
        cached = self._minute_align_cache.get(utc_minute_ns)
        if cached is not None:
            return cached
        dt_utc = datetime.fromtimestamp(utc_minute_ns // 1_000_000_000, tz=timezone.utc)
        dt_local = dt_utc.astimezone(self._align_tz).replace(second=0, microsecond=0)
        minute_ns = int(dt_local.astimezone(timezone.utc).timestamp() * 1_000_000_000)
        self._minute_align_cache[utc_minute_ns] = minute_ns
        return minute_ns

    def _minute_start_for_trade(self, ts_ns: int) -> int:
        if (
            self._open_minute_ns is not None
            and self._open_minute_end_ns is not None
            and self._open_minute_ns <= ts_ns < self._open_minute_end_ns
        ):
            return self._open_minute_ns
        return self._minute_start_ns(ts_ns)

    def _window_start_ns(self, minute_start_ns: int, window_minutes: int) -> int:
        dt_utc = datetime.fromtimestamp(minute_start_ns // 1_000_000_000, tz=timezone.utc)
        dt_local = dt_utc.astimezone(self._align_tz)
        aligned = (dt_local.minute // window_minutes) * window_minutes
        dt_local_window = dt_local.replace(minute=aligned, second=0, microsecond=0)
        return int(dt_local_window.astimezone(timezone.utc).timestamp() * 1_000_000_000)

    def _trim_if_needed(self) -> None:
        if self._max_minutes is None:
            return
        while len(self._cold_minutes_queue) > self._max_minutes:
            oldest = self._cold_minutes_queue.popleft()
            self._cold_minutes_set.discard(oldest)
            self._con.execute("DELETE FROM footprint_1m WHERE minute_start_ns = ?", [oldest])

    def _register_cold_minute(self, minute_ns: int) -> None:
        if minute_ns in self._cold_minutes_set:
            return
        self._cold_minutes_set.add(minute_ns)
        self._cold_minutes_queue.append(minute_ns)

    def _flush_open_minute(self) -> None:
        if self._open_minute_ns is None or not self._open_cells:
            return
        minute_ns = self._open_minute_ns
        rows = []
        for tick, c in self._open_cells.items():
            rows.append((minute_ns, tick, c[0], c[1], int(c[2]), int(c[3]), int(c[4])))
        placeholders = ",".join(["(?, ?, ?, ?, ?, ?, ?)"] * len(rows))
        params: list[float | int] = []
        for row in rows:
            params.extend(row)
        self._con.execute(f"INSERT INTO footprint_1m VALUES {placeholders}", params)
        self._register_cold_minute(minute_ns)
        self._open_minute_ns = None
        self._open_minute_end_ns = None
        self._open_cells = {}
        self._trim_if_needed()

    def on_trade(self, trade: Trade) -> None:
        minute_ns = self._minute_start_for_trade(int(trade.ts_ns))
        if self._open_minute_ns is None:
            self._open_minute_ns = minute_ns
            self._open_minute_end_ns = minute_ns + MINUTE_NS
        elif minute_ns > self._open_minute_ns:
            self._flush_open_minute()
            self._open_minute_ns = minute_ns
            self._open_minute_end_ns = minute_ns + MINUTE_NS

        tick = int(round(trade.price / self._tick_size))
        cell = self._open_cells.get(tick)
        if cell is None:
            cell = [0.0, 0.0, 0.0, 0.0, 0.0]
            self._open_cells[tick] = cell
        if trade.side == BUY:
            cell[1] += trade.size
            cell[4] += 1.0
        else:
            cell[0] += trade.size
            cell[3] += 1.0
        cell[2] += 1.0
        self._trades_processed += 1

    def on_batch(self, trades: list[Trade]) -> None:
        for trade in trades:
            self.on_trade(trade)

    def query_rows(self, *, level: str | int = "1m", start_ns: int | None = None, end_ns: int | None = None, count: int | None = None) -> dict[str, object]:
        if count is not None and (start_ns is not None or end_ns is not None):
            raise ValueError("Use either (start_ns + end_ns) or count, not both")
        if count is None and (start_ns is None or end_ns is None):
            raise ValueError("Provide either count, or both start_ns and end_ns")

        wm = self._window_minutes(level)
        window_ns = wm * MINUTE_NS
        # minute_start_ns 已经是按业务时区对齐到分钟，因此这里可直接按窗口纳秒整除聚合。
        window_expr = (
            "minute_start_ns"
            if wm == 1
            else f"(minute_start_ns / {window_ns}) * {window_ns}"
        )
        if count is not None:
            windows_filter = f"""
                SELECT window_start_ns
                FROM (
                    SELECT {window_expr} AS window_start_ns
                    FROM footprint_1m
                    GROUP BY 1
                    ORDER BY 1 DESC
                    LIMIT ?
                ) t
            """
            params = [int(count)]
        else:
            assert start_ns is not None and end_ns is not None
            windows_filter = f"""
                SELECT {window_expr} AS window_start_ns
                FROM footprint_1m
                GROUP BY 1
                HAVING window_start_ns < ? AND (window_start_ns + ?) > ?
            """
            params = [int(end_ns), int(window_ns), int(start_ns)]

        vals = self._con.execute(
            f"""
            WITH windows AS (
                {windows_filter}
            )
            SELECT
                w.window_start_ns,
                SUM(f.bid_volume) AS bid_volume,
                SUM(f.ask_volume) AS ask_volume,
                SUM(f.trade_count) AS trade_count,
                SUM(f.bid_count) AS bid_count,
                SUM(f.ask_count) AS ask_count
            FROM footprint_1m f
            INNER JOIN windows w
                ON {window_expr} = w.window_start_ns
            GROUP BY 1
            ORDER BY 1
            """,
            params,
        ).fetchall()

        rows: list[dict[str, object]] = []
        for ws, sum_bid, sum_ask, sum_trade, sum_bid_count, sum_ask_count in vals:
            total = float(sum_ask) + float(sum_bid)
            delta = float(sum_ask) - float(sum_bid)
            rows.append(
                {
                    "timeframe": f"{wm}m",
                    "window_start_ns": int(ws),
                    "window_end_ns": int(ws) + window_ns,
                    "window_label": "",
                    "ts_ns": int(ws),
                    "ts": datetime.fromtimestamp(int(ws) / 1_000_000_000, tz=timezone.utc).isoformat(),
                    "price": None,
                    "symbol": self._symbol,
                    "bid_volume": float(sum_bid),
                    "ask_volume": float(sum_ask),
                    "total_volume": total,
                    "delta": delta,
                    "trade_count": int(sum_trade),
                    "bid_count": int(sum_bid_count),
                    "ask_count": int(sum_ask_count),
                    "delta_percent": (delta / total) if total else None,
                }
            )
        return {"timeframe": f"{wm}m", "window_minutes": wm, "window_count": len(rows), "row_count": len(rows), "rows": rows}

    def snapshot(self) -> dict[str, object]:
        n = int(self._con.execute("SELECT COUNT(DISTINCT minute_start_ns) FROM footprint_1m").fetchone()[0])
        c = max(n, 1)
        return {
            "tick_size": self._tick_size,
            "symbol": self._symbol,
            "align_timezone": self._timezone_name,
            "trades_processed": self._trades_processed,
            "bucket_count": n,
            "rows_1m": self.query_rows(level="1m", count=c)["rows"],
            "rows_5m": self.query_rows(level="5m", count=c)["rows"],
            "rows_15m": self.query_rows(level="15m", count=c)["rows"],
        }

    def reset(self) -> None:
        self._open_minute_ns = None
        self._open_minute_end_ns = None
        self._open_cells = {}
        self._minute_align_cache.clear()
        self._cold_minutes_queue.clear()
        self._cold_minutes_set.clear()
        self._con.execute("DELETE FROM footprint_1m")
        self._trades_processed = 0
