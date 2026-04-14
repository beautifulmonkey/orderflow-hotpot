from __future__ import annotations

import duckdb
from datetime import datetime

from orderflow.analyzers.base import BaseAnalyzer
from orderflow.core.trade import BUY, Trade

NS_PER_SECOND = 1_000_000_000


class FootprintAnalyzerSimple(BaseAnalyzer):
    M1, M5, M15, H1 = 1, 5, 15, 60

    def __init__(self, *, tick_size: float, timeframe_min: int = M1) -> None:
        self.tick_size = float(tick_size)
        self.timeframe_min = int(timeframe_min)
        self.ts = None  # 当前未收盘桶起点（Unix 秒），与表列 ts 一致
        self.cells = {}  # price_tick -> [bid_vol, ask_vol, trade_cnt, bid_cnt, ask_cnt]

        self._con = duckdb.connect(database=":memory:")
        self._con.execute(
            """
            CREATE TABLE footprint_1m (
                ts BIGINT,
                price DOUBLE,
                bid_volume DOUBLE,
                ask_volume DOUBLE,
                trade_count INT,
                bid_count INT,
                ask_count INT
            )
            """
        )
        self._con.execute(
            "CREATE INDEX IF NOT EXISTS idx_footprint_1m_ts "
            "ON footprint_1m(ts)"
        )

    def on_trade(self, trade: Trade) -> None:
        ts_sec = int(trade.ts_ns) // NS_PER_SECOND
        bucket_sec = self.timeframe_min * 60
        bucket_ts = (ts_sec // bucket_sec) * bucket_sec
        if self.ts is None:
            self.ts = bucket_ts
        elif bucket_ts > self.ts:
            self._flush()
            self.ts = bucket_ts

        price_tick = int(round(float(trade.price) / self.tick_size))
        cell = self.cells.get(price_tick)
        if cell is None:
            cell = [0, 0, 0, 0, 0]
            self.cells[price_tick] = cell

        if trade.side == BUY:
            cell[1] += float(trade.size)
            cell[4] += 1
        else:
            cell[0] += float(trade.size)
            cell[3] += 1
        cell[2] += 1

    def _flush(self) -> None:
        if self.ts is None or not self.cells:
            self.cells.clear()
            return
        print(str(datetime.fromtimestamp(self.ts)))
        params = []
        for tick, cell in self.cells.items():
            price = float(tick) * self.tick_size
            params.extend((self.ts, price, *cell))
        placeholders = ",".join(["(?, ?, ?, ?, ?, ?, ?)"] * len(self.cells))
        self._con.execute(f"INSERT INTO footprint_1m VALUES {placeholders}", params)
        self.cells.clear()

    def reset(self) -> None:
        self.ts = None
        self.cells.clear()
        self._con.execute("DELETE FROM footprint_1m")

    def snapshot(self) -> dict[str, object]:
        minute_count = int(
            self._con.execute("SELECT COUNT(DISTINCT ts) FROM footprint_1m")
            .fetchone()[0]
        )
        return {
            "tick_size": self.tick_size,
            "timeframe_min": self.timeframe_min,
            "ts_open": self.ts,
            "minute_count": minute_count,
        }

