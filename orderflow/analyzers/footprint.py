from __future__ import annotations

import duckdb
import pandas as pd
from orderflow.analyzers.base import BaseAnalyzer
from orderflow.core.trade import BUY, Trade

NS_PER_SECOND = 1_000_000_000


class FootprintAnalyzer(BaseAnalyzer):
    M1, M5, M15, H1 = 1, 5, 15, 60

    def __init__(self, *, tick_size: float, timeframe_min: int = M1) -> None:
        self.tick_size = float(tick_size)
        self.timeframe_min = int(timeframe_min)
        self.ts = None  # 当前未收盘桶起点（Unix 秒），与表列 ts 一致
        self.cells = {}  # price_tick -> [bid_vol, ask_vol, bid_cnt, ask_cnt]

        self._con = duckdb.connect(database=":memory:")
        self._init_db()

    def _init_db(self) -> None:
        self._con.execute(
            """
            CREATE TABLE footprint_tf (
                ts BIGINT,
                timeframe_min INT,
                bid_volume DOUBLE,
                ask_volume DOUBLE,
                total_volume DOUBLE,
                delta DOUBLE,
                delta_percent DOUBLE,
                trade_count INT,
                bid_count INT,
                ask_count INT,
                level_count INT
            )
            """
        )
        self._con.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_footprint_tf_ts "
            "ON footprint_tf(ts)"
        )

        self._con.execute(
            """
            CREATE TABLE footprint_levels (
                ts BIGINT,
                price DOUBLE,
                bid_volume DOUBLE,
                ask_volume DOUBLE,
                bid_count INT,
                ask_count INT
            )
            """
        )
        self._con.execute(
            "CREATE INDEX IF NOT EXISTS idx_footprint_levels_ts "
            "ON footprint_levels(ts)"
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
            cell = [0, 0, 0, 0]
            self.cells[price_tick] = cell

        if trade.side == BUY:
            cell[1] += float(trade.size)
            cell[3] += 1
        else:
            cell[0] += float(trade.size)
            cell[2] += 1

    def _flush(self) -> None:
        if self.ts is None or not self.cells:
            self.cells.clear()
            return
        bid_volume_sum = ask_volume_sum = 0.0
        bid_count_sum = ask_count_sum = 0

        params: list[int | float] = []
        for tick, cell in self.cells.items():
            bid_volume, ask_volume, bid_count, ask_count = cell
            price = float(tick) * self.tick_size
            params.extend((self.ts, price, bid_volume, ask_volume, bid_count, ask_count))
            bid_volume_sum += float(bid_volume)
            ask_volume_sum += float(ask_volume)
            bid_count_sum += int(bid_count)
            ask_count_sum += int(ask_count)
        placeholders = ",".join(["(?, ?, ?, ?, ?, ?)"] * len(self.cells))
        self._con.execute(f"INSERT INTO footprint_levels VALUES {placeholders}", params)

        total_volume = bid_volume_sum + ask_volume_sum
        delta = ask_volume_sum - bid_volume_sum
        trade_count_sum = bid_count_sum + ask_count_sum
        delta_percent = (delta / total_volume) if total_volume != 0 else None
        self._con.execute(
            """
            INSERT INTO footprint_tf VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [self.ts, self.timeframe_min, bid_volume_sum, ask_volume_sum, total_volume, delta, delta_percent, trade_count_sum, bid_count_sum, ask_count_sum, len(self.cells)],
        )
        self.cells.clear()

    def reset(self) -> None:
        self.ts = None
        self.cells.clear()
        self._con.execute("DELETE FROM footprint_levels")
        self._con.execute("DELETE FROM footprint_tf")

    def query_levels(
        self,
        *,
        start_ts: int = 0,
        end_ts: int = 9_999_999_999,
    ) -> pd.DataFrame:
        res = self._con.execute(
            f"""
            SELECT ts, price, bid_volume, ask_volume, bid_count, ask_count
            FROM footprint_levels
            WHERE ts >= ? AND ts < ?
            ORDER BY ts, price
            """,
            [int(start_ts), int(end_ts)],
        )
        return res.df()

    def snapshot(self) -> dict[str, object]:
        minute_count = int(
            self._con.execute("SELECT COUNT(DISTINCT ts) FROM footprint_levels")
            .fetchone()[0]
        )
        tf_count = int(self._con.execute("SELECT COUNT(*) FROM footprint_tf").fetchone()[0])
        return {
            "tick_size": self.tick_size,
            "timeframe_min": self.timeframe_min,
            "ts_open": self.ts,
            "minute_count": minute_count,
            "tf_count": tf_count,
        }

