from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.ticker import ScalarFormatter
from scipy.ndimage import gaussian_filter1d
from scipy.signal import find_peaks

try:
    import plotly.graph_objects as go

    HAS_PLOTLY = True
except Exception:
    HAS_PLOTLY = False


@dataclass
class PeakDetectionConfig:
    smooth_sigma: float = 2.0
    prominence_k: float = 1.8
    min_distance_bins: int = 6
    min_width_bins: float = 2.0


@dataclass
class PeakDetectionResult:
    peak_indices: np.ndarray
    peak_prices: np.ndarray
    peak_smoothed_volumes: np.ndarray
    prominences: np.ndarray
    widths: np.ndarray
    smoothed_volume: np.ndarray


class VolumeProfileTool:
    """
    A compact OO interface inspired by chart-style APIs:
    1) pass a preprocessed DataFrame
    2) detect peaks
    3) plot static/interactive chart
    """

    def __init__(
        self,
        data: pd.DataFrame,
        time_col: str = "time",
        price_col: str = "price",
        bid_volume_col: str = "bid_volume",
        ask_volume_col: str = "ask_volume",
        start: Optional[str] = None,
        end: Optional[str] = None,
        config: PeakDetectionConfig | None = None,
    ) -> None:
        self.data = data.copy()
        self.time_col = time_col
        self.price_col = price_col
        self.bid_volume_col = bid_volume_col
        self.ask_volume_col = ask_volume_col
        self.start = start
        self.end = end
        self.config = config or PeakDetectionConfig()
        self.prices, self.volumes = self._build_profile()
        self._last_hvn_result: Optional[PeakDetectionResult] = None

    def _build_profile(self) -> Tuple[np.ndarray, np.ndarray]:
        required = [
            self.time_col,
            self.price_col,
            self.bid_volume_col,
            self.ask_volume_col,
        ]
        missing = [col for col in required if col not in self.data.columns]
        if missing:
            raise ValueError(
                "Input DataFrame missing required columns: "
                + ", ".join(missing)
                + ". Expected: time, price, bid_volume, ask_volume (names are configurable)."
            )

        df = self.data[required].copy()
        df[self.time_col] = pd.to_datetime(df[self.time_col], errors="coerce")
        df = df.dropna(subset=[self.time_col, self.price_col, self.bid_volume_col, self.ask_volume_col])

        if self.start:
            start_ts = pd.to_datetime(self.start)
            df = df[df[self.time_col] >= start_ts]
        if self.end:
            end_ts = pd.to_datetime(self.end)
            df = df[df[self.time_col] <= end_ts]

        if df.empty:
            raise ValueError("No rows available after required-column validation and time filtering.")

        df["__total_volume__"] = df[self.bid_volume_col].astype(float) + df[self.ask_volume_col].astype(float)

        grouped = (
            df[[self.price_col, "__total_volume__"]]
            .groupby(self.price_col, as_index=False)["__total_volume__"]
            .sum()
            .sort_values(self.price_col)
        )
        prices = grouped[self.price_col].to_numpy(dtype=float)
        volumes = grouped["__total_volume__"].to_numpy(dtype=float)
        return prices, volumes

    @staticmethod
    def _robust_mad(x: np.ndarray) -> float:
        med = np.median(x)
        return float(np.median(np.abs(x - med)) + 1e-9)

    def _smooth(self) -> np.ndarray:
        return gaussian_filter1d(self.volumes, sigma=self.config.smooth_sigma)

    def hvn_peaks(self) -> PeakDetectionResult:
        smoothed = self._smooth()
        min_prominence = self.config.prominence_k * self._robust_mad(smoothed)

        peak_idx, props = find_peaks(
            smoothed,
            prominence=min_prominence,
            distance=self.config.min_distance_bins,
            width=self.config.min_width_bins,
        )
        prominences = np.asarray(props.get("prominences", np.zeros(len(peak_idx))), dtype=float)
        widths = np.asarray(props.get("widths", np.zeros(len(peak_idx))), dtype=float)

        result = PeakDetectionResult(
            peak_indices=peak_idx,
            peak_prices=self.prices[peak_idx].astype(float),
            peak_smoothed_volumes=smoothed[peak_idx].astype(float),
            prominences=prominences,
            widths=widths,
            smoothed_volume=smoothed,
        )
        self._last_hvn_result = result
        return result

    def plot(
        self,
        interactive: bool = True,
        title: str = "Volume Profile Peak Detection",
    ) -> None:
        result = self._last_hvn_result or self.hvn_peaks()
        if interactive:
            self._plot_interactive(result=result, title=title)
        else:
            self._plot_static(result=result, title=title)

    def _plot_static(self, result: PeakDetectionResult, title: str) -> None:
        fig, ax = plt.subplots(figsize=(11, 10))
        if len(self.prices) > 1:
            bar_h = float(np.median(np.diff(self.prices))) * 0.9
        else:
            bar_h = 1.0

        ax.barh(self.prices, self.volumes, height=bar_h, color="#9e9e9e", alpha=0.9, label="Raw VP")
        ax.plot(result.smoothed_volume, self.prices, color="#1565C0", linewidth=2.2, label="Smoothed VP")

        if len(result.peak_indices) > 0:
            p = result.peak_indices
            ax.scatter(
                result.smoothed_volume[p],
                self.prices[p],
                s=70,
                color="#D32F2F",
                marker="o",
                zorder=5,
                label="Detected Peaks",
            )
            for idx in p:
                ax.annotate(
                    f"{self.prices[idx]:.2f}",
                    (result.smoothed_volume[idx], self.prices[idx]),
                    textcoords="offset points",
                    xytext=(10, 0),
                    va="center",
                    fontsize=9,
                    color="#222222",
                )

        ax.set_title(title)
        ax.set_xlabel("Volume")
        ax.set_ylabel("Price")
        yfmt = ScalarFormatter(useOffset=False)
        yfmt.set_scientific(False)
        ax.yaxis.set_major_formatter(yfmt)
        ax.grid(alpha=0.2)
        ax.legend()
        plt.tight_layout()
        plt.show()

    def _plot_interactive(self, result: PeakDetectionResult, title: str) -> None:
        if not HAS_PLOTLY:
            raise RuntimeError("plotly is not installed. Run: pip install plotly")

        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=self.volumes,
                y=self.prices,
                name="Raw VP",
                orientation="h",
                marker=dict(color="#9e9e9e", opacity=0.9),
                hovertemplate="Volume=%{x:.2f}<br>Price=%{y:.2f}<extra></extra>",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=result.smoothed_volume,
                y=self.prices,
                mode="lines",
                name="Smoothed VP",
                line=dict(color="#1565C0", width=2.5),
                hovertemplate="SmoothVol=%{x:.2f}<br>Price=%{y:.2f}<extra></extra>",
            )
        )
        if len(result.peak_indices) > 0:
            p = result.peak_indices
            fig.add_trace(
                go.Scatter(
                    x=result.smoothed_volume[p],
                    y=self.prices[p],
                    mode="markers+text",
                    name="Detected Peaks",
                    marker=dict(color="#D32F2F", size=9),
                    text=[f"{self.prices[idx]:.2f}" for idx in p],
                    textposition="middle right",
                    hovertemplate="Price=%{text}<br>SmoothVol=%{x:.2f}<extra></extra>",
                )
            )

        fig.update_layout(
            title=title,
            xaxis_title="Volume",
            yaxis_title="Price",
            template="plotly_white",
            hovermode="closest",
            dragmode="zoom",
            yaxis=dict(tickformat=".2f"),
        )
        fig.show(config={"scrollZoom": True, "displaylogo": False})
