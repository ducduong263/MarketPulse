"""
ingestion/common/gap_detector.py

Detects sequence gaps in cumulative volume/count fields per symbol.
Encapsulates the per-symbol last-seen state that was previously held
as a module-level global dict in individual producers.
"""
from __future__ import annotations


class GapDetector:
    """
    Track the last cumulative value seen per symbol and report gaps.

    Usage:
        gap = GapDetector()
        if gap.check(symbol, total_volume_traded):
            print(f"Gap detected for {symbol}!")
    """

    def __init__(self) -> None:
        self._last: dict[str, int] = {}

    def check(self, symbol: str, current_total: int) -> bool:
        """
        Return True if a gap is detected (current_total decreased or jumped).

        Updates internal state after each call.

        Args:
            symbol:        Instrument symbol used as tracking key.
            current_total: Current cumulative value from the feed.

        Returns:
            True if current_total < last seen value (sequence reset or gap).
        """
        last = self._last.get(symbol)
        self._last[symbol] = current_total
        if last is None:
            return False
        return current_total < last

    def reset(self, symbol: str | None = None) -> None:
        """Reset state for a symbol or all symbols (e.g. on session change)."""
        if symbol is None:
            self._last.clear()
        else:
            self._last.pop(symbol, None)
