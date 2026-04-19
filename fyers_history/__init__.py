"""
fyers_history
=============
OHLCV historical data fetcher for Fyers API v3.

    from fyers_history import fetch_history
"""

from .history import fetch_history, VALID_RESOLUTIONS

__all__ = ["fetch_history", "VALID_RESOLUTIONS"]
