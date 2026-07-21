"""Compatibility exports for the extracted sentiment strategies.

The multi-strategy prototype implementations that historically lived in this
module have been retired.  Keep these imports so old worker payloads and saved
entrypoints can still resolve the sentiment classes while all new registry
entries use :mod:`a_share_sentiment_strategy` directly.
"""

from app.strategies.active.a_share_sentiment_strategy import (
    AShareSentimentStrategy,
    AShareSentimentV05Strategy,
)

__all__ = ["AShareSentimentStrategy", "AShareSentimentV05Strategy"]
