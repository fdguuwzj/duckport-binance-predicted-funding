"""Map one premiumIndex payload into a single minute cross-section."""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any

import pandas as pd

PREDICTED_FUNDING_COLUMNS = [
    "open_time",
    "symbol",
    "funding_rate",
    "next_funding_time",
    "interest_rate",
    "source_time",
]
_RATE = re.compile(r"^-?\d+(\.\d+)?$")


def _decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    text = str(value)
    if not _RATE.fullmatch(text):
        return None
    try:
        parsed = Decimal(text)
    except InvalidOperation:
        return None
    return parsed if parsed.is_finite() else None


def _millis(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and not value.is_integer():
        return None
    millis = int(value)
    if millis <= 0:
        return None
    return millis


def parse_premium_index(payload: Any) -> pd.DataFrame:
    """Keep USDT/USDC perpetuals from the latest minute in one payload.

    ``open_time`` is Binance ``time`` floored to the minute. Dated futures and
    blank rates are dropped. This is the predicted rate, not a settlement.
    """
    empty = pd.DataFrame(columns=PREDICTED_FUNDING_COLUMNS)
    if not isinstance(payload, list):
        raise ValueError("premiumIndex payload must be an array")
    records = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        symbol = row.get("symbol")
        if not isinstance(symbol, str) or "_" in symbol:
            continue
        if symbol.endswith("USDT"):
            quote_length = 4
        elif symbol.endswith("USDC"):
            quote_length = 4
        else:
            continue
        base = symbol[:-quote_length]
        if not base.isalnum():
            continue
        rate = _decimal(row.get("lastFundingRate"))
        source_ms = _millis(row.get("time"))
        next_ms = _millis(row.get("nextFundingTime"))
        if rate is None or source_ms is None or next_ms is None:
            continue
        source_time = pd.to_datetime(source_ms, unit="ms", utc=True)
        records.append(
            {
                "open_time": source_time.floor("1min"),
                "symbol": symbol,
                "funding_rate": rate,
                "next_funding_time": pd.to_datetime(next_ms, unit="ms", utc=True),
                "interest_rate": _decimal(row.get("interestRate")),
                "source_time": source_time,
            }
        )
    if not records:
        return empty
    latest = max(record["open_time"] for record in records)
    frame = pd.DataFrame.from_records(
        [record for record in records if record["open_time"] == latest],
        columns=PREDICTED_FUNDING_COLUMNS,
    )
    frame = frame.sort_values("source_time").drop_duplicates("symbol", keep="last")
    return frame.sort_values("symbol").reset_index(drop=True)


def plan_snapshot(now, duck_time, delay_seconds: int = 2):
    """Return ``("capture", bar)`` or ``("sleep", until)``. Missed minutes stay empty."""
    current = pd.Timestamp(now)
    if current.tzinfo is None:
        current = current.tz_localize("UTC")
    else:
        current = current.tz_convert("UTC")
    bar = current.floor("1min")
    ready = bar + pd.Timedelta(seconds=delay_seconds)
    if current < ready:
        return ("sleep", ready)
    if duck_time is not None:
        stored = pd.Timestamp(duck_time)
        if stored.tzinfo is None:
            stored = stored.tz_localize("UTC")
        else:
            stored = stored.tz_convert("UTC")
        if stored >= bar:
            return ("sleep", bar + pd.Timedelta(minutes=1, seconds=delay_seconds))
    return ("capture", bar)
