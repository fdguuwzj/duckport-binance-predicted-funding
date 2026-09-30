#!/usr/bin/env python3
"""Sample Binance premiumIndex.lastFundingRate once per minute into duckport."""

from __future__ import annotations

import asyncio
import gc
import os
import signal
import sys
import threading

os.environ["TZ"] = "UTC"

import aiohttp
import pandas as pd

from binance_predicted_funding.config import (
    DUCKPORT_ADDR,
    DUCKPORT_SCHEMA,
    HTTP_TIMEOUT_SECONDS,
    POLL_DELAY_SECONDS,
    proxy,
)
from binance_predicted_funding.duckport_client import DuckportClient
from binance_predicted_funding.log import logger
from binance_predicted_funding.model import parse_premium_index, plan_snapshot

PREMIUM_INDEX_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"


async def fetch_premium_index(session: aiohttp.ClientSession):
    async with session.get(PREMIUM_INDEX_URL, proxy=proxy) as response:
        response.raise_for_status()
        payload = await response.json(content_type=None)
    if not isinstance(payload, list):
        raise RuntimeError("premiumIndex did not return an array")
    return payload


class PredictedFundingJobs:
    def __init__(self, client: DuckportClient, *, auto_start: bool = True):
        self.client = client
        self.duck_time = client.read_duck_time()
        logger.info(f"Predicted funding duck_time loaded: {self.duck_time}")
        logger.info("1m predicted funding starts at the current minute; missed minutes are not backfilled")
        if auto_start:
            threading.Thread(target=self._run_loop, daemon=True).start()

    def _run_loop(self) -> None:
        asyncio.run(self.run_forever())

    async def run_forever(self) -> None:
        while True:
            action, when = plan_snapshot(pd.Timestamp.now(tz="UTC"), self.duck_time, POLL_DELAY_SECONDS)
            if action == "sleep":
                delay = max(0.0, (when - pd.Timestamp.now(tz="UTC")).total_seconds())
                logger.info(f"Next predicted funding sample at {when.isoformat()}")
                await asyncio.sleep(delay)
                continue
            try:
                await self.capture()
            except Exception as exc:
                logger.error(f"Predicted funding sample failed: {exc}")
                gc.collect()
                await asyncio.sleep(2)

    async def capture(self) -> bool:
        timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            payload = await fetch_premium_index(session)
        frame = parse_premium_index(payload)
        if frame.empty:
            raise RuntimeError("premiumIndex produced no perpetual funding rows")
        bar = pd.Timestamp(frame["open_time"].iloc[0])
        if bar.tzinfo is None:
            bar = bar.tz_localize("UTC")
        if self.duck_time is not None and bar <= pd.Timestamp(self.duck_time):
            logger.info(f"Predicted funding minute {bar} is already stored")
            return False
        self.client.write_slice(frame, bar)
        self.duck_time = bar
        logger.info(f"Predicted funding sample complete: {bar}, rows={len(frame):,}")
        return True


def main() -> None:
    os.environ.setdefault("WATERMARK_MAX_LAG_SECONDS", "180")
    logger.info("Binance 1m predicted-funding ingestor starting")
    client = DuckportClient(addr=DUCKPORT_ADDR, schema=DUCKPORT_SCHEMA)

    def shutdown(signum, frame):
        logger.warning(f"Received signal {signum}; shutting down")
        client.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        info = client.ping()
        logger.info(f"duckport-rs server: {info}")
    except Exception as exc:
        logger.error(f"Cannot reach duckport-rs at {DUCKPORT_ADDR}: {exc}")
        sys.exit(1)

    client.init_schema(pd.Timestamp.now(tz="UTC").floor("1min"))
    PredictedFundingJobs(client)
    try:
        signal.pause()
    except AttributeError:
        threading.Event().wait()


if __name__ == "__main__":
    main()
