"""Environment-driven configuration for binance-predicted-funding."""

from __future__ import annotations

import os
import pathlib

from dotenv import load_dotenv

_env_file = os.getenv("INGESTOR_ENV_FILE", "config.env")
load_dotenv(_env_file, override=False)

_server_env = os.getenv(
    "DUCKPORT_SERVER_ENV",
    str(pathlib.Path.home() / ".duckport" / "server.env"),
)
load_dotenv(_server_env, override=False)
load_dotenv("/opt/duckport/server.env", override=False)


def _derive_duckport_addr() -> str:
    raw = os.getenv("DUCKPORT_LISTEN_ADDR", "0.0.0.0:50051")
    host, _, port = raw.rpartition(":")
    host = "localhost" if host in ("0.0.0.0", "", "*") else host
    return f"{host}:{port}"


def _bounded_int(name: str, default: int, minimum: int, maximum: int) -> int:
    value = int(os.getenv(name, str(default)))
    return min(maximum, max(minimum, value))


DUCKPORT_ADDR = _derive_duckport_addr()
DUCKPORT_SCHEMA = "data"
PREDICTED_FUNDING_TABLE = "um_predicted_funding_1m"
POLL_DELAY_SECONDS = _bounded_int("POLL_DELAY_SECONDS", 2, 0, 40)
HTTP_TIMEOUT_SECONDS = _bounded_int("HTTP_TIMEOUT_SECONDS", 20, 5, 60)
PROXY_URL = os.getenv("PROXY_URL", "").strip()
proxy = PROXY_URL or None
DUCKPORT_APPEND_MAX_MESSAGE_BYTES = int(
    os.getenv("DUCKPORT_APPEND_MAX_MESSAGE_BYTES", str(48 * 1024 * 1024))
)
