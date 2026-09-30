from __future__ import annotations

import json
import math
import os
import threading
from decimal import Decimal
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight

from binance_predicted_funding.config import (
    DUCKPORT_APPEND_MAX_MESSAGE_BYTES,
    PREDICTED_FUNDING_TABLE,
)
from binance_predicted_funding.log import logger
from binance_predicted_funding.model import PREDICTED_FUNDING_COLUMNS


class DuckportClient:
    COLUMN_DEFS = [
        "open_time TIMESTAMP",
        "symbol VARCHAR",
        "funding_rate DECIMAL(38,18)",
        "next_funding_time TIMESTAMP",
        "interest_rate DECIMAL(38,18)",
        "source_time TIMESTAMP",
    ]
    ARROW_SCHEMA = pa.schema(
        [
            ("open_time", pa.timestamp("ns")),
            ("symbol", pa.string()),
            ("funding_rate", pa.decimal128(38, 18)),
            ("next_funding_time", pa.timestamp("ns")),
            ("interest_rate", pa.decimal128(38, 18)),
            ("source_time", pa.timestamp("ns")),
        ]
    )
    _KEEPALIVE_OPTIONS = [
        ("grpc.keepalive_time_ms", 30_000),
        ("grpc.keepalive_timeout_ms", 10_000),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.http2.max_pings_without_data", 0),
    ]

    def __init__(self, addr: str = "localhost:50051", schema: str = "data"):
        self.location = f"grpc://{addr}"
        self.schema = schema
        self.table = PREDICTED_FUNDING_TABLE
        self.ingestor = (
            os.getenv("INGESTOR_NAME")
            or os.getenv("INGESTOR_INSTANCE")
            or os.getenv("INGESTOR_EXEC")
            or "binance-predicted-funding"
        )
        self.max_lag_seconds = int(os.getenv("WATERMARK_MAX_LAG_SECONDS", "180"))
        self.append_max_message_bytes = DUCKPORT_APPEND_MAX_MESSAGE_BYTES
        self._write_lock = threading.Lock()
        self.client = flight.FlightClient(self.location, generic_options=self._KEEPALIVE_OPTIONS)
        logger.info(f"DuckportClient connected: {self.location}, schema={schema}")

    @staticmethod
    def _sql_str(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def _reconnect(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass
        self.client = flight.FlightClient(self.location, generic_options=self._KEEPALIVE_OPTIONS)

    def ping(self) -> dict:
        results = list(self.client.do_action(flight.Action("duckport.ping", b"")))
        return json.loads(results[0].body.to_pybytes())

    def execute(self, sql: str) -> dict:
        action = flight.Action("duckport.execute", json.dumps({"sql": sql}).encode())
        try:
            results = list(self.client.do_action(action))
        except flight.FlightUnavailableError:
            self._reconnect()
            results = list(self.client.do_action(action))
        return json.loads(results[0].body.to_pybytes())

    def execute_transaction(self, statements: list[str]) -> dict:
        body = json.dumps({"statements": statements}).encode()
        action = flight.Action("duckport.execute_transaction", body)
        try:
            results = list(self.client.do_action(action))
        except flight.FlightUnavailableError:
            self._reconnect()
            results = list(self.client.do_action(action))
        return json.loads(results[0].body.to_pybytes())

    def append(self, schema: str, table: str, data: pa.Table) -> dict:
        descriptor = flight.FlightDescriptor.for_path("duckport.append", schema, table)
        try:
            writer, reader = self.client.do_put(descriptor, data.schema)
        except flight.FlightUnavailableError:
            self._reconnect()
            writer, reader = self.client.do_put(descriptor, data.schema)
        bytes_per_row = max(1, math.ceil(data.nbytes / max(1, len(data))))
        max_chunk = max(1, self.append_max_message_bytes // bytes_per_row)
        writer.write_table(data, max_chunksize=max_chunk)
        writer.done_writing()
        response = json.loads(bytes(reader.read()))
        writer.close()
        return response

    def read_table(self, schema: str, table: str) -> pa.Table:
        ticket = flight.Ticket(json.dumps({"schema": schema, "table": table}).encode())
        try:
            reader = self.client.do_get(ticket)
        except flight.FlightUnavailableError:
            self._reconnect()
            reader = self.client.do_get(ticket)
        return reader.read_all()

    def init_schema(self, start_time: pd.Timestamp) -> None:
        columns = ", ".join(self.COLUMN_DEFS)
        start_ts = pd.to_datetime(start_time, utc=True).strftime("%Y-%m-%d %H:%M:%S")
        statements = [
            f"CREATE SCHEMA IF NOT EXISTS {self.schema}",
            f"CREATE TABLE IF NOT EXISTS {self.schema}.{self.table} "
            f"({columns}, PRIMARY KEY (open_time, symbol))",
            f"DROP TABLE IF EXISTS _staging_{self.table}",
            f"CREATE TABLE _staging_{self.table} ({columns})",
            f"INSERT INTO {self.schema}.watermark "
            f"(table_name, ingestor, max_lag_seconds, time_column, start_time, duck_time, updated_at) "
            f"VALUES ('{self.table}', {self._sql_str(self.ingestor)}, {self.max_lag_seconds}, "
            f"'open_time', '{start_ts}', NULL, CURRENT_TIMESTAMP) "
            f"ON CONFLICT (table_name) DO UPDATE SET "
            f"ingestor = excluded.ingestor, max_lag_seconds = excluded.max_lag_seconds, "
            f"time_column = excluded.time_column",
        ]
        self.execute_transaction(statements)
        logger.info(f"Schema '{self.schema}' initialised for {self.table}")

    def _arrow(self, frame: pd.DataFrame) -> pa.Table:
        records = []
        for row in frame.itertuples(index=False):
            def naive(value):
                timestamp = pd.to_datetime(value, utc=True)
                return timestamp.tz_localize(None).to_pydatetime()

            interest = row.interest_rate
            records.append(
                {
                    "open_time": naive(row.open_time),
                    "symbol": str(row.symbol),
                    "funding_rate": Decimal(row.funding_rate),
                    "next_funding_time": naive(row.next_funding_time),
                    "interest_rate": None if interest is None or pd.isna(interest) else Decimal(interest),
                    "source_time": naive(row.source_time),
                }
            )
        return pa.Table.from_pylist(records, schema=self.ARROW_SCHEMA)

    def write_slice(self, frame: pd.DataFrame, complete_through: pd.Timestamp) -> dict:
        timestamp = pd.to_datetime(complete_through, utc=True).strftime("%Y-%m-%d %H:%M:%S")
        target = f"{self.schema}.{self.table}"
        staging = f"_staging_{self.table}"
        with self._write_lock:
            self.execute(f"TRUNCATE {staging}")
            if frame.empty:
                response = self.execute_transaction([self._watermark_sql(timestamp), f"TRUNCATE {staging}"])
                return response
            arrow = self._arrow(frame[PREDICTED_FUNDING_COLUMNS])
            append_response = self.append("main", staging, arrow)
            response = self.execute_transaction(
                [
                    f"INSERT INTO {target} BY NAME SELECT * FROM {staging} "
                    f"ON CONFLICT (open_time, symbol) DO UPDATE SET "
                    f"funding_rate = excluded.funding_rate, "
                    f"next_funding_time = excluded.next_funding_time, "
                    f"interest_rate = excluded.interest_rate, "
                    f"source_time = excluded.source_time "
                    f"WHERE excluded.source_time >= {target}.source_time",
                    self._watermark_sql(timestamp),
                    f"TRUNCATE {staging}",
                ]
            )
        logger.info(
            f"Predicted funding write complete: staged={append_response.get('rows_appended', len(arrow))}, "
            f"through={timestamp}"
        )
        return response

    def _watermark_sql(self, timestamp: str) -> str:
        return (
            f"INSERT INTO {self.schema}.watermark "
            f"(table_name, ingestor, max_lag_seconds, time_column, start_time, duck_time, updated_at) "
            f"VALUES ('{self.table}', {self._sql_str(self.ingestor)}, "
            f"{self.max_lag_seconds}, 'open_time', NULL, '{timestamp}', CURRENT_TIMESTAMP) "
            f"ON CONFLICT (table_name) DO UPDATE SET "
            f"ingestor = excluded.ingestor, max_lag_seconds = excluded.max_lag_seconds, "
            f"duck_time = excluded.duck_time, updated_at = excluded.updated_at"
        )

    def read_duck_time(self) -> Optional[pd.Timestamp]:
        try:
            frame = self.read_table(self.schema, "watermark").to_pandas()
            row = frame[frame["table_name"] == self.table]
            if row.empty or pd.isna(row["duck_time"].iloc[0]):
                return None
            return pd.to_datetime(row["duck_time"].iloc[0], utc=True)
        except Exception as exc:
            logger.warning(f"Cannot read predicted funding watermark: {exc}")
            return None

    def close(self) -> None:
        self.client.close()
        logger.info("DuckportClient closed")
