import sqlite3
import os
from datetime import datetime
from typing import Optional

from dagster import ConfigurableResource
from dagster._config.pythonic_config.resource import InitResourceContext


class CurrencyRatesDB(ConfigurableResource):
    db_path: str

    _conn: Optional[sqlite3.Connection] = None

    def setup_for_execution(self, context: InitResourceContext) -> None:
        if self.db_path:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        self._conn.row_factory = sqlite3.Row
        self._initialize_schema()

    def setup(self) -> None:
        if self.db_path:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        self._conn.row_factory = sqlite3.Row
        self._initialize_schema()

    def _initialize_schema(self) -> None:
        if not self._conn:
            raise RuntimeError("DB not initialized")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS exchange_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                base TEXT NOT NULL DEFAULT 'EUR',
                quote TEXT NOT NULL DEFAULT 'GBP',
                rate REAL NOT NULL,
                timestamp TEXT NOT NULL,
                UNIQUE(base, quote, timestamp)
            )
            """
        )
        self._conn.commit()

    def insert_rate(self, base: str, quote: str, rate: float, timestamp: str) -> bool:
        if not self._conn:
            raise RuntimeError("DB not initialized")
        try:
            self._conn.execute(
                "INSERT OR IGNORE INTO exchange_rates (base, quote, rate, timestamp) VALUES (?, ?, ?, ?)",
                (base, quote, rate, timestamp),
            )
            self._conn.commit()
            return True
        except Exception:
            return False

    def get_last_timestamp(self) -> Optional[datetime]:
        if not self._conn:
            raise RuntimeError("DB not initialized")
        cursor = self._conn.execute(
            "SELECT timestamp FROM exchange_rates ORDER BY timestamp DESC LIMIT 1"
        )
        row = cursor.fetchone()
        if not row:
            return None
        return datetime.fromisoformat(row[0])

    def get_all_rates(self) -> list:
        if not self._conn:
            raise RuntimeError("DB not initialized")
        cursor = self._conn.execute("SELECT * FROM exchange_rates ORDER BY timestamp ASC")
        return cursor.fetchall()

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None