import sqlite3
from pathlib import Path
from typing import Optional

from dagster import ConfigurableResource
from dagster._config.pythonic_config.resource import InitResourceContext
from pydantic import model_validator


class SqliteClientResource(ConfigurableResource):
    db_path: str

    _conn: Optional[sqlite3.Connection] = None

    @model_validator(mode="after")
    def check_db_path(self) -> "SqliteClientResource":
        if not self.db_path:
            raise ValueError("SQLITE_PATH must be set")
        return self

    def create_resource(self, context: InitResourceContext) -> "SqliteClientResource":
        self.setup_for_execution(context)
        return self

    def setup_for_execution(self, context: InitResourceContext) -> None:
        if self._conn is not None:
            return
        db_file = Path(self.db_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            self.db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
        self._conn.row_factory = sqlite3.Row
        self._initialize_schema()

    def _initialize_schema(self) -> None:
        if not self._conn:
            raise RuntimeError("SQLite connection not initialized")

        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS cardlist_dimension (
                card_id TEXT NOT NULL,
                card_version TEXT,
                card_name TEXT NOT NULL,
                set_code TEXT NOT NULL,
                rarity TEXT,
                card_type TEXT,
                attribute TEXT,
                power INTEGER,
                cost INTEGER,
                color TEXT,
                source_url TEXT NOT NULL,
                scraped_at TIMESTAMP NOT NULL,
                PRIMARY KEY (card_id, card_version, source_url, scraped_at)
            );

            CREATE TABLE IF NOT EXISTS fact_events (
                card_id TEXT NOT NULL,
                card_version TEXT,
                event_type TEXT NOT NULL,
                price REAL,
                currency TEXT,
                sold_date TEXT,
                scraped_from TEXT NOT NULL,
                source TEXT,
                source_url TEXT NOT NULL,
                language TEXT NOT NULL DEFAULT 'EN',
                scraped_at TIMESTAMP NOT NULL,
                PRIMARY KEY (card_id, source_url)
            );

            CREATE INDEX IF NOT EXISTS idx_cardlist_card_id
                ON cardlist_dimension(card_id);
            CREATE INDEX IF NOT EXISTS idx_fact_events_card_id
                ON fact_events(card_id);
            CREATE INDEX IF NOT EXISTS idx_fact_events_sold_date
                ON fact_events(sold_date);
        """)
        try:
            self._conn.execute(
                "ALTER TABLE fact_events ADD COLUMN image_url TEXT"
            )
        except Exception:
            pass
        try:
            self._conn.execute(
                "ALTER TABLE fact_events ADD COLUMN local_image_path TEXT"
            )
        except Exception:
            pass
        try:
            self._conn.execute(
                "ALTER TABLE fact_events ADD COLUMN parqueted INTEGER DEFAULT 0"
            )
        except Exception:
            pass
        self._conn.commit()

    def execute(
        self,
        query: str,
        params: tuple = (),
        fetch: str = "none",
    ) -> list[sqlite3.Row] | int | None:
        if not self._conn:
            raise RuntimeError("SQLite connection not initialized")
        cursor = self._conn.cursor()
        cursor.execute(query, params)
        if fetch == "all":
            return cursor.fetchall()
        elif fetch == "one":
            return cursor.fetchone()
        else:
            self._conn.commit()
            return cursor.lastrowid

    def execute_many(self, query: str, params_list: list[tuple]) -> None:
        if not self._conn:
            raise RuntimeError("SQLite connection not initialized")
        cursor = self._conn.cursor()
        cursor.executemany(query, params_list)
        self._conn.commit()

    def get_seen_ebay_item_ids(self) -> set[str]:
        if not self._conn:
            raise RuntimeError("SQLite connection not initialized")
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT source_url FROM fact_events WHERE scraped_from = 'ebay'"
        )
        import re
        ITEM_ID_RE = re.compile(r"/itm/(\d+)")
        ids = set()
        for row in cursor.fetchall():
            url = row[0] or ""
            match = ITEM_ID_RE.search(url)
            if match:
                ids.add(match.group(1))
        return ids

    def get_unparqueted_fact_events(self, source_region: str) -> list[sqlite3.Row]:
        if not self._conn:
            raise RuntimeError("SQLite connection not initialized")
        cursor = self._conn.cursor()
        cursor.execute(
            """SELECT * FROM fact_events
               WHERE scraped_from = 'ebay' AND source = ?
               AND (parqueted IS NULL OR parqueted = 0)
               ORDER BY scraped_at ASC""",
            (source_region,),
        )
        return cursor.fetchall()

    def mark_fact_events_parqueted(self, source_region: str) -> int:
        if not self._conn:
            raise RuntimeError("SQLite connection not initialized")
        cursor = self._conn.cursor()
        cursor.execute(
            """UPDATE fact_events SET parqueted = 1
               WHERE scraped_from = 'ebay' AND source = ?
               AND (parqueted IS NULL OR parqueted = 0)""",
            (source_region,),
        )
        self._conn.commit()
        return cursor.rowcount

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None