import asyncio
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import httpx
from tqdm.asyncio import tqdm_asyncio

from vega import format_groups, get_edition


DB_PATH = Path(__file__).with_name("vega.db")

BIB_COLUMNS = ("id", "title", "publicationDate", "coverUrl", "editionId")
EDITION_COLUMNS = ("id", "genre", "actors", "summary")


@dataclass
class SyncStats:
    inserted: int = 0
    updated: int = 0
    deleted: int = 0
    unchanged: int = 0
    skipped: int = 0


class RateLimiter:
    def __init__(self, min_interval_seconds: float = 0.25) -> None:
        self.min_interval_seconds = min_interval_seconds
        self._lock = asyncio.Lock()
        self._next_allowed = 0.0

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            if now < self._next_allowed:
                await asyncio.sleep(self._next_allowed - now)
                now = time.monotonic()
            self._next_allowed = now + self.min_interval_seconds


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bib (
            id TEXT PRIMARY KEY,
            title TEXT,
            publicationDate TEXT,
            coverUrl TEXT,
            editionId TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS editions (
            id TEXT PRIMARY KEY,
            genre TEXT,
            actors TEXT,
            summary TEXT
        )
        """
    )


def normalize_row(row: dict, columns: Iterable[str]) -> dict:
    return {column: row.get(column) for column in columns}


def load_existing_rows(
    conn: sqlite3.Connection, table_name: str, columns: Iterable[str]
) -> dict[str, dict]:
    selected = ", ".join(columns)
    rows = conn.execute(f"SELECT {selected} FROM {table_name}").fetchall()
    return {row["id"]: {column: row[column] for column in columns} for row in rows}


def rows_equal(current: dict, desired: dict, columns: Iterable[str]) -> bool:
    return all(current.get(column) == desired.get(column) for column in columns)


def insert_row(
    conn: sqlite3.Connection, table_name: str, columns: Iterable[str], row: dict
) -> None:
    column_list = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    values = [row[column] for column in columns]
    conn.execute(
        f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})",
        values,
    )


def update_row(
    conn: sqlite3.Connection, table_name: str, columns: Iterable[str], row: dict
) -> None:
    assignments = ", ".join(f"{column} = ?" for column in columns if column != "id")
    values = [row[column] for column in columns if column != "id"]
    values.append(row["id"])
    conn.execute(f"UPDATE {table_name} SET {assignments} WHERE id = ?", values)


def delete_row(conn: sqlite3.Connection, table_name: str, row_id: str) -> None:
    conn.execute(f"DELETE FROM {table_name} WHERE id = ?", (row_id,))


def sync_rows(
    conn: sqlite3.Connection,
    table_name: str,
    columns: Iterable[str],
    desired_rows: list[dict],
    expected_ids: set[str] | None = None,
    preserved_ids: set[str] | None = None,
) -> SyncStats:
    stats = SyncStats()
    existing_rows = load_existing_rows(conn, table_name, columns)
    desired_map = {
        row["id"]: normalize_row(row, columns)
        for row in desired_rows
        if row.get("id")
    }

    if expected_ids is None:
        expected_ids = set(desired_map)
    if preserved_ids is None:
        preserved_ids = set()

    for row_id in sorted(expected_ids):
        desired = desired_map.get(row_id)
        current = existing_rows.get(row_id)

        if desired is None:
            if row_id in preserved_ids:
                stats.skipped += 1
            continue

        if current is None:
            insert_row(conn, table_name, columns, desired)
            stats.inserted += 1
            continue

        if rows_equal(current, desired, columns):
            stats.unchanged += 1
            continue

        update_row(conn, table_name, columns, desired)
        stats.updated += 1

    delete_ids = set(existing_rows) - set(expected_ids)
    for row_id in sorted(delete_ids):
        delete_row(conn, table_name, row_id)
        stats.deleted += 1

    return stats


async def fetch_bib_rows() -> list[dict]:
    async with httpx.AsyncClient() as client:
        first_page_rows, total_pages = await format_groups(client, 1, True)
        bib_rows = [normalize_row(row, BIB_COLUMNS) for row in first_page_rows]

        if total_pages and total_pages > 1:
            remaining_pages = await tqdm_asyncio.gather(
                *(format_groups(client, page_num) for page_num in range(2, total_pages + 1)),
                desc="Fetching bib pages",
            )
            for page_rows in remaining_pages:
                bib_rows.extend(normalize_row(row, BIB_COLUMNS) for row in page_rows)

    return bib_rows


def extract_edition_ids(bib_rows: list[dict]) -> list[str]:
    return sorted(
        {
            row["editionId"]
            for row in bib_rows
            if row.get("editionId")
        }
    )


async def fetch_edition_rows(edition_ids: list[str]) -> tuple[list[dict], set[str]]:
    limiter = RateLimiter()
    failed_ids: set[str] = set()
    
    async def fetch_one(client: httpx.AsyncClient, edition_id: str) -> dict | None:
        try:
            row = await get_edition(client, limiter, edition_id)
        except Exception as exc:
            failed_ids.add(edition_id)
            print(f"edition fetch failed for {edition_id}: {exc}")
            return None

        return normalize_row(row, EDITION_COLUMNS)

    async with httpx.AsyncClient() as client:
        rows = await tqdm_asyncio.gather(
            *(fetch_one(client, edition_id) for edition_id in edition_ids),
            desc="Fetching editions",
        )

    return [row for row in rows if row is not None], failed_ids


def build_desired_edition_rows(
    existing_rows: dict[str, dict], fetched_rows: list[dict], edition_ids: list[str]
) -> list[dict]:
    desired_rows = []
    fetched_map = {row["id"]: row for row in fetched_rows if row.get("id")}

    for edition_id in edition_ids:
        if edition_id in fetched_map:
            desired_rows.append(fetched_map[edition_id])
        elif edition_id in existing_rows:
            desired_rows.append(existing_rows[edition_id])

    return desired_rows


def log_stats(label: str, stats: SyncStats) -> None:
    message = (
        f"{label}: inserted={stats.inserted}, updated={stats.updated}, "
        f"deleted={stats.deleted}, unchanged={stats.unchanged}"
    )
    if stats.skipped:
        message += f", skipped={stats.skipped}"
    print(message)


def main() -> None:
    bib_rows = asyncio.run(fetch_bib_rows())
    edition_ids = extract_edition_ids(bib_rows)

    conn = connect(DB_PATH)
    try:
        with conn:
            ensure_tables(conn)
            existing_edition_rows = load_existing_rows(conn, "editions", EDITION_COLUMNS)
            missing_edition_ids = [
                edition_id for edition_id in edition_ids if edition_id not in existing_edition_rows
            ]
            fetched_edition_rows, failed_edition_ids = asyncio.run(
                fetch_edition_rows(missing_edition_ids)
            )
            edition_rows = build_desired_edition_rows(
                existing_edition_rows, fetched_edition_rows, edition_ids
            )
            bib_stats = sync_rows(conn, "bib", BIB_COLUMNS, bib_rows)
            edition_stats = sync_rows(
                conn,
                "editions",
                EDITION_COLUMNS,
                edition_rows,
                expected_ids=set(edition_ids),
                preserved_ids=failed_edition_ids,
            )
    finally:
        conn.close()

    print(f"database: {DB_PATH}")
    print(f"bib rows fetched: {len(bib_rows)}")
    print(f"edition ids queued: {len(edition_ids)}")
    print(f"edition ids fetched: {len(missing_edition_ids)}")
    if failed_edition_ids:
        print(f"edition fetch failures: {len(failed_edition_ids)}")
    log_stats("bib", bib_stats)
    log_stats("editions", edition_stats)


if __name__ == "__main__":
    main()
