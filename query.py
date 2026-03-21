import sqlite3
from pathlib import Path


DB_PATH = Path(__file__).with_name("vega.db")


def get_row_count(conn: sqlite3.Connection, table_name: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return row[0]


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        bib_count = get_row_count(conn, "bib")
        editions_count = get_row_count(conn, "editions")
    finally:
        conn.close()

    print(f"database: {DB_PATH}")
    print(f"bib rows: {bib_count}")
    print(f"editions rows: {editions_count}")


if __name__ == "__main__":
    main()
