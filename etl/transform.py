import argparse
import os
import sqlite3
from typing import List, Tuple


def read_sql_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def run_script(conn: sqlite3.Connection, script: str, filename: str) -> None:
    try:
        conn.executescript(script)
    except sqlite3.Error as e:
        raise RuntimeError(f"SQLite error while running {filename}: {e}") from e


def table_count(conn: sqlite3.Connection, table_name: str) -> Tuple[str, int]:
    try:
        cur = conn.execute(f"SELECT COUNT(*) FROM {table_name};")
        return table_name, int(cur.fetchone()[0])
    except sqlite3.Error:
        return table_name, -1  # table missing or error


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SQLite SQL scripts in order.")
    parser.add_argument("--db", default="data/breweries.db", help="Path to SQLite DB file.")
    parser.add_argument(
        "--sql-dir",
        default="etl/sql",
        help="Directory containing SQL scripts.",
    )
    parser.add_argument(
        "--scripts",
        nargs="*",
        default=["01_staging.sql", "02_dimensions.sql"], # "03_qa_checks.sql" these are the optional QA checks, no output is shown on sqlite3.execute, so we can skip them for now
        help="SQL scripts to run in order (filenames, relative to --sql-dir).",
    )
    args = parser.parse_args()

    db_path = args.db
    sql_dir = args.sql_dir
    scripts = args.scripts

    # Basic validations
    if not os.path.exists(db_path):
        raise FileNotFoundError(
            f"Database not found at '{db_path}'. Run extraction first (etl/extract.py) to create it."
        )

    script_paths: List[str] = []
    for s in scripts:
        p = os.path.join(sql_dir, s)
        if not os.path.exists(p):
            raise FileNotFoundError(f"SQL script not found: '{p}'")
        script_paths.append(p)

    print(f"Using DB: {db_path}")
    print("Running scripts:")
    for p in script_paths:
        print(f" - {p}")

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")

        for path in script_paths:
            filename = os.path.basename(path)
            print(f"\n==> Running {filename} ...")
            script = read_sql_file(path)
            run_script(conn, script, filename)
            print(f"Completed {filename}")

        summary_tables = [
            "stg_breweries",
            "dim_type",
            "dim_geo",
            "dim_brewery"
        ]
        print("\nRow counts (missing tables show -1):")
        for t in summary_tables:
            name, cnt = table_count(conn, t)
            print(f" - {name}: {cnt}")

    print("\nDone")


if __name__ == "__main__":
    main()