import argparse
import sqlite3
from pathlib import Path
from typing import List, Optional

import pandas as pd


def get_tables(conn: sqlite3.Connection, include_views: bool = False) -> List[str]:
    types = ("table", "view") if include_views else ("table",)
    placeholders = ",".join(["?"] * len(types))
    q = f"""
    SELECT name
    FROM sqlite_master
    WHERE type IN ({placeholders})
      AND name NOT LIKE 'sqlite_%'
    ORDER BY type, name;
    """
    return [r[0] for r in conn.execute(q, types).fetchall()]


def read_table(conn: sqlite3.Connection, name: str) -> pd.DataFrame:
    return pd.read_sql_query(f'SELECT * FROM "{name}";', conn)


def safe_filename(name: str) -> str:
    return name.replace("/", "_").replace("\\", "_").replace(" ", "_")


def export_csv(df: pd.DataFrame, out_path: Path) -> None:
    df.to_csv(out_path, index=False, encoding="utf-8")

# TODO Future extension to Parquet, being lazy to add pyarrow dependency for now because PowerBI works better with CSVs anyway
def main() -> None:
    parser = argparse.ArgumentParser(description="Export SQLite tables to CSV for Power BI.")
    parser.add_argument("--db", default="data/breweries.db", help="Path to SQLite DB.")
    parser.add_argument("--out", default="data/exports", help="Output directory.")
    parser.add_argument(
        "--format",
        choices=["csv"],
        default="csv",
        help="Export format.",
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        default=None,
        help="Optional list of tables to export. If omitted, exports all tables (not views).",
    )
    parser.add_argument(
        "--include-views",
        action="store_true",
        help="Also export views (disabled by default).",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}. Run extract + transform first.")

    with sqlite3.connect(db_path) as conn:
        available = get_tables(conn, include_views=args.include_views)

        if args.tables is None:
            targets = available
        else:
            missing = [t for t in args.tables if t not in available]
            if missing:
                raise ValueError(f"Requested tables not found in DB: {missing}\nAvailable: {available}")
            targets = args.tables

        print(f"DB: {db_path}")
        print(f"Export dir: {out_dir}")
        print(f"Tables to export ({len(targets)}): {targets}")
        print(f"Format: {args.format}")

        for name in targets:
            df = read_table(conn, name)
            base = safe_filename(name)

            if args.format in ("csv"):
                csv_path = out_dir / f"{base}.csv"
                export_csv(df, csv_path)
                print(f"CSV  {name} -> {csv_path} ({len(df)} rows)")

    print("Done")


if __name__ == "__main__":
    main()