import argparse
import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import requests


API_URL = "https://api.openbrewerydb.org/v1/breweries"
DEFAULT_PER_PAGE = 200


def fetch_page(page: int, per_page: int, session: requests.Session, timeout: int = 30) -> List[Dict[str, Any]]:
    params = {"page": page, "per_page": per_page}
    resp = session.get(API_URL, params=params, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise ValueError(f"Unexpected response shape (expected list), got: {type(data)}")
    return data


def fetch_all(per_page: int = DEFAULT_PER_PAGE, sleep_s: float = 0.1, max_pages: Optional[int] = None) -> List[Dict[str, Any]]:
    "Fetch all breweries via pagination until an empty page is returned. max_pages is for testing to limit how many pages to fetch."
    out: List[Dict[str, Any]] = []
    page = 1

    with requests.Session() as session:
        session.headers.update({"User-Agent": "brewery-etl-challenge/1.0"})
        while True:
            if max_pages is not None and page > max_pages:
                break

            rows = fetch_page(page=page, per_page=per_page, session=session)
            if not rows:
                break

            out.extend(rows)
            page += 1
            if sleep_s:
                time.sleep(sleep_s)

    return out


def ensure_dirs(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_jsonl(records: List[Dict[str, Any]], out_path: str) -> None:
    " Write raw records as JSON Lines."
    with open(out_path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def load_to_sqlite(
    df: pd.DataFrame,
    db_path: str,
    table: str = "raw_breweries",
    if_exists: str = "append",
) -> None:
    """
    Load dataframe to SQLite.
    if_exists: append | replace | fail
    """
    ensure_dirs(os.path.dirname(db_path) or ".")
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table, conn, if_exists=if_exists, index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract Open Brewery DB breweries and load to SQLite.")
    parser.add_argument("--db", default="data/breweries.db", help="Path to SQLite DB file.")
    parser.add_argument("--table", default="raw_breweries", help="Target SQLite table name.")
    parser.add_argument("--per-page", type=int, default=DEFAULT_PER_PAGE, help="Rows per page.")
    parser.add_argument("--sleep", type=float, default=0.1, help="Sleep between requests (seconds).")
    parser.add_argument("--max-pages", type=int, default=None, help="Limit pages for testing.")
    parser.add_argument(
        "--mode",
        choices=["append", "replace"],
        default="replace",
        help="Load mode for raw table. replace is simplest for challenges.",
    )
    parser.add_argument(
        "--raw-dir",
        default="data/raw",
        help="Directory to store raw JSONL snapshots.",
    )
    args = parser.parse_args()

    ingested_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    print(f"Fetching breweries from {API_URL} ...")
    records = fetch_all(per_page=args.per_page, sleep_s=args.sleep, max_pages=args.max_pages)
    print(f"Fetched {len(records)} records.")

    ensure_dirs(args.raw_dir)
    raw_out = os.path.join(args.raw_dir, f"breweries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl")
    write_jsonl(records, raw_out)
    print(f"Wrote raw snapshot: {raw_out}")

    df = pd.json_normalize(records)

    # Add ingestion metadata
    df["ingested_at_utc"] = ingested_at_utc
    df["source"] = "openbrewerydb"

    preferred_cols = [
        "id",
        "name",
        "brewery_type",
        "address_1",
        "address_2",
        "address_3",
        "city",
        "state_province",
        "postal_code",
        "country",
        "longitude",
        "latitude",
        "phone",
        "website_url",
        "state",    # (deprecated) may exist in some payloads
        "street",   # (deprecated) may exist in some payloads
        "ingested_at_utc",
        "source",
    ]
    # Keep only columns that exist, then add the rest
    cols_existing = [c for c in preferred_cols if c in df.columns]
    cols_rest = [c for c in df.columns if c not in cols_existing]
    df = df[cols_existing + cols_rest]

    print(f"Loading to SQLite: {args.db} (table={args.table}, mode={args.mode}) ...")
    load_to_sqlite(df, db_path=args.db, table=args.table, if_exists=args.mode)
    print("Done")

    # Quick sanity check
    print(df.head(1).to_string(index=False))


if __name__ == "__main__":
    main()