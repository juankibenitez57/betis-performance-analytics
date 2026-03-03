import argparse
from pathlib import Path

from src.db import get_conn, init_schema


def parse_args():
    p = argparse.ArgumentParser(description="Initialize Betis SQLite DB schema.")
    p.add_argument("--seasons", nargs="+", required=True, help="Seasons, e.g. 2025-26 2024-25")
    return p.parse_args()


def main():
    _ = parse_args()  # not used yet, but keeps CLI stable
    db_path = Path("data") / "betis.db"
    conn = get_conn(str(db_path))
    try:
        init_schema(conn)
        print(f"[OK] SQLite DB ready: {db_path}")
        print("[OK] Tables ensured: matches, features_matches")
    finally:
        conn.close()


if __name__ == "__main__":
    main()