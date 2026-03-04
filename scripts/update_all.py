# scripts/update_all.py
import argparse
from pathlib import Path

from src.db import get_conn, init_schema
from src.ingest_footballdata import fetch_betis_matches_footballdata


def parse_args():
    p = argparse.ArgumentParser(description="Update Betis SQLite DB from Football-Data CSV.")
    p.add_argument("--seasons", nargs="+", required=True, help="e.g. 2024-25 2025-26")
    return p.parse_args()


def upsert_matches(conn, rows) -> int:
    sql = """
    INSERT INTO matches(season, match_date, round, home_away, opponent, gf, ga, points, xg, xga)
    VALUES(?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(season, round) DO UPDATE SET
        match_date=excluded.match_date,
        opponent=excluded.opponent,
        gf=excluded.gf,
        ga=excluded.ga,
        points=excluded.points
    ;
    """
    before = conn.total_changes
    conn.executemany(sql, rows)
    conn.commit()
    return conn.total_changes - before


def main():
    args = parse_args()
    db_path = Path("data") / "betis.db"

    conn = get_conn(str(db_path))
    try:
        init_schema(conn)
        print(f"[OK] SQLite DB ready: {db_path}")
        print("[OK] Tables ensured: matches, features_matches")

        for season in args.seasons:
            print(f"\n=== {season} ===")
            try:
                df = fetch_betis_matches_footballdata(season)
            except Exception as e:
                print(f"[ERROR] {season}: {e}")
                continue

            if df.empty:
                print(f"[WARN] {season}: no matches found.")
                continue

            rows = [
                (
                    r["season"],
                    r["match_date"],
                    int(r["round"]),
                    r["home_away"],
                    r["opponent"],
                    int(r["gf"]),
                    int(r["ga"]),
                    int(r["points"]),
                    None,
                    None,
                )
                for _, r in df.iterrows()
            ]

            changed = upsert_matches(conn, rows)
            print(f"[OK] {season}: fetched={len(df)} | inserted/updated={changed}")

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM matches;")
        total = cur.fetchone()[0]
        print(f"\n[SUMMARY] Total rows in matches: {total}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()