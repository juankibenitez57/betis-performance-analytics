import os
import sqlite3
from pathlib import Path


def get_conn(db_path: str) -> sqlite3.Connection:
    """
    Open a SQLite connection and ensure parent folder exists.
    """
    db_path = str(db_path)
    Path(os.path.dirname(db_path)).mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    """
    Create base tables for matches and ML features.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS matches (
            season TEXT NOT NULL,
            match_date TEXT,
            round INTEGER,
            home_away TEXT,
            opponent TEXT,
            gf INTEGER,
            ga INTEGER,
            points INTEGER,
            xg REAL,
            xga REAL,
            UNIQUE(season, round, home_away)
        );
        """
    )

    conn.execute("CREATE INDEX IF NOT EXISTS idx_matches_season_round ON matches(season, round);")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS features_matches (
            season TEXT,
            round INTEGER,
            points_rolling_5 REAL,
            gf_rolling_5 REAL,
            ga_rolling_5 REAL,
            xg_diff REAL,
            xg_diff_rolling_5 REAL
        );
        """
    )

    conn.commit()