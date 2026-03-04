import re
from io import StringIO

import pandas as pd
import requests


def season_to_code(season: str) -> str:
    m = re.fullmatch(r"(\d{4})-(\d{2})", season.strip())
    if not m:
        raise ValueError("Season must look like '2024-25'")
    return f"{m.group(1)[-2:]}{m.group(2)}"


def fetch_laliga_csv(season: str) -> pd.DataFrame:
    code = season_to_code(season)
    url = f"https://datahub.io/core/spanish-la-liga/_r/-/season-{code}.csv"

    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=40)
    r.raise_for_status()

    return pd.read_csv(StringIO(r.text))


def fetch_betis_matches_footballdata(season: str) -> pd.DataFrame:
    raw = fetch_laliga_csv(season)

    teams = pd.Index(pd.concat([raw["HomeTeam"], raw["AwayTeam"]]).dropna().unique())
    betis_name = next((t for t in teams if isinstance(t, str) and "betis" in t.lower()), None)
    if not betis_name:
        raise RuntimeError("Betis not found in dataset")

    df = raw[(raw["HomeTeam"] == betis_name) | (raw["AwayTeam"] == betis_name)].copy()
    df = df[df["FTHG"].notna() & df["FTAG"].notna()].copy()

    # Parse date robustly without inference warnings
    df["_dt"] = pd.to_datetime(df["Date"], format="%d/%m/%Y", errors="coerce")
    mask = df["_dt"].isna()
    df.loc[mask, "_dt"] = pd.to_datetime(df.loc[mask, "Date"], format="%d/%m/%y", errors="coerce")

    df = df.sort_values("_dt").reset_index(drop=True)
    df["match_date"] = df["_dt"].dt.strftime("%Y-%m-%d")
    df = df.drop(columns=["_dt"])

    # Round = sequential Betis match number within that season
    df["round"] = range(1, len(df) + 1)

    is_home = df["HomeTeam"] == betis_name
    df["home_away"] = is_home.map({True: "H", False: "A"})

    df["opponent"] = df.apply(
        lambda r: r["AwayTeam"] if r["HomeTeam"] == betis_name else r["HomeTeam"],
        axis=1,
    )

    # Goals for/against from Betis perspective
    df["gf"] = df.apply(
        lambda r: r["FTHG"] if r["HomeTeam"] == betis_name else r["FTAG"],
        axis=1,
    )
    df["ga"] = df.apply(
        lambda r: r["FTAG"] if r["HomeTeam"] == betis_name else r["FTHG"],
        axis=1,
    )

    df["gf"] = pd.to_numeric(df["gf"], errors="coerce").astype(int)
    df["ga"] = pd.to_numeric(df["ga"], errors="coerce").astype(int)

    df["points"] = df.apply(
        lambda r: 3 if r["gf"] > r["ga"] else (1 if r["gf"] == r["ga"] else 0),
        axis=1,
    )

    out = df[["match_date", "round", "home_away", "opponent", "gf", "ga", "points"]].copy()
    out.insert(0, "season", season)

    return out.reset_index(drop=True)