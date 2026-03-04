import pandas as pd
import requests


def fetch_betis_matches_fbref(season: str) -> pd.DataFrame:
    """
    Fetch Betis matches from FBref (basic version).
    """
    url = f"https://fbref.com/en/squads/6a9f39a9/{season}/matchlogs/all_comps/schedule/"
    
    response = requests.get(url)
    response.raise_for_status()

    tables = pd.read_html(response.text)

    if not tables:
        return pd.DataFrame()

    df = tables[0]

    # Keep only played matches
    df = df[df["GF"].notna()]

    df_clean = pd.DataFrame({
        "season": season,
        "match_date": df["Date"],
        "round": df["Round"],
        "opponent": df["Opponent"],
        "gf": df["GF"],
        "ga": df["GA"],
    })

    # Compute points
    df_clean["points"] = df_clean.apply(
        lambda r: 3 if r["gf"] > r["ga"] else (1 if r["gf"] == r["ga"] else 0),
        axis=1
    )

    return df_clean